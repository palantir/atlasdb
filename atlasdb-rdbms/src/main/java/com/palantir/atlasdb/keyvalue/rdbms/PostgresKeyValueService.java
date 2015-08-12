package com.palantir.atlasdb.keyvalue.rdbms;

import static com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils.makeSlots;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.postgresql.jdbc2.optional.PoolingDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils;
import com.palantir.atlasdb.keyvalue.rdbms.utils.CellValueMapper;
import com.palantir.atlasdb.keyvalue.rdbms.utils.Columns;
import com.palantir.atlasdb.keyvalue.rdbms.utils.MetaTable;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public final class PostgresKeyValueService extends AbstractKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(PostgresKeyValueService.class);
    private static final int CHUNK_SIZE = 10;

    private static final String USER_TABLE_PREFIX = "";
    private static final String USER_TABLE_PREFIX(String tableName) {
        return USER_TABLE_PREFIX + tableName;
    }

    private final DBI dbi;

    private DBI getDbi() {
        return dbi;
    }

    protected DataSource getTestDataSource() {
        return getTestPostgresDataSource();
    }


    private static DataSource getTestPostgresDataSource() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            Throwables.throwUncheckedException(e);
        }
        PoolingDataSource pgDataSource = new PoolingDataSource();
        pgDataSource.setDatabaseName("test");
        pgDataSource.setUser("test");
        pgDataSource.setPassword("password");
        try {
            pgDataSource.getConnection().setAutoCommit(true);
        } catch (SQLException e) {
            Throwables.throwUncheckedException(e);
        }
        return pgDataSource;
    }

    public PostgresKeyValueService(ExecutorService executor) {
        super(executor);
        dbi = new DBI(getTestDataSource());
    }

    @Override
    public void initializeFromFreshInstance() {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle conn) throws Exception {
                conn.execute("CREATE TABLE IF NOT EXISTS " + MetaTable.META_TABLE_NAME + " ("
                        + MetaTable.Columns.TABLE_NAME + " VARCHAR(128), "
                        + MetaTable.Columns.METADATA + " BYTEA NOT NULL)");
                return null;
            }
        });
    }

    @Override
    public void teardown() {
        for (String tableName : getAllTableNames()) {
            dropTable(tableName);
        }
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                conn.execute("DROP TABLE IF EXISTS " + MetaTable.META_TABLE_NAME);
                return null;
            }
        });
        super.teardown();
    };

    private Map<Cell, Value> getRowsInternal(final String tableName,
                                    final Collection<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        if (columnSelection.noColumnsSelected()) {
            return Maps.newHashMap();
        }

        return getDbi().inTransaction(new TransactionCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> inTransaction(Handle handle, TransactionStatus status) {

                final List<Pair<Cell, Value>> list;

                if (columnSelection.allColumnsSelected()) {
                    final Query<Pair<Cell,Value>> query = handle.createQuery(
                            "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                            "FROM " + tableName + " t " +
                            "LEFT JOIN " + tableName + " t2 " +
                    		"ON " + Columns.ROW("t") + " = " + Columns.ROW("t2") + " " +
            				"    AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("t2") + " " +
                    		"    AND " + Columns.TIMESTAMP("t") + " < " + Columns.TIMESTAMP("t2") + " " +
            				"    AND " + Columns.TIMESTAMP("t2") + " < " + timestamp + " " +
                            "WHERE " + Columns.ROW("t") + " IN (" + makeSlots("row", rows.size()) + ") " +
            				"    AND " + Columns.TIMESTAMP("t2") + " IS NULL" +
                            // This is a LEFT JOIN -> the below condition cannot be in ON clause
                            "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                            .map(CellValueMapper.instance());
                    AtlasSqlUtils.bindAll(query, rows);
                    list = query.list();
                } else {
                    SortedSet<byte[]> columns = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
                    Iterables.addAll(columns, columnSelection.getSelectedColumns());

                    final Query<Pair<Cell, Value>> query = handle.createQuery(
                            "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                            "FROM " + tableName + " t " +
                    		"LEFT JOIN " + tableName + " t2 " +
                    		"ON " + Columns.ROW("t") + " = " + Columns.ROW("t2") + " " +
            				"    AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("t2") + " " +
                    		"    AND " + Columns.TIMESTAMP("t") + "<" + Columns.TIMESTAMP("t2") + " " +
            				"    AND " + Columns.TIMESTAMP("t2") + " < " + timestamp + " " +
                    		"WHERE " + Columns.ROW("t") + " IN (" + makeSlots("row", rows.size()) + ") " +
            				"    AND " + Columns.COLUMN("t") + " IN (" + makeSlots("column", columns.size()) + ") " +
                    		"    AND " + Columns.TIMESTAMP("t2") + " IS NULL " +
                            // This is a LEFT JOIN -> the below condition cannot be in ON clause
                            "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                    		.map(CellValueMapper.instance());

                    AtlasSqlUtils.bindAll(query, rows);
                    AtlasSqlUtils.bindAll(query, columns, rows.size());
                    list = query.list();
                }

                Map<Cell, Value> result = Maps.newHashMap();
                for (Pair<Cell, Value> cv : list) {
                    Preconditions.checkState(!result.containsKey(cv.getLhSide()));
                    result.put(cv.getLhSide(), cv.getRhSide());
                }
                return result;
            }
        });
    }

    private <V> void batch(Iterable<V> items, Function<Collection<V>, Void> runWithBatch) {
        for (List<V> chunk : Iterables.partition(items, CHUNK_SIZE)) {
            runWithBatch.apply(chunk);
        }
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        // TODO: Sort for best performance?
        final Map<Cell, Value> result = Maps.newHashMap();
        batch(rows, new Function<Collection<byte[]>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Collection<byte[]> input) {
                result.putAll(getRowsInternal(tableName, input, columnSelection, timestamp));
                return null;
            }
        });
        return result;
    }

    private Map<Cell, Value> getInternal(final String tableName,
                                         final Collection<Entry<Cell, Long>> timestampByCell) {
        return getDbi().withHandle(new HandleCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> withHandle(Handle handle) throws Exception {

                Map<Cell, Value> result = Maps.newHashMap();

                Query<Pair<Cell,Value>> query = handle.createQuery(
                        "WITH matching_cells AS (" +
                        "    SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                        "    FROM " + USER_TABLE_PREFIX(tableName) + " t " +
                        "    JOIN (VALUES " + makeSlots("cell", timestampByCell.size(), 3) + ") " +
                        "        AS t2(" + Columns.ROW.append(Columns.COLUMN).append(Columns.TIMESTAMP) + ")" +
                        "    ON " + Columns.ROW("t").eq(Columns.ROW("t2"))
                                .and(Columns.COLUMN("t").eq(Columns.COLUMN("t2")))
                                .and(Columns.TIMESTAMP("t").lt(Columns.TIMESTAMP("t2"))) + ") " +
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                        "FROM matching_cells t " +
                        "LEFT JOIN matching_cells t2 " +
                        "ON " + Columns.ROW("t").eq(Columns.ROW("t2"))
                                .and(Columns.COLUMN("t").eq(Columns.COLUMN("t2")))
                                .and(Columns.TIMESTAMP("t").lt(Columns.TIMESTAMP("t2"))).appendSpace() +
                        "WHERE t2.atlasdb_timestamp IS NULL")
                        .map(CellValueMapper.instance());

                AtlasSqlUtils.bindCellsTimestamps(query, timestampByCell);

                for (Pair<Cell, Value> cv : query.list()) {
                    Preconditions.checkState(!result.containsKey(cv.getLhSide()));
                    result.put(cv.getLhSide(), cv.getRhSide());
                }

                return result;
            }
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, final Map<Cell, Long> timestampByCell) {
        final Map<Cell, Value> result = Maps.newHashMap();
        batch(timestampByCell.entrySet(), new Function<Collection<Entry<Cell, Long>>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Collection<Entry<Cell, Long>> input) {
                result.putAll(getInternal(tableName, input));
                return null;
            }
        });
        return result;
    }

    private void putInternal(final String tableName,
                             final Collection<Entry<Cell, byte[]>> values,
                             final long timestamp) {
        try {
            getDbi().withHandle(new HandleCallback<Void>() {
                @Override
                public Void withHandle(Handle handle) throws Exception {
                    Update update = handle.createStatement("INSERT INTO "
                            + USER_TABLE_PREFIX(tableName) + " (" + "    " + Columns.ROW + ", "
                            + "    " + Columns.COLUMN + ", " + "    " + Columns.TIMESTAMP + ", "
                            + "    " + Columns.CONTENT + ") VALUES " + "    "
                            + makeSlots("cell", values.size(), 4));
                    int pos = 0;
                    for (Entry<Cell, byte[]> entry : values) {
                        update.bind(pos++, entry.getKey().getRowName());
                        update.bind(pos++, entry.getKey().getColumnName());
                        update.bind(pos++, timestamp);
                        update.bind(pos++, entry.getValue());
                    }
                    update.execute();
                    return null;
                }
            });
        } catch (RuntimeException e) {
            if (throwableContainsMessage(e, "ORA-00001", "unique constraint")) {
                throw new KeyAlreadyExistsException("Unique constraint violation", e);
            }
        }
    }

    static boolean throwableContainsMessage(Throwable e, String... messages) {
        for (Throwable ex = e; ex != null; ex = ex.getCause()) {
            for (String message : messages) {
                if (ex.getMessage().contains(message)) {
                    return true;
                }
            }
            if (ex == ex.getCause()) {
                return false;
            }
        }
        return false;
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        batch(values.entrySet(), new Function<Collection<Entry<Cell, byte[]>>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Collection<Entry<Cell, byte[]>> input) {
                putInternal(tableName, input, timestamp);
                return null;
            }
        });
    }

    private void putWithTimestampsInternal(final String tableName,
                                           final Collection<Entry<Cell, Value>> cellValues) {
        try {
            getDbi().withHandle(new HandleCallback<Void>() {
                @Override
                public Void withHandle(Handle handle) throws Exception {
                    Update update = handle.createStatement("INSERT INTO "
                            + USER_TABLE_PREFIX(tableName) + " (" + "    " + Columns.ROW + ", "
                            + "    " + Columns.COLUMN + ", " + "    " + Columns.TIMESTAMP + ", "
                            + "    " + Columns.CONTENT + ") VALUES " + "    "
                            + makeSlots("cell", cellValues.size(), 4));
                    AtlasSqlUtils.bindCellsValues(update, cellValues);
                    update.execute();
                    return null;
                }
            });
        } catch (RuntimeException e) {
            if (throwableContainsMessage(e, "ORA-00001", "unique constraint")) {
                throw new KeyAlreadyExistsException("Unique constraint violation", e);
            }
        }
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        batch(cellValues.entries(), new Function<Collection<Entry<Cell, Value>>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Collection<Entry<Cell, Value>> input) {
                putWithTimestampsInternal(tableName, input);
                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        put(tableName, values, 0);
    }

    private void deleteInternal(final String tableName, final Collection<Entry<Cell, Long>> keys) {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                Update update = handle.createStatement(
                        "DELETE FROM " + tableName + " t " +
                		"WHERE EXISTS (" +
                		"    SELECT 1 FROM (VALUES " + makeSlots("cell", keys.size(), 3) + ") " +
        				"        t2(atlasdb_row, atlasdb_column, atlasdb_timestamp) " +
                		"    WHERE " + Columns.ROW("t") + " = " + Columns.ROW("t2") + " " +
                		"        AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("t2") + " " +
                		"        AND " + Columns.TIMESTAMP("t") + " = " + Columns.TIMESTAMP("t2") + ")");
                AtlasSqlUtils.bindCellsTimestamps(update, keys);
                update.execute();
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        batch(keys.entries(), new Function<Collection<Entry<Cell, Long>>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Collection<Entry<Cell, Long>> input) {
                deleteInternal(tableName, input);
                return null;
            }
        });
    }

    private int getMaxRows(RangeRequest rangeRequest) {
        return rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
    }

    private List<byte[]> getRowsInRange(String tableName,
                                        RangeRequest rangeRequest,
                                        long timestamp,
                                        int limit,
                                        Handle handle) {
        return handle.createQuery(
                "SELECT DISTINCT " + Columns.ROW + " FROM " + tableName + " " +
                "WHERE " + Columns.ROW + " >= :startRow" +
                "    AND " + Columns.ROW + " < :endRow " +
                "    AND " + Columns.TIMESTAMP + " < :timestamp " +
                "LIMIT :limit")
                .bind("startRow", rangeRequest.getStartInclusive())
                .bind("endRow", RangeRequests.endRowExclusiveOrOneAfterMax(rangeRequest))
                .bind("timestamp", timestamp)
                .bind("limit", limit)
                .map(ByteArrayMapper.FIRST)
                .list();
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPage(final String tableName,
                                                                          final RangeRequest rangeRequest,
                                                                          final long timestamp) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        final int maxRows = getMaxRows(rangeRequest);
        return getDbi().withHandle(new HandleCallback<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Value>, byte[]> withHandle(Handle handle)
                    throws Exception {
                List<byte[]> rows = getRowsInRange(tableName, rangeRequest,
                        timestamp, maxRows, handle);
                if (rows.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(
                            rangeRequest.getStartInclusive(),
                            Collections.<RowResult<Value>> emptyList(),
                            false);
                }
                ColumnSelection columns = RangeRequests.extractColumnSelection(rangeRequest);
                Map<Cell, Value> cells = getRows(tableName, rows, columns, timestamp);
                List<RowResult<Value>> result = AtlasSqlUtils.cellsToRows(cells);
                return SimpleTokenBackedResultsPage.create(
                        AtlasSqlUtils.generateToken(rangeRequest, rows),
                        result, rows.size() == maxRows);
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        return ClosableIterators.wrap(new AbstractPagingIterable<RowResult<Value>, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getFirstPage()
                    throws Exception {
                return getPage(tableName, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<Value>, byte[]> previous)
                    throws Exception {
                return getPage(
                        tableName,
                        rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(),
                        timestamp);
            }
        }.iterator());
    }

    List<Cell> getCellsInRange(final String tableName, RangeRequest rangeRequest, long timestamp, int limit, Handle handle) {
        return handle.createQuery(
                "SELECT DISTINCT " +
                "    " + Columns.ROW + ", " +
                "    " + Columns.COLUMN + " " +
                "FROM " + USER_TABLE_PREFIX(tableName) + " " +
                "WHERE " + Columns.ROW + ">= :startRow" +
                "    AND " + Columns.ROW + "< :endRow " +
        		"    AND " + Columns.TIMESTAMP + "< :timestamp " +
                "LIMIT :limit")
                .bind("startRow", rangeRequest.getStartInclusive())
                .bind("endRow", RangeRequests.endRowExclusiveOrOneAfterMax(rangeRequest))
                .bind("timestamp", timestamp)
                .bind("limit", limit)
                .map(new ResultSetMapper<Cell>() {
                    @Override
                    public Cell map(int index, ResultSet r, StatementContext ctx)
                            throws SQLException {
                        byte[] row = r.getBytes(Columns.ROW.toString());
                        byte[] column = r.getBytes(Columns.COLUMN.toString());
                        return Cell.create(row, column);
                    }
                })
                .list();
    }

    TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> getPageWithHistory(final String tableName,
                                                                                  final RangeRequest rangeRequest,
                                                                                  final long timestamp) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        final int maxRows = getMaxRows(rangeRequest);
        return getDbi().withHandle(
                new HandleCallback<TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]>>() {
                    @Override
                    public TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> withHandle(Handle handle)
                            throws Exception {
                        List<Cell> cells = getCellsInRange(
                                tableName,
                                rangeRequest,
                                timestamp,
                                maxRows,
                                handle);
                        // TODO: Will this fail if none of the rows in the first range
                        // has matching columns (but later rows do have).
                        if (cells.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(
                                    rangeRequest.getStartInclusive(),
                                    Collections.<RowResult<Set<Value>>> emptyList(),
                                    false);
                        }

                        SetMultimap<Cell, Value> timestamps = getAllVersionsInternal(
                                tableName,
                                ImmutableSet.copyOf(cells),
                                timestamp);
                        Set<RowResult<Set<Value>>> finalResult = AtlasSqlUtils.cellsToRows(timestamps);
                        byte[] lastRow = cells.get(cells.size() - 1).getRowName();
                        byte[] token = RangeRequests.getNextStartRow(
                                rangeRequest.isReverse(), lastRow);
                        return SimpleTokenBackedResultsPage.create(
                                token,
                                finalResult,
                                cells.size() == maxRows);
                    }
                });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        return ClosableIterators.wrap(new AbstractPagingIterable<RowResult<Set<Value>>, TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]>>() {

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> getFirstPage()
                    throws Exception {
                return getPageWithHistory(tableName, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> previous)
                    throws Exception {
                return getPageWithHistory(
                        tableName,
                        rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(),
                        timestamp);
            }

        }.iterator());
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp)
            throws InsufficientConsistencyException {
        return ClosableIterators.wrap(new AbstractPagingIterable<RowResult<Set<Long>>, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getFirstPage()
                    throws Exception {
                return getPageOfTimestamps(tableName, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> previous)
                    throws Exception {
                return getPageOfTimestamps(
                        tableName,
                        rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(),
                        timestamp);
            }
            }.iterator());
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getPageOfTimestamps(final String tableName,
                                                                                          final RangeRequest rangeRequest,
                                                                                          final long timestamp) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        final int maxRows = getMaxRows(rangeRequest);
        return getDbi().withHandle(
                new HandleCallback<TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
                    @Override
                    public TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> withHandle(Handle handle)
                            throws Exception {
                        List<Cell> cells = getCellsInRange(
                                tableName,
                                rangeRequest,
                                timestamp,
                                maxRows,
                                handle);
                        if (cells.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(
                                    rangeRequest.getStartInclusive(),
                                    Collections.<RowResult<Set<Long>>> emptyList(),
                                    false);
                        }

                        SetMultimap<Cell, Long> timestamps = getAllTimestamps(
                                tableName,
                                ImmutableSet.copyOf(cells),
                                timestamp);
                        Set<RowResult<Set<Long>>> result = AtlasSqlUtils.cellsToRows(timestamps);
                        byte[] token = RangeRequests.getNextStartRow(
                                rangeRequest.isReverse(),
                                cells.get(cells.size() - 1).getRowName());
                        return new SimpleTokenBackedResultsPage<RowResult<Set<Long>>, byte[]>(
                                token,
                                result,
                                cells.size() == maxRows);
                    }
                });
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(
                this,
                tableName,
                rangeRequests,
                timestamp);
    }

    @Override
    @Idempotent
    public void dropTable(final String tableName) throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute("DROP TABLE " + tableName);
                handle.execute("DELETE FROM " + MetaTable.META_TABLE_NAME + " WHERE "
                        + MetaTable.Columns.TABLE_NAME + " = ?", tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final String tableName, int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute(
                        "CREATE TABLE " + tableName + " ( " +
                		"    " + Columns.ROW + " BYTEA NOT NULL, " +
                		"    " + Columns.COLUMN + " BYTEA NOT NULL, " +
                		"    " + Columns.TIMESTAMP + " INT NOT NULL, " +
                		"    " + Columns.CONTENT + " BYTEA NOT NULL," +
        				"    PRIMARY KEY (" +
        				"        " + Columns.ROW + ", " +
						"        " + Columns.COLUMN + ", " +
        				"        " + Columns.TIMESTAMP + "))");
                handle.execute("INSERT INTO " + MetaTable.META_TABLE_NAME + " (" +
                        "    " + MetaTable.Columns.TABLE_NAME + ", " +
                        "    " + MetaTable.Columns.METADATA + " ) VALUES (" +
                        "    ?, ?)", tableName, ArrayUtils.EMPTY_BYTE_ARRAY);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        return Sets.newHashSet(getDbi().withHandle(new HandleCallback<List<String>>() {
            @Override
            public List<String> withHandle(Handle handle) throws Exception {
                return handle.createQuery(
                        "SELECT " + MetaTable.Columns.TABLE_NAME + " FROM "
                                + MetaTable.META_TABLE_NAME).map(StringMapper.FIRST).list();
            }
        }));
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(final String tableName) {
        return getDbi().inTransaction(new TransactionCallback<byte[]>() {
            @Override
            public byte[] inTransaction(Handle conn, TransactionStatus status) throws Exception {
                return conn.createQuery(
                        "SELECT " + MetaTable.Columns.METADATA + " FROM "
                                + MetaTable.META_TABLE_NAME + " WHERE "
                                + MetaTable.Columns.TABLE_NAME + " = :tableName").bind(
                        "tableName",
                        tableName).map(ByteArrayMapper.FIRST).first();
            }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final String tableName, final byte[] metadata) {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                conn.execute("UPDATE " + MetaTable.META_TABLE_NAME + " SET "
                        + MetaTable.Columns.METADATA + " = ? WHERE " + MetaTable.Columns.TABLE_NAME
                        + " = ?", metadata, tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName, final Set<Cell> cells) {
        // TODO: Make it faster by using dedicated query?
        final Value invalidValue = Value.create(ArrayUtils.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
        Multimap<Cell, Value> cellsWithInvalidValues = HashMultimap.create();
        for (Cell cell : cells) {
            cellsWithInvalidValues.put(cell, invalidValue);
        }
        putWithTimestamps(tableName, cellsWithInvalidValues);
    }

    private SetMultimap<Cell, Value> getAllVersionsInternal(final String tableName,
                                                            final Set<Cell> cells,
                                                            final long timestamp)
            throws InsufficientConsistencyException {
        return getDbi().withHandle(new HandleCallback<SetMultimap<Cell, Value>>() {
            @Override
            public SetMultimap<Cell, Value> withHandle(Handle handle) throws Exception {

                Query<Pair<Cell, Value>> query = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                        "FROM " + tableName + " t " +
                        "WHERE (" + Columns.ROW + ", " + Columns.COLUMN + ") IN (" +
                        "    " + makeSlots("cell", cells.size(), 2) + ") " +
                        "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                        .map(CellValueMapper.instance());
                int pos = 0;
                for (Cell cell : cells) {
                    query.bind(pos++, cell.getRowName());
                    query.bind(pos++, cell.getColumnName());
                }

                SetMultimap<Cell, Value> result = HashMultimap.create();
                for (Pair<Cell, Value> p : query.list()) {
                    result.put(p.getLhSide(), p.getRhSide());
                }
                return result;
            }
        });
    }

    private SetMultimap<Cell, Long> getAllTimestampsInternal(final String tableName,
                                                 final Collection<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {

        return getDbi().withHandle(new HandleCallback<SetMultimap<Cell, Long>>() {
            @Override
            public SetMultimap<Cell, Long> withHandle(Handle handle) throws Exception {

                Query<Pair<Cell, Long>> query = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_AS("t") + " " +
                        "FROM " + tableName + " t " +
                        "WHERE (" + Columns.ROW.append(Columns.COLUMN) + ") IN (" +
                        "    " + makeSlots("cell", cells.size(), 2) + ") " +
                        "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                        .map(new ResultSetMapper<Pair<Cell, Long>>() {
                            @Override
                            public Pair<Cell, Long> map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException {
                                byte[] row = r.getBytes(Columns.ROW.toString());
                                byte[] column = r.getBytes(Columns.COLUMN.toString());
                                long timestamp = r.getLong(Columns.TIMESTAMP.toString());
                                Cell cell = Cell.create(row, column);
                                return Pair.create(cell, timestamp);
                            }
                        });
                AtlasSqlUtils.bindCells(query, cells);

                SetMultimap<Cell, Long> result = HashMultimap.create();
                for (Pair<Cell, Long> p : query.list()) {
                    result.put(p.getLhSide(), p.getRhSide());
                }

                return result;
            }
        });
    }


    @Override
    @Idempotent
    public SetMultimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {
        // TODO: Sort for better performance?
        final SetMultimap<Cell, Long> result = HashMultimap.create();
        batch(cells, new Function<Collection<Cell>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Collection<Cell> input) {
                result.putAll(getAllTimestampsInternal(tableName, input, timestamp));
                return null;
            }
        });
        return result;
    }

    @Override
    public void compactInternally(String tableName) {
        // TODO Auto-generated method stub
    }

}
