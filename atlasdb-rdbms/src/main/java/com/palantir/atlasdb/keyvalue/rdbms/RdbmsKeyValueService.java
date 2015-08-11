package com.palantir.atlasdb.keyvalue.rdbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.postgresql.jdbc2.optional.ConnectionPool;
import org.postgresql.jdbc2.optional.PoolingDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public final class RdbmsKeyValueService extends AbstractKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(RdbmsKeyValueService.class);

    private static final class Columns {
        public static final String TIMESTAMP = "atlasdb_timestamp";
        public static final String CONTENT = "atlasdb_content";
        public static final String ROW = "atlasdb_row";
        public static final String COLUMN = "atlasdb_column";

        public static final String ROW(String tableName) {
            return tableName + "." + ROW;
        }

        public static final String COLUMN(String tableName) {
            return tableName + "." + COLUMN;
        }

        public static final String TIMESTAMP(String tableName) {
            return tableName + "." + TIMESTAMP;
        }

        public static final String CONTENT(String tableName) {
            return tableName + "." + CONTENT;
        }

        public static final String ROW_COLUMN(String tableName) {
            return ROW(tableName) + ", " + COLUMN(tableName);
        }

        public static final String ROW_COLUMN_TIMESTAMP(String tableName) {
            return ROW(tableName) + ", " + COLUMN(tableName) + ", " + TIMESTAMP(tableName);
        }

        public static final String ROW_COLUMN_TIMESTAMP_AS(String tableName) {
            return ROW(tableName) + " AS " + ROW + ", " + COLUMN(tableName) + " AS " + COLUMN + ", " + TIMESTAMP(tableName) + " AS " + TIMESTAMP;
        }

        public static final String ROW_COLUMN_TIMESTAMP_CONTENT_AS(String tableName) {
            return ROW_COLUMN_TIMESTAMP_AS(tableName) + ", " + CONTENT(tableName) + " AS " + CONTENT;
        }
    }

    private static final String USER_TABLE_PREFIX = "atlasdb_user_";
    private static final String USER_TABLE_PREFIX(String tableName) {
        return USER_TABLE_PREFIX + tableName;
    }

    private static final class MetaTable {
        public static final String META_TABLE_NAME = "_table_meta";
        private static final class Columns {
            public static final String TABLE_NAME = "table_name";
            public static final String METADATA = "metadata";
        }
    }

    private final DBI dbi;

    private DBI getDbi() {
        return dbi;
    }

    private DataSource getTestDataSource() {
//        return getTestH2DataSource();
        return getTestPostgresDataSource();
    }

    private DataSource getTestH2DataSource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test", "username", "password");
    }

    ConnectionPool connectionPool = new ConnectionPool();

    private DataSource getTestPostgresDataSource() {
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

    public RdbmsKeyValueService(ExecutorService executor) {
        super(executor);
        dbi = new DBI(getTestDataSource());
    }

    @Override
    public void initializeFromFreshInstance() {
        try {
            getDbi().inTransaction(new TransactionCallback<Void>() {
                @Override
                public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                    // Silently ignore if already initialized
                    handle.select("SELECT 1 FROM " + MetaTable.META_TABLE_NAME);
                    log.warn("Initializing an already initialized rdbms kvs. Ignoring.");
                    return null;
                }
            });
        } catch (RuntimeException e) {
            log.warn("Initializing from fresh instance " + this);
            getDbi().inTransaction(new TransactionCallback<Void>() {

                @Override
                public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                    conn.execute("CREATE TABLE " + MetaTable.META_TABLE_NAME + " ("
                            + MetaTable.Columns.TABLE_NAME + " VARCHAR(128), "
                            + MetaTable.Columns.METADATA + " BYTEA NOT NULL)");
                    return null;
                }
            });
        }
    }

    private String makeSlots(String prefix, int number) {
        String result = "";
        for (int i=0; i<number; ++i) {
            result += ":" + prefix + i;
            if (i + 1 < number) {
                result += ", ";
            }
        }
        return result;
    }

    private Map<Cell, Value> getRowsInternal(final String tableName,
                                    final SortedSet<byte[]> rows,
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
                    int pos = 0;
                    for (byte[] row : rows) {
                        query.bind("row" + pos++, row);
                    }
                    list = query.list();
                } else {
                    SortedSet<byte[]> columns = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
                    for (byte[] column : columnSelection.getSelectedColumns()) {
                        columns.add(column);
                    }

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

                    int pos = 0;
                    for (byte[] row : rows) {
                        query.bind("row" + pos++, row);
                    }
                    pos = 0;
                    for (byte[] column : columns) {
                        query.bind("column" + pos++, column);
                    }

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


    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        SortedSet<byte[]> rowSet = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        final int CHUNK_SIZE = 1;
        for (byte[] row : rows) {
            rowSet.add(row);
        }

        Map<Cell, Value> result = Maps.newHashMap();

        while (!rowSet.isEmpty()) {
            SortedSet<byte[]> chunk = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
            for (int i = 0; i < CHUNK_SIZE; ++i) {
                chunk.add(rowSet.first());
                rowSet.remove(rowSet.first());
                if (rowSet.isEmpty()) {
                    break;
                }
            }
            Map<Cell, Value> chunkResult = getRowsInternal(tableName, chunk, columnSelection, timestamp);
            result.putAll(chunkResult);
        }
        return result;
    }

    private static class RowResultMapper implements ResultSetMapper<RowResult<Value>> {
        private static RowResultMapper instance;

        @Override
        public RowResult<Value> map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            byte[] row = r.getBytes(Columns.ROW);
            byte[] col = r.getBytes(Columns.COLUMN);
            long timestamp = r.getLong(Columns.TIMESTAMP);
            byte[] content = r.getBytes(Columns.CONTENT);
            return RowResult.of(Cell.create(row, col), Value.create(content, timestamp));
        }

        private static RowResultMapper instance() {
            if (instance == null) {
                RowResultMapper ret = new RowResultMapper();
                instance = ret;
            }
            return instance;
        }
    }

    private static class CellValueMapper implements ResultSetMapper<Pair<Cell, Value>> {
        private static CellValueMapper instance;

        @Override
        public Pair<Cell, Value> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            byte[] row = r.getBytes(Columns.ROW);
            byte[] col = r.getBytes(Columns.COLUMN);
            long timestamp = r.getLong(Columns.TIMESTAMP);
            byte[] content = r.getBytes(Columns.CONTENT);
            Cell cell = Cell.create(row, col);
            Value value = Value.create(content, timestamp);
            return Pair.create(cell, value);
        }

        private static CellValueMapper instance() {
            if (instance == null) {
                CellValueMapper ret = new CellValueMapper();
                instance = ret;
            }
            return instance;
        }
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, final Map<Cell, Long> timestampByCell) {
        return getDbi().withHandle(new HandleCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> withHandle(Handle handle) throws Exception {

                handle.execute(
                        "CREATE TEMPORARY TABLE CellsToRetrieve (" +
                        "    " + Columns.ROW + " BYTEA NOT NULL, " +
                        "    " + Columns.COLUMN + " BYTEA NOT NULL, " +
                        "    " + Columns.TIMESTAMP + " INT NOT NULL)");

                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO CellsToRetrieve (" +
                        "    " + Columns.ROW + ", " +
                		"    " + Columns.COLUMN + ", " +
                        "    " + Columns.TIMESTAMP + ") VALUES (" +
                		"    :row, :column, :timestamp)");
                for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                    batch.add(entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue());
                }
                batch.execute();

                Map<Cell, Value> result = Maps.newHashMap();

                List<Pair<Cell, Value>> values = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
        		        "FROM " + tableName + " t " +
		        		"JOIN CellsToRetrieve c " +
		        		"ON " + Columns.ROW("t") + " = " + Columns.ROW("c") + " " +
		        		"    AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("c") + " " +
		        		"    AND " + Columns.TIMESTAMP("t") + " < " + Columns.TIMESTAMP("c") + " " +
		        		"LEFT JOIN " + tableName + " t2 " +
        				"ON " + Columns.ROW("t") + " = " + Columns.ROW("t2") + " " +
		        		"    AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("t2") + " " +
        				"    AND " + Columns.TIMESTAMP("t") + " < " + Columns.TIMESTAMP("t2") + " " +
		        		"    AND " + Columns.TIMESTAMP("t2") + " < " + Columns.TIMESTAMP("c") + " " +
        				"WHERE " + Columns.TIMESTAMP("t2") + " IS NULL")
        				.map(CellValueMapper.instance())
        				.list();

                handle.execute("DROP TABLE CellsToRetrieve");

                for (Pair<Cell, Value> cv : values) {
                    Preconditions.checkState(!result.containsKey(cv.getLhSide()));
                    result.put(cv.getLhSide(), cv.getRhSide());
                }

                return result;
            }
        });
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        // TODO: Throw the KeyAlreadyExistsException when appropriate
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {

                handle.execute(
                        "CREATE TEMPORARY TABLE ValuesToPut (" +
                		"    " + Columns.ROW + " BYTEA NOT NULL, " +
                		"    " + Columns.COLUMN + " BYTEA NOT NULL, " +
                		"    " + Columns.CONTENT + " BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO ValuesToPut (" +
                        "    " + Columns.ROW + ", " +
                        "    " + Columns.COLUMN + ", " +
                        "    " + Columns.CONTENT + ") "  +
                        "VALUES (" +
                        "    :row, :column, :content)");
                for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                    batch.add(e.getKey().getRowName(), e.getKey().getColumnName(), e.getValue());
                }
                batch.execute();

                handle.execute(
                        "INSERT INTO " + tableName + " (" +
                		"    " + Columns.ROW + ", " +
                		"    " + Columns.COLUMN + ", " +
                		"    " + Columns.TIMESTAMP + ", " +
                		"    " + Columns.CONTENT + ") " +
                		"SELECT " +
                		"    " + Columns.ROW + ", " +
                		"    " + Columns.COLUMN + ", " +
                		"    " + timestamp + ", " +
                		"    " + Columns.CONTENT + " " +
                        "FROM ValuesToPut");

                handle.execute("DROP TABLE ValuesToPut");

                return null;
            }
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        // TODO: Throw the KeyAlreadyExistsException when appropriate
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {

                handle.execute(
                        "CREATE TEMPORARY TABLE ValuesToPut (" +
                        "    " + Columns.ROW + " BYTEA NOT NULL, " +
                        "    " + Columns.COLUMN + " BYTEA NOT NULL, " +
                        "    " + Columns.TIMESTAMP + " INT NOT NULL, " +
                        "    " + Columns.CONTENT + " BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO ValuesToPut (" +
                        "    " + Columns.ROW + ", " +
                        "    " + Columns.COLUMN + ", " +
                        "    " + Columns.TIMESTAMP + ", " +
                        "    " + Columns.CONTENT + ") VALUES (" +
                        "    :row, :column, :timestamp, :content)");
                for (Entry<Cell, Value> e : cellValues.entries()) {
                    batch.add(
                            e.getKey().getRowName(),
                            e.getKey().getColumnName(),
                            e.getValue().getTimestamp(),
                            e.getValue().getContents());
                }
                batch.execute();

                handle.execute(
                        "INSERT INTO " + tableName + " (" +
                        "    " + Columns.ROW + ", " +
                        "    " + Columns.COLUMN + ", " +
                        "    " + Columns.TIMESTAMP + ", " +
                        "    " + Columns.CONTENT + ") " +
                		"SELECT " +
                        "    " + Columns.ROW + ", " +
                		"    " + Columns.COLUMN + ", " +
                        "    " + Columns.TIMESTAMP + ", " +
                		"    " + Columns.CONTENT + " " +
                        "FROM ValuesToPut");

                handle.execute("DROP TABLE ValuesToPut");

                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        put(tableName, values, 0);
    }

    @Override
    @Idempotent
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                handle.execute(
                        "CREATE TEMPORARY TABLE CellsToDelete (" +
                        "    " + Columns.ROW + " BYTEA NOT NULL, " +
                        "    " + Columns.COLUMN + " BYTEA NOT NULL, " +
                        "    " + Columns.TIMESTAMP + " INT NOT NULL)");
                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO CellsToDelete (" +
                        "    " + Columns.ROW + ", " +
                        "    " + Columns.COLUMN + ", " +
                        "    " + Columns.TIMESTAMP + ") VALUES (" +
                        "    :row, :column, :timestamp)");
                for (Entry<Cell, Long> entry : keys.entries()) {
                    batch.add(entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue());
                }
                batch.execute();

                handle.execute(
                        "DELETE FROM " + tableName + " t " +
                		"WHERE EXISTS (" +
                		"    SELECT 1 FROM CellsToDelete c " +
                		"    WHERE " + Columns.ROW("t") + " = " + Columns.ROW("c") + " " +
                		"        AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("c") + " " +
                		"        AND " + Columns.TIMESTAMP("t") + " = " + Columns.TIMESTAMP("c") + " " +
                		"    LIMIT 1)");

                handle.execute("DROP TABLE CellsToDelete");
                return null;
            }
        });
    }

    private int getMaxRows(RangeRequest rangeRequest) {
        return rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPage(final String tableName,
                                                                          final RangeRequest rangeRequest,
                                                                          final long timestamp) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        final int maxRows = getMaxRows(rangeRequest);
        return getDbi().withHandle(new HandleCallback<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Value>, byte[]> withHandle(Handle handle) throws Exception {
                List<byte[]> rows = handle.createQuery(
                        "SELECT DISTINCT " + Columns.ROW + " FROM " + tableName + " " +
                		"WHERE " + Columns.ROW + " >= :startRow AND " + Columns.TIMESTAMP + " < :timestamp " +
        				"LIMIT :limit")
                        .bind("startRow", rangeRequest.getStartInclusive())
                        .bind("timestamp", timestamp)
                        .bind("limit", maxRows)
                        .map(ByteArrayMapper.FIRST)
                        .list();
                if (rows.isEmpty()) {
                    return new SimpleTokenBackedResultsPage<RowResult<Value>, byte[]>(
                            rangeRequest.getStartInclusive(),
                            Collections.<RowResult<Value>> emptyList(),
                            false);
                }
                Map<Cell, Value> cells = getRows(tableName, rows, ColumnSelection.all(), timestamp);
                NavigableMap<byte[], SortedMap<byte[], Value>> byRow = Cells.breakCellsUpByRow(cells);
                List<RowResult<Value>> finalResult = Lists.newArrayList();
                for (Entry<byte[], SortedMap<byte[], Value>> e : byRow.entrySet()) {
                    finalResult.add(RowResult.create(e.getKey(), e.getValue()));
                }
                byte[] token = RangeRequests.getNextStartRow(rangeRequest.isReverse(), rows.get(rows.size() - 1));
                return new SimpleTokenBackedResultsPage<RowResult<Value>, byte[]>(token, finalResult, rows.size() == maxRows);
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

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw new UnsupportedOperationException();
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
        return getDbi().withHandle(new HandleCallback<TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> withHandle(Handle handle) throws Exception {
                List<byte[]> rows = handle.createQuery(
                        "SELECT DISTINCT row FROM " + tableName + " WHERE row >= :startRow AND timestamp < :timestamp LIMIT :limit")
                        .bind("startRow", rangeRequest.getStartInclusive())
                        .bind("timestamp", timestamp)
                        .bind("limit", maxRows)
                        .map(ByteArrayMapper.FIRST)
                        .list();
                if (rows.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(
                            rangeRequest.getStartInclusive(),
                            Collections.<RowResult<Set<Long>>> emptyList(),
                            false);
                }

                handle.execute("DECLARE TABLE RowsToRetrieve (row BYTEA NOT NULL)");
                PreparedBatch batch = handle.prepareBatch("INSERT INTO RowsToRetrieve (row) VALUES (:row)");
                for (byte[] row : rows) {
                    batch.add(row);
                }
                batch.execute();

                List<Cell> cells = handle.createQuery(
                        "SELECT row, column, timestamp FROM " + tableName + " " +
                		"WHERE row IN (" +
                		"    SELECT row FROM RowsToRetrieve)" +
                		"    AND timestamp < :timestamp")
                        .bind("timestamp", timestamp)
                        .map(new ResultSetMapper<Cell>() {
                            @Override
                            public Cell map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException {
                                return Cell.create(r.getBytes("row"), r.getBytes("column"));
                            }
                        })
                        .list();
                if (cells.isEmpty()) {
                    return new SimpleTokenBackedResultsPage<RowResult<Set<Long>>, byte[]>(
                            rangeRequest.getStartInclusive(),
                            Collections.<RowResult<Set<Long>>> emptyList(),
                            false);
                }
                List<RowResult<Set<Long>>> finalResult = Lists.newArrayList();
                SetMultimap<Cell, Long> timestamps = getAllTimestamps(tableName, ImmutableSet.copyOf(cells), timestamp);
                Map<byte[], SortedMap<byte[], Set<Long>>> s = Cells.breakCellsUpByRow(Multimaps.asMap(timestamps));
                for (Entry<byte[], SortedMap<byte[], Set<Long>>> e : s.entrySet()) {
                    finalResult.add(RowResult.create(e.getKey(), e.getValue()));
                }
                byte[] token = RangeRequests.getNextStartRow(rangeRequest.isReverse(), cells.get(cells.size() - 1).getRowName());
                return new SimpleTokenBackedResultsPage<RowResult<Set<Long>>, byte[]>(token, finalResult, rows.size() == maxRows);
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
                		"atlasdb_row BYTEA NOT NULL, " +
                		"atlasdb_column BYTEA NOT NULL, " +
                		"atlasdb_timestamp INT NOT NULL, " +
                		"atlasdb_content BYTEA NOT NULL)");
                handle.execute("INSERT INTO " + MetaTable.META_TABLE_NAME + " (" +
                        "table_name, metadata) VALUES (" +
                        "?, ?)", tableName, new byte[0]);
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
        // TODO: Can it fail if the cell did not exist originally?
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                for (Cell c : cells) {
                    handle.execute(
                            "UPDATE " + tableName + " SET (" + Columns.CONTENT + ", "
                                    + Columns.TIMESTAMP + ") VALUES (?, ?) WHERE " + Columns.ROW
                                    + "  = ? AND " + Columns.COLUMN + " = ?",
                            new byte[0],
                            Value.INVALID_VALUE_TIMESTAMP,
                            c.getRowName(),
                            c.getColumnName());
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public SetMultimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {

        return getDbi().withHandle(new HandleCallback<SetMultimap<Cell, Long>>() {
            @Override
            public SetMultimap<Cell, Long> withHandle(Handle handle) throws Exception {

                handle.execute(
                        "CREATE TEMPORARY TABLE CellsToRetrieve (" +
                        "    " + Columns.ROW + " BYTEA NOT NULL, " +
                        "    " + Columns.COLUMN + " BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO CellsToRetrieve (" +
                        "    " + Columns.ROW + ", " +
                        "    " + Columns.COLUMN + ") VALUES (" +
                        ":row, :column)");
                for (Cell c : cells) {
                    batch.add(c.getRowName(), c.getColumnName());
                }
                batch.execute();

                List<Pair<Cell, Long>> sqlResult = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_AS("t") + " " +
                        "FROM " + tableName + " t " +
                        "JOIN CellsToRetrieve c " +
                        "ON " + Columns.ROW("t") + " = " + Columns.ROW("c") + " " +
                        "    AND " + Columns.COLUMN("t") + " = " + Columns.COLUMN("c") + " " +
                        "WHERE " + Columns.TIMESTAMP("t") + " < " + timestamp)
                        .map(new ResultSetMapper<Pair<Cell, Long>>() {
                            @Override
                            public Pair<Cell, Long> map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException {
                                byte[] row = r.getBytes(Columns.ROW);
                                byte[] column = r.getBytes(Columns.COLUMN);
                                long timestamp = r.getLong(Columns.TIMESTAMP);
                                Cell cell = Cell.create(row, column);
                                return Pair.create(cell, timestamp);
                            }
                        }).list();


                SetMultimap<Cell, Long> result = HashMultimap.create();
                for (Pair<Cell, Long> p : sqlResult) {
                    result.put(p.getLhSide(), p.getRhSide());
                }

                handle.execute("DROP TABLE CellsToRetrieve");

                return result;
            }
        });
    }

    @Override
    public void compactInternally(String tableName) {
        // TODO Auto-generated method stub
    }

}
