/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.rdbms;

import static com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils.MAX_TABLE_NAME_LEN;
import static com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils.USR_TABLE;
import static com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils.batch;
import static com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils.getBatchSize;
import static com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils.makeSlots;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import javax.ws.rs.QueryParam;

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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
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
import com.palantir.atlasdb.keyvalue.impl.SimpleKvsTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.rdbms.utils.AtlasSqlUtils;
import com.palantir.atlasdb.keyvalue.rdbms.utils.CellMapper;
import com.palantir.atlasdb.keyvalue.rdbms.utils.CellValueMapper;
import com.palantir.atlasdb.keyvalue.rdbms.utils.Columns;
import com.palantir.atlasdb.keyvalue.rdbms.utils.MetaTable;
import com.palantir.atlasdb.keyvalue.rdbms.utils.TimestampMapper;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.collect.Maps2;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Pair;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public final class PostgresKeyValueService extends AbstractKeyValueService {

    // *** Connection / driver ********************************************************************
    private final DBI dbi;
    private DBI getDbi() {
        return dbi;
    }

    // *** Construction ***************************************************************************
    public PostgresKeyValueService(DataSource dataSource, ExecutorService executor) {
        super(executor);
        dbi = new DBI(dataSource);
    }

    public PostgresKeyValueService(DataSource dataSource) {
        this(dataSource, PTExecutors.newCachedThreadPool());
    }

    public static PostgresKeyValueService create(PostgresKeyValueConfiguration config) {
        PoolingDataSource ds = new PoolingDataSource();
        ds.setServerName(config.getHost());
        ds.setPortNumber(config.getPort());
        ds.setDatabaseName(config.getDb());
        ds.setUser(config.getUser());
        ds.setPassword(config.getPassword());
        return new PostgresKeyValueService(ds);
    }

    // *** Initialization and teardown ************************************************************
    @Override
    public void initializeFromFreshInstance() {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle conn) throws Exception {
                conn.execute("CREATE TABLE IF NOT EXISTS " + MetaTable.META_TABLE_NAME + " ("
                        + MetaTable.Columns.TABLE_NAME + " VARCHAR(" + MAX_TABLE_NAME_LEN + "), "
                        + MetaTable.Columns.METADATA + " BYTEA NOT NULL, "
                        + "PRIMARY KEY (" + MetaTable.Columns.TABLE_NAME + "))");
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
                conn.execute("DELETE FROM " + MetaTable.META_TABLE_NAME);
                return null;
            }
        });
        super.teardown();
    };

    // *** getRows ********************************************************************************
    private Map<Cell, Value> getRowsInternal(final String tableName,
                                    final Collection<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        if (rows.isEmpty()) {
            return Maps.newHashMap();
        }

        return getDbi().inTransaction(new TransactionCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> inTransaction(Handle handle, TransactionStatus status) {

                final List<Pair<Cell, Value>> list;

                if (columnSelection.allColumnsSelected()) {
                    final Query<Pair<Cell,Value>> query = handle.createQuery(
                            "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                            "FROM " + USR_TABLE(tableName, "t") + " " +
                            "LEFT JOIN " + USR_TABLE(tableName, "t2") + " " +
                            "ON " + Columns.ROW("t").eq(Columns.ROW("t2"))
                                    .and(Columns.COLUMN("t")).eq(Columns.COLUMN("t2"))
                                    .and(Columns.TIMESTAMP("t")).lt(Columns.TIMESTAMP("t2"))
                                    .and(Columns.TIMESTAMP("t2")) + "<" + timestamp + " " +
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
                            "FROM " + USR_TABLE(tableName, "t") + " " +
                            "LEFT JOIN " + USR_TABLE(tableName, "t2") + " " +
                            "ON " + Columns.ROW("t").eq(Columns.ROW("t2"))
                                    .and(Columns.COLUMN("t").eq(Columns.COLUMN("t2")))
                                    .and(Columns.TIMESTAMP("t")).lt(Columns.TIMESTAMP("t2"))
                                    .and(Columns.TIMESTAMP("t2")) + "<" + timestamp + " " +
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
                return AtlasSqlUtils.listToMap(list);
            }
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
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


    // *** get ************************************************************************************
    private Map<Cell, Value> getInternal(final String tableName,
                                         final Collection<Entry<Cell, Long>> timestampByCell) {
        return getDbi().withHandle(new HandleCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> withHandle(Handle handle) throws Exception {
                Query<Pair<Cell,Value>> query = handle.createQuery(
                        "WITH matching_cells AS (" +
                        "    SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                        "    FROM " + USR_TABLE(tableName, "t") +
                        "    JOIN (VALUES " + makeSlots("cell", timestampByCell.size(), 3) + ") " +
                        "        AS t2(" + Columns.ROW.comma(Columns.COLUMN).comma(Columns.TIMESTAMP) + ")" +
                        "    ON " + Columns.ROW("t").eq(Columns.ROW("t2"))
                                .and(Columns.COLUMN("t").eq(Columns.COLUMN("t2")))
                                .and(Columns.TIMESTAMP("t").lt(Columns.TIMESTAMP("t2"))) + ") " +
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                        "FROM matching_cells t " +
                        "LEFT JOIN matching_cells t2 " +
                        "ON " + Columns.ROW("t").eq(Columns.ROW("t2"))
                                .and(Columns.COLUMN("t").eq(Columns.COLUMN("t2")))
                                .and(Columns.TIMESTAMP("t").lt(Columns.TIMESTAMP("t2"))).appendSpace() +
                        "WHERE " + Columns.TIMESTAMP("t2") + " IS NULL")
                        .map(CellValueMapper.instance());
                AtlasSqlUtils.bindCellsTimestamps(query, timestampByCell);
                return AtlasSqlUtils.listToMap(query.list());
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

    // *** put ************************************************************************************
    private void putInternalInTransaction(final String tableName,
                             final Collection<Entry<Cell, byte[]>> values,
                             final long timestamp, Handle handle) {
                Update update = handle.createStatement(
                        "INSERT INTO " + USR_TABLE(tableName) + " (" +
                                Columns.ROW.comma(Columns.COLUMN).comma(
                                        Columns.TIMESTAMP).comma(Columns.CONTENT) +
                                ") VALUES " + makeSlots("cell", values.size(), 4));
                AtlasSqlUtils.bindCellsValues(update, values, timestamp);
                update.execute();
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        try {
            batch(values.entrySet(), new Function<Collection<Entry<Cell, byte[]>>, Void>() {
                @Override @Nullable
                public Void apply(@Nullable final Collection<Entry<Cell, byte[]>> input) {
                    getDbi().withHandle(new HandleCallback<Void>() {
                        @Override
                        public Void withHandle(Handle handle) throws Exception {
                            deleteInternalInTransaction(tableName, Maps.transformValues(values, Functions.constant(timestamp)).entrySet(), handle);
                            putInternalInTransaction(tableName, input, timestamp, handle);
                            return null;
                        }
                    });
                    return null;
                }
            });
        } catch (RuntimeException e) {
            if (AtlasSqlUtils.isKeyAlreadyExistsException(e)) {
                throw new KeyAlreadyExistsException("Unique constraint violation", e);
            }
            throw e;
        }
    }

    private void putInTransaction(final String tableName, final Map<Cell, byte[]> values,
                                  final long timestamp, final Handle handle)
            throws KeyAlreadyExistsException {
        try {
            batch(values.entrySet(), new Function<Collection<Entry<Cell, byte[]>>, Void>() {
                @Override @Nullable
                public Void apply(@Nullable final Collection<Entry<Cell, byte[]>> input) {
                            putInternalInTransaction(tableName, input, timestamp, handle);
                    return null;
                }
            });
        } catch (RuntimeException e) {
            if (AtlasSqlUtils.isKeyAlreadyExistsException(e)) {
                throw new KeyAlreadyExistsException("Unique constraint violation", e);
            }
            throw e;
        }
    }

    // *** putWithTimestamps **********************************************************************
    private void putWithTimestampsInternalInTransaction(final String tableName, final Collection<Entry<Cell, Value>> cellValues,
                                                        final Handle handle) {
        Update update = handle.createStatement(
                "INSERT INTO " + USR_TABLE(tableName) + " (" +
                    Columns.ROW.comma(Columns.COLUMN).comma(
                    Columns.TIMESTAMP).comma(Columns.CONTENT) +
                ") VALUES " + makeSlots("cell", cellValues.size(), 4));
        AtlasSqlUtils.bindCellsValues(update, cellValues);
        update.execute();
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {

        // Validate the cellValues
        for (Entry<Cell, Value> e : cellValues.entries()) {
            for (Value val : cellValues.get(e.getKey())) {
                if (e.getValue() != val) {
                    assert e.getValue().getTimestamp() != val.getTimestamp();
                }
            }
        }

        batch(cellValues.entries(), new Function<Collection<Entry<Cell, Value>>, Void>() {
            @Override
            @Nullable
            public Void apply(@Nullable final Collection<Entry<Cell, Value>> input) {
                return getDbi().inTransaction(new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Handle conn,
                                              TransactionStatus status) throws Exception {
                        deleteInternalInTransaction(tableName, Collections2.transform(input, new Function<Entry<Cell, Value>, Entry<Cell, Long>>() {
                            @Override
                            public Entry<Cell, Long> apply(
                                    Entry<Cell, Value> input) {
                                return new AbstractMap.SimpleEntry<Cell, Long>(input.getKey(), input.getValue().getTimestamp());
                            }
                        }), conn);
                        putWithTimestampsInternalInTransaction(tableName, input, conn);
                        return null;
                    }
                });
            }
        });
    }

    // *** putUnlessExists ************************************************************************
    @Override
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        try {
            batch(values.entrySet(), new Function<Collection<Entry<Cell, byte[]>>, Void>() {
                @Override @Nullable
                public Void apply(@Nullable final Collection<Entry<Cell, byte[]>> input) {
                    getDbi().withHandle(new HandleCallback<Void>() {
                        @Override
                        public Void withHandle(Handle handle) throws Exception {
                            putInternalInTransaction(tableName, input, 0L, handle);
                            return null;
                        }
                    });
                    return null;
                }
            });
        } catch (RuntimeException e) {
            if (AtlasSqlUtils.isKeyAlreadyExistsException(e)) {
                throw new KeyAlreadyExistsException("Unique constraint violation", e);
            }
            throw e;
        }
    }

    // *** delete *********************************************************************************
    private void deleteInternalInTransaction(final String tableName, final Collection<Entry<Cell, Long>> keys, Handle handle) {
            Update update = handle.createStatement(
                    "DELETE FROM " + USR_TABLE(tableName) + " t " +
                    "WHERE (" + Columns.ROW.comma(Columns.COLUMN).comma(Columns.TIMESTAMP) + ") " +
                    "    IN (" + makeSlots("cell", keys.size(), 3) + ") ");
            AtlasSqlUtils.bindCellsTimestamps(update, keys);
            update.execute();
    }

    private void deleteInTransaction(final String tableName, final Multimap<Cell, Long> keys, final Handle handle) {
            batch(keys.entries(), new Function<Collection<Entry<Cell, Long>>, Void>() {
                @Override
                @Nullable
                public Void apply(@Nullable Collection<Entry<Cell, Long>> input) {
                    deleteInternalInTransaction(tableName, input, handle);
                    return null;
                }
            });
    }

    /**
     * Performs entire batched delete in a single transaction.
     * TODO: Alternatively sort the keys and values and then
     *       split into multiple transaction.
     * @param tableName
     * @param keys
     * @param handle
     *
     */
    @Override @Idempotent
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(final Handle conn, TransactionStatus status) throws Exception {
                // Just perform entire delete in a single transaction.
                deleteInTransaction(tableName, keys, conn);
                return null;
            }
        });
    }

    // *** truncate *******************************************************************************
    @Override
    public void truncateTable(final String tableName) throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute("TRUNCATE TABLE " + USR_TABLE(tableName));
                return null;
            }
        });
    }

    // *** getRange *******************************************************************************
    private Collection<byte[]> getRowsInRange(String tableName,
                                        RangeRequest rangeRequest,
                                        long timestamp,
                                        int limit,
                                        Handle handle) {
        if (rangeRequest.isReverse()) {
            return handle.createQuery(
                    "SELECT DISTINCT " + Columns.ROW + " FROM " + USR_TABLE(tableName) + " " +
                    "WHERE " + Columns.ROW + " <= :startRow" +
                    "    AND " + Columns.ROW + " > :endRow " +
                    "    AND " + Columns.TIMESTAMP + " < :timestamp " +
                    "LIMIT :limit")
                    .bind("startRow", RangeRequests.startRowInclusiveOrLargestRow(rangeRequest))
                    .bind("endRow", rangeRequest.getEndExclusive())
                    .bind("timestamp", timestamp)
                    .bind("limit", limit)
                    .map(ByteArrayMapper.FIRST)
                    .list();
        } else {
            return handle.createQuery(
                    "SELECT DISTINCT " + Columns.ROW + " FROM " + USR_TABLE(tableName) + " " +
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
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPage(final String tableName,
                                                                          final RangeRequest rangeRequest,
                                                                          final long timestamp) {
        final int maxRows = getBatchSize(rangeRequest);
        return getDbi().withHandle(new HandleCallback<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Value>, byte[]> withHandle(Handle handle)
                    throws Exception {
                Collection<byte[]> rows = getRowsInRange(tableName, rangeRequest,
                        timestamp, maxRows, handle);
                if (rows.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create( rangeRequest.getStartInclusive(),
                            Collections.<RowResult<Value>> emptyList(), false);
                }
                ColumnSelection columns = RangeRequests.extractColumnSelection(rangeRequest);
                Map<Cell, Value> cells = getRows(tableName, rows, columns, timestamp);

                final Ordering<RowResult<Value>> ordering = RowResult.<Value>getOrderingByRowName(rangeRequest.isReverse());
                final SortedSet<RowResult<Value>> result = ImmutableSortedSet.orderedBy(ordering)
                            .addAll(AtlasSqlUtils.cellsToRows(cells)).build();

                return SimpleTokenBackedResultsPage.create(
                        AtlasSqlUtils.generateToken(rangeRequest, result.last().getRowName()),
                        assertThatRowResultsAreOrdered(result, rangeRequest), rows.size() == maxRows);
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
                return getPage(tableName, rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(), timestamp);
            }
        }.iterator());
    }

    // *** getRangeWithHistory ********************************************************************
    private ListMultimap<Cell, Value> getAllVersionsInternal(final String tableName,
                                                            final Collection<byte[]> rows,
                                                            final ColumnSelection columns,
                                                            final long timestamp)
            throws InsufficientConsistencyException {
        return getDbi().withHandle(new HandleCallback<ListMultimap<Cell, Value>>() {
            @Override
            public ListMultimap<Cell, Value> withHandle(Handle handle) throws Exception {
                Query<Pair<Cell, Value>> query = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_CONTENT_AS("t") + " " +
                        "FROM " + USR_TABLE(tableName, "t") + " " +
                        "WHERE (" + Columns.ROW + ") IN (" +
                        "    " + makeSlots("row", rows.size(), 1) + ") " +
                        "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                        .map(CellValueMapper.instance());
                AtlasSqlUtils.bindAll(query, rows);

                // Validate proper row ordering
                List<Pair<Cell, Value>> list = query.list();
                ListMultimap<Cell, Value> ret = AtlasSqlUtils.listToListMultimap(list);
                return ret;
            }
        });
    }

    private static <T extends Iterable<byte[]>> T assertThatRowsAreOrdered(T cells, RangeRequest rangeRequest) {
        Iterator<byte[]> it = cells.iterator();
        if (!it.hasNext()) {
            return cells;
        }

        final boolean reverse = rangeRequest.isReverse();
        final byte[] startRow = rangeRequest.getStartInclusive();
        final byte[] endRow = rangeRequest.getEndExclusive();

        byte[] row = it.next();

        // Make sure that the first row is after startRow
        // Only care if there is a lower-bound requested
        if (startRow.length > 0) {
            if (!reverse) {
                assert UnsignedBytes.lexicographicalComparator().compare(startRow, row) <= 0;
            } else {
                assert UnsignedBytes.lexicographicalComparator().compare(startRow, row) >= 0;
            }
        }

        // Make sure that rows are ordered
        while (it.hasNext()) {
            byte[] nextRow = it.next();

            if (!reverse) {
                assert UnsignedBytes.lexicographicalComparator().compare(row, nextRow) <= 0;
            } else {
                assert UnsignedBytes.lexicographicalComparator().compare(row, nextRow) >= 0;
            }

            row = nextRow;
        }

        // Make sure that the last row is before endRow
        // Only care if there is an upper-bound requested
        if (endRow.length > 0) {
            if (!reverse) {
                assert UnsignedBytes.lexicographicalComparator().compare(row, endRow) < 0;
            } else {
                assert UnsignedBytes.lexicographicalComparator().compare(row, endRow) > 0;
            }
        }

        return cells;
    }

    private static <U, T extends Iterable<RowResult<U>>> T assertThatRowResultsAreOrdered(T rows, RangeRequest rangeRequest) {
        assertThatRowsAreOrdered(Iterables.transform(rows, new Function<RowResult<?>, byte[]>() {
            @Override
            public byte[] apply(RowResult<?> input) {
                return input.getRowName();
            }
        }), rangeRequest);
        return rows;
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> getPageWithHistory(final String tableName,
                                                                                  final RangeRequest rangeRequest,
                                                                                  final long timestamp) {
        final int maxRows = getBatchSize(rangeRequest);
        return getDbi().withHandle(
                new HandleCallback<TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]>>() {
                    @Override
                    public TokenBackedBasicResultsPage<RowResult<Set<Value>>, byte[]> withHandle(Handle handle)
                            throws Exception {
                        Collection<byte[]> rows = getRowsInRange(tableName, rangeRequest, timestamp, maxRows, handle);
                        if (rows.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(rangeRequest.getStartInclusive(),
                                    Collections.<RowResult<Set<Value>>> emptyList(), false);
                        }
                        ListMultimap<Cell, Value> timestamps = getAllVersionsInternal(tableName, rows,
                                RangeRequests.extractColumnSelection(rangeRequest), timestamp);

                        final Ordering<RowResult<Set<Value>>> ordering = RowResult.getOrderingByRowName(rangeRequest.isReverse());
                        final SortedSet<RowResult<Set<Value>>> result = ImmutableSortedSet.orderedBy(ordering)
                                .addAll(AtlasSqlUtils.cellsToRows(timestamps)).build();

                        return SimpleTokenBackedResultsPage.create(
                                AtlasSqlUtils.generateToken(rangeRequest, result.last().getRowName()),
                                assertThatRowResultsAreOrdered(result, rangeRequest),
                                rows.size() == maxRows);
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
                return getPageWithHistory(tableName,
                        rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(), timestamp);
            }

        }.iterator());
    }

    // *** getRangeOfTimestamps *******************************************************************
    private ListMultimap<Cell, Long> getAllTimestampsInternal(final String tableName,
                                                            final Collection<byte[]> rows,
                                                            final ColumnSelection columns,
                                                            final long timestamp)
            throws InsufficientConsistencyException {
        return getDbi().withHandle(new HandleCallback<ListMultimap<Cell, Long>>() {
            @Override
            public ListMultimap<Cell, Long> withHandle(Handle handle) throws Exception {
                Query<Pair<Cell,Long>> query = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_AS("t") + " " +
                        "FROM " + USR_TABLE(tableName, "t") + " " +
                        "WHERE (" + Columns.ROW + ") IN (" +
                        "    " + makeSlots("row", rows.size(), 1) + ") " +
                        "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                        .map(new ResultSetMapper<Pair<Cell, Long>>() {
                            @Override
                            public Pair<Cell, Long> map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException {
                                Cell cell = CellMapper.instance().map(index, r, ctx);
                                long timestamp = TimestampMapper.instance().map(index, r, ctx);
                                return Pair.create(cell, timestamp);
                            }
                        });
                AtlasSqlUtils.bindAll(query, rows);
                ListMultimap<Cell, Long> result = AtlasSqlUtils.listToListMultimap(query.list());
                return result;
            }
        });
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getPageOfTimestamps(final String tableName,
                                                                                          final RangeRequest rangeRequest,
                                                                                          final long timestamp) {
        final int maxRows = getBatchSize(rangeRequest);
        return getDbi().withHandle(
                new HandleCallback<TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
                    @Override
                    public TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> withHandle(Handle handle)
                            throws Exception {
                        Collection<byte[]> rows = getRowsInRange(tableName, rangeRequest,
                                timestamp, maxRows, handle);
                        if (rows.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(rangeRequest.getStartInclusive(),
                                    Collections.<RowResult<Set<Long>>> emptyList(), false);
                        }

                        ColumnSelection columns = RangeRequests.extractColumnSelection(rangeRequest);
                        ListMultimap<Cell,Long> timestamps = getAllTimestampsInternal(tableName, rows, columns, timestamp);

                        final Ordering<RowResult<Set<Long>>> ordering = RowResult.<Set<Long>>getOrderingByRowName(rangeRequest.isReverse());
                        final SortedSet<RowResult<Set<Long>>> result = ImmutableSortedSet.orderedBy(ordering)
                                .addAll(AtlasSqlUtils.cellsToRows(timestamps)).build();

                        return new SimpleTokenBackedResultsPage<RowResult<Set<Long>>, byte[]>(
                                AtlasSqlUtils.generateToken(rangeRequest, result.last().getRowName()),
                                assertThatRowResultsAreOrdered(result, rangeRequest),
                                rows.size() == maxRows);
                    }
                });
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
                return getPageOfTimestamps(tableName,
                        rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(), timestamp);
            }
            }.iterator());
    }

    // *** getAllTimestamps ***********************************************************************
    private ListMultimap<Cell, Long> getAllTimestampsInternal(final String tableName,
                                                 final Collection<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {

        return getDbi().withHandle(new HandleCallback<ListMultimap<Cell, Long>>() {
            @Override
            public ListMultimap<Cell,Long> withHandle(Handle handle) throws Exception {
                Query<Pair<Cell, Long>> query = handle.createQuery(
                        "SELECT " + Columns.ROW_COLUMN_TIMESTAMP_AS("t") + " " +
                        "FROM " + USR_TABLE(tableName, "t") + " " +
                        "WHERE (" + Columns.ROW.comma(Columns.COLUMN) + ") IN (" +
                        "    " + makeSlots("cell", cells.size(), 2) + ") " +
                        "    AND " + Columns.TIMESTAMP("t") + " < " + timestamp)
                        .map(new ResultSetMapper<Pair<Cell, Long>>() {
                            @Override
                            public Pair<Cell, Long> map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException {
                                Cell cell = CellMapper.instance().map(index, r, ctx);
                                long timestamp = TimestampMapper.instance().map(index, r, ctx);
                                return Pair.create(cell, timestamp);
                            }
                        });
                AtlasSqlUtils.bindCells(query, cells);
                ListMultimap<Cell,Long> result = AtlasSqlUtils.listToListMultimap(query.list());
                return result;
            }
        });
    }

    /**
     * The rows are NOT sorted here!
     *
     * @param tableName
     * @param cells
     * @param timestamp
     * @return
     * @throws InsufficientConsistencyException
     */
    @Override
    @Idempotent
    public SetMultimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {
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

    // *** Miscellaneous **************************************************************************
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(
                this, tableName, rangeRequests, timestamp);
    }

    @Override
    @Idempotent
    public void dropTable(final String tableName) throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute("DROP TABLE IF EXISTS " + USR_TABLE(tableName));
                handle.execute(
                        "DELETE FROM " + MetaTable.META_TABLE_NAME + " " +
                        "WHERE " + MetaTable.Columns.TABLE_NAME + " = ?",
                        tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final String tableName, final byte[] tableMetadata)
            throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute(
                        "CREATE TABLE IF NOT EXISTS " + USR_TABLE(tableName) + " ( " +
                		"    " + Columns.ROW + " BYTEA NOT NULL, " +
                		"    " + Columns.COLUMN + " BYTEA NOT NULL, " +
                		"    " + Columns.TIMESTAMP + " INT NOT NULL, " +
                		"    " + Columns.CONTENT + " BYTEA NOT NULL," +
        				"    PRIMARY KEY (" +
        				"        " + Columns.ROW + ", " +
						"        " + Columns.COLUMN + ", " +
        				"        " + Columns.TIMESTAMP + "))");
                try {
                    handle.execute("INSERT INTO " + MetaTable.META_TABLE_NAME + " (" +
                            "    " + MetaTable.Columns.TABLE_NAME + ", " +
                            "    " + MetaTable.Columns.METADATA + " ) VALUES (" +
                            "    ?, ?)", tableName, tableMetadata);
                } catch (RuntimeException e) {
                    if (AtlasSqlUtils.isKeyAlreadyExistsException(e)) {
                        // The table has existed perviously: no-op
                    } else {
                        throw e;
                    }
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        Set<String> hiddenTables = ImmutableSet.of(
                MetaTable.META_TABLE_NAME, SimpleKvsTimestampBoundStore.TIMESTAMP_TABLE);
        Set<String> allTables = Sets.newHashSet(getDbi().withHandle(new HandleCallback<List<String>>() {
            @Override
            public List<String> withHandle(Handle handle) throws Exception {
                return handle.createQuery(
                        "SELECT " + MetaTable.Columns.TABLE_NAME + " " +
                        "FROM " + MetaTable.META_TABLE_NAME)
                        .map(StringMapper.FIRST)
                        .list();
            }
        }));
        return Sets.difference(allTables, hiddenTables);
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(final String tableName) {
        byte[] ret = getDbi().withHandle(new HandleCallback<byte[]>() {
            @Override
            public byte[] withHandle(Handle conn) throws Exception {
                return conn.createQuery(
                        "SELECT " + MetaTable.Columns.METADATA + " " +
                        "FROM " + MetaTable.META_TABLE_NAME + " " +
                        "WHERE " + MetaTable.Columns.TABLE_NAME + " = :tableName")
                        .bind("tableName", tableName)
                        .map(ByteArrayMapper.FIRST)
                        .first();
            }
        });
        return Preconditions.checkNotNull(ret);
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final String tableName, final byte[] metadata) {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                conn.execute(
                        "UPDATE " + MetaTable.META_TABLE_NAME + " " +
                        "SET " + MetaTable.Columns.METADATA + " = ? " +
                        "WHERE " + MetaTable.Columns.TABLE_NAME + " = ?",
                        metadata, tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName, final Set<Cell> cells) {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                Map<Cell, byte[]> cellsWithInvalidValues = Maps2.createConstantValueMap(
                        cells, ArrayUtils.EMPTY_BYTE_ARRAY);
                Multimap<Cell, Long> cellsAsMultimap = Multimaps.forMap(Maps2.createConstantValueMap(
                        cells, Value.INVALID_VALUE_TIMESTAMP));
                deleteInTransaction(tableName, cellsAsMultimap, conn);
                putInTransaction(tableName, cellsWithInvalidValues, Value.INVALID_VALUE_TIMESTAMP, conn);
                return null;
            }
        });
    }

    @Override
    public void compactInternally(String tableName) {
        // TODO Auto-generated method stub
    }

}
