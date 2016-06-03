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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Atomics;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.base.Visitors;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimers;
import com.palantir.nexus.db.pool.HikariCPConnectionManager;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SQL;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.nexus.db.sql.SqlConnectionHelper;
import com.palantir.nexus.db.sql.SqlConnectionImpl;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class DbKvs extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(DbKvs.class);
    private final DbKeyValueServiceConfig config;
    private final DbTableFactory dbTables;
    private final SqlConnectionSupplier connections;

    public static DbKvs create(DbKeyValueServiceConfig config) {
        Preconditions.checkArgument(config.connection().isPresent(),
                "Connection configuration is not present. You must have a connection block in your atlas config.");
        HikariCPConnectionManager connManager = new HikariCPConnectionManager(config.connection().get(), Visitors.emptyVisitor());
        ReentrantManagedConnectionSupplier connSupplier = new ReentrantManagedConnectionSupplier(connManager);
        SqlConnectionSupplier sqlConnSupplier = getSimpleTimedSqlConnectionSupplier(connSupplier);

        return new DbKvs(config, config.tableFactorySupplier().get(), sqlConnSupplier);
    }

    private static SqlConnectionSupplier getSimpleTimedSqlConnectionSupplier(ReentrantManagedConnectionSupplier connectionSupplier) {
        Supplier<Connection> supplier = () -> connectionSupplier.get();
        SQL sql = new SQL() {
            @Override
            protected SqlConfig getSqlConfig() {
                return new SqlConfig() {
                    @Override
                    public boolean isSqlCancellationDisabled() {
                        return false;
                    }

                    protected Iterable<SqlTimer> getSqlTimers() {
                        return ImmutableList.of(
                                SqlTimers.createDurationSqlTimer(),
                                SqlTimers.createSqlStatsSqlTimer());
                    }

                    @Override
                    final public SqlTimer getSqlTimer() {
                        return SqlTimers.createCombinedSqlTimer(getSqlTimers());
                    }
                };
            }
        };

        return new SqlConnectionSupplier() {
            @Override
            public SqlConnection get() {
                return new SqlConnectionImpl(supplier, new SqlConnectionHelper(sql));
            }

            @Override
            public void close() {
                connectionSupplier.close();
            }
        };
    }

    /**
     * Constructor for a SQL (either Postgres or Oracle) backed key value store.  Exposed as public
     * for use by a legacy internal product that needs to supply it's own connection supplier.
     * Use {@link #create(DbKeyValueServiceConfig)} instead.
     */
    public DbKvs(DbKeyValueServiceConfig config,
                 DbTableFactory dbTables,
                 SqlConnectionSupplier connections) {
        super(AbstractKeyValueService.createFixedThreadPool("Atlas Relational KVS", config.shared().poolSize()));
        this.config = config;
        this.dbTables = dbTables;
        this.connections = connections;
    }

    public DbKeyValueServiceConfig getConfig() {
        return config;
    }

    @Override
    public void initializeFromFreshInstance() {
        // do nothing
    }

    @Override
    public void close() {
        super.close();
        dbTables.close();
        connections.close();
    }

    @Override
    public void teardown() {
        super.teardown();
        close();
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        return runRead(tableRef, new Function<com.palantir.atlasdb.keyvalue.dbkvs.impl.DbReadTable, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DbReadTable table) {
                ClosableIterator<AgnosticLightResultRow> iter = table.getLatestRows(rows, columnSelection, timestamp, true);
                return extractResults(table, iter);
            }
        });
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, final Map<Cell, Long> timestampByCell) {
        return runRead(tableRef, new Function<DbReadTable, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DbReadTable table) {
                ClosableIterator<AgnosticLightResultRow> iter = table.getLatestCells(timestampByCell, true);
                return extractResults(table, iter);
            }
        });
    }

    @SuppressWarnings("deprecation")
    private Map<Cell, Value> extractResults(DbReadTable table, ClosableIterator<AgnosticLightResultRow> iter) {
        boolean hasOverflow = table.hasOverflowValues();
        Map<Cell, Value> results = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowResults = Maps.newHashMap();
        try {
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                Long overflowId = hasOverflow ? row.getLongObject("overflow") : null;
                if (overflowId == null) {
                    Value value = Value.create(row.getBytes("val"), row.getLong("ts"));
                    Value oldValue = results.put(cell, value);
                    if (oldValue != null && oldValue.getTimestamp() > value.getTimestamp()) {
                        results.put(cell, oldValue);
                    }
                } else {
                    OverflowValue ov = new OverflowValue(row.getLong("ts"), overflowId);
                    OverflowValue oldOv = overflowResults.put(cell, ov);
                    if (oldOv != null && oldOv.ts > ov.ts) {
                        overflowResults.put(cell, oldOv);
                    }
                }
            }
        } finally {
            iter.close();
        }
        for (Iterator<Entry<Cell, OverflowValue>> entryIter = overflowResults.entrySet().iterator(); entryIter.hasNext(); ) {
            Entry<Cell, OverflowValue> entry = entryIter.next();
            Value value = results.get(entry.getKey());
            if (value != null && value.getTimestamp() > entry.getValue().ts) {
                entryIter.remove();
            }
        }
        if (!overflowResults.isEmpty()) {
            Map<Long, byte[]> values = Maps.newHashMapWithExpectedSize(overflowResults.size());
            ClosableIterator<AgnosticLightResultRow> overflowIter = table.getOverflow(overflowResults.values());
            try {
                while (overflowIter.hasNext()) {
                    AgnosticLightResultRow row = overflowIter.next();
                    // QA-94468 LONG RAW typed columns ("val" in this case) must be retrieved first from the result set
                    // see https://docs.oracle.com/cd/B19306_01/java.102/b14355/jstreams.htm#i1007581
                    byte[] val = row.getBytes("val");
                    long id = row.getLong("id");
                    values.put(id, val);
                }
            } finally {
                overflowIter.close();
            }
            for (Entry<Cell, OverflowValue> entry : overflowResults.entrySet()) {
                Cell cell = entry.getKey();
                OverflowValue ov = entry.getValue();
                byte[] val = values.get(ov.id);
                Preconditions.checkNotNull(val, "Failed to load overflow data: cell=%s, overflowId=%s", cell, ov.id);
                results.put(cell, Value.create(val, ov.ts));
            }
        }
        return results;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, final Map<Cell, Long> timestampByCell) {
        return runRead(tableRef, new Function<DbReadTable, Map<Cell, Long>>() {
            @Override
            @SuppressWarnings("deprecation")
            public Map<Cell, Long> apply(DbReadTable table) {
                ClosableIterator<AgnosticLightResultRow> iter = table.getLatestCells(timestampByCell, false);
                try {
                    Map<Cell, Long> results = Maps.newHashMap();
                    while (iter.hasNext()) {
                        AgnosticLightResultRow row = iter.next();
                        Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                        long ts = row.getLong("ts");
                        Long oldTs = results.put(cell, ts);
                        if (oldTs != null && oldTs > ts) {
                            results.put(cell, oldTs);
                        }
                    }
                    return results;
                } finally {
                    iter.close();
                }
            }
        });
    }

    public Function<Entry<Cell, byte[]>, Long> getByteSizingFunction() {
        return new Function<Entry<Cell, byte[]>, Long>(){
            @Override
            public Long apply(Entry<Cell, byte[]> entry) {
                return Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().length;
            }
        };
    }

    public Function<Entry<Cell, Value>, Long> getValueSizingFunction() {
        return new Function<Entry<Cell, Value>, Long>(){
            @Override
            public Long apply(Entry<Cell, Value> entry) {
                return Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length;
            }
        };
    }

    private void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp, boolean idempotent) {
        final Iterable<List<Entry<Cell, byte[]>>> batches = partitionByCountAndBytes(
                values.entrySet(), config.shared().mutationBatchCount(), config.shared().mutationBatchSizeBytes(), tableRef, getByteSizingFunction());
        runWrite(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                for (List<Entry<Cell, byte[]>> batch : batches) {
                    try {
                        table.put(batch, timestamp);
                    } catch (KeyAlreadyExistsException e) {
                        if (idempotent) {
                            putIfNotUpdate(tableRef, table, batch, timestamp, e);
                        } else {
                            throw e;
                        }
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void put(TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp) throws KeyAlreadyExistsException {
        put(tableRef, values, timestamp, true);
    }

    private void putIfNotUpdate(TableReference tableRef, DbWriteTable table, List<Entry<Cell, Value>> batch, KeyAlreadyExistsException e) {
        Map<Cell, Long> timestampByCell = Maps.newHashMap();
        for (Entry<Cell, Value> entry : batch) {
            timestampByCell.put(entry.getKey(), entry.getValue().getTimestamp() + 1);
        }
        Map<Cell, Value> results = get(tableRef, timestampByCell);

        ListIterator<Entry<Cell, Value>> iter = batch.listIterator();
        while (iter.hasNext()) {
            Entry<Cell, Value> entry = iter.next();
            Cell key = entry.getKey();
            Value value = entry.getValue();
            if (results.containsKey(key)) {
                if (results.get(key).equals(value)) {
                    iter.remove();
                } else {
                    throw new KeyAlreadyExistsException("primary key violation for key " + key + " with value " + value, e);
                }
            }
        }
        table.put(batch);
    }

    private void putIfNotUpdate(TableReference tableRef, DbWriteTable table, List<Entry<Cell, byte[]>> batch, final long timestamp, KeyAlreadyExistsException e) {
        List<Entry<Cell, Value>> batchValues = Lists.transform(batch, new Function<Entry<Cell, byte[]>, Entry<Cell, Value>>() {
            @Override
            public Entry<Cell, Value> apply(final Entry<Cell, byte[]> input) {
                final Value value = Value.create(input.getValue(), timestamp);
                return new Entry<Cell, Value>() {
                    @Override
                    public Cell getKey() {
                        return input.getKey();
                    }

                    @Override
                    public Value getValue() {
                        return value;
                    }

                    @Override
                    public Value setValue(Value value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        });
        putIfNotUpdate(tableRef, table, batchValues, e);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, final Multimap<Cell, Value> cellValues) throws KeyAlreadyExistsException {
        final Iterable<List<Entry<Cell, Value>>> batches = partitionByCountAndBytes(
                cellValues.entries(), config.shared().mutationBatchCount(), config.shared().mutationBatchSizeBytes(), tableRef, getValueSizingFunction());
        runWrite(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                for (List<Entry<Cell, Value>> batch : batches) {
                    try {
                        table.put(batch);
                    } catch (KeyAlreadyExistsException e) {
                        putIfNotUpdate(tableRef, table, batch, e);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        put(tableRef, values, AtlasDbConstants.TRANSACTION_TS, false);
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        // QA-86494: We sort our deletes here because we have seen oracle deadlock errors here.
        ImmutableList<Entry<Cell, Long>> sorted = ORDERING.immutableSortedCopy(keys.entries());
        final Iterable<List<Entry<Cell, Long>>> partitions =
                partitionByCountAndBytes(sorted, 10000, getMultiPutBatchSizeBytes(), tableRef,
                new Function<Entry<Cell, Long>, Long>() {
            @Override
            public Long apply(Entry<Cell, Long> entry) {
                return Cells.getApproxSizeOfCell(entry.getKey()) + 8;
            }
        });
        runWriteForceAutocommit(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                for (List<Entry<Cell, Long>> partition : partitions) {
                    table.delete(partition);
                }
                return null;
            }
        });
    }

    private static final Ordering<Entry<Cell, Long>> ORDERING = Ordering.from(new Comparator<Entry<Cell, Long>>() {
        @Override
        public int compare(Entry<Cell, Long> entry1, Entry<Cell, Long> entry2) {
            int comparison = Ordering.natural().compare(entry1.getKey(), entry2.getKey());
            if (comparison == 0) {
                comparison = Ordering.natural().compare(entry1.getValue(), entry2.getValue());
            }
            return comparison;
        }
    });

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(final TableReference tableRef,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           final long timestamp) {
        return new DbKvsGetRanges(this, dbTables.getDbType(), connections).getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(final TableReference tableRef,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        Iterable<RowResult<Value>> rows = new AbstractPagingIterable<RowResult<Value>, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getFirstPage() {
                return getPage(tableRef, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getNextPage(
                    TokenBackedBasicResultsPage<RowResult<Value>, byte[]> previous) {
                byte[] newStartRow = previous.getTokenForNextPage();
                final RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(newStartRow).build();
                return getPage(tableRef, newRange, timestamp);
            }
        };
        return ClosableIterators.wrap(rows.iterator());
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(TableReference tableRef,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw new UnsupportedOperationException();
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPage(TableReference tableRef,
                                                                          final RangeRequest range,
                                                                          final long timestamp) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return runRead(tableRef, new Function<DbReadTable, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
                @Override
                public TokenBackedBasicResultsPage<RowResult<Value>, byte[]> apply(DbReadTable table) {
                    return getPageInternal(table, range, timestamp);
                }
            });
        } finally {
            log.info("Call to KVS.getPage on table {} took {} ms.", tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @SuppressWarnings("deprecation")
    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPageInternal(DbReadTable table,
                                                                                  RangeRequest range,
                                                                                  long timestamp) {
        Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
        SortedSet<byte[]> rows = Sets.newTreeSet(comp);
        int maxRows = range.getBatchHint() == null ? 100 : (int) (1.1 * range.getBatchHint());
        ClosableIterator<AgnosticLightResultRow> rangeResults = table.getRange(range, timestamp, maxRows);
        try {
            while (rows.size() < maxRows && rangeResults.hasNext()) {
                byte[] rowName = rangeResults.next().getBytes("row_name");
                if (rowName != null) {
                    rows.add(rowName);
                }
            }
            if (rows.isEmpty()) {
                return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Value>>of(), false);
            }
        } finally {
            rangeResults.close();
        }
        ColumnSelection columns = ColumnSelection.all();
        if (!range.getColumnNames().isEmpty()) {
            columns = ColumnSelection.create(range.getColumnNames());
        }
        ClosableIterator<AgnosticLightResultRow> rowResults = table.getLatestRows(rows, columns, timestamp, true);
        Map<Cell, Value> results = extractResults(table, rowResults);
        NavigableMap<byte[], SortedMap<byte[], Value>> cellsByRow = Cells.breakCellsUpByRow(results);
        if (range.isReverse()) {
            cellsByRow = cellsByRow.descendingMap();
        }
        List<RowResult<Value>> finalResults = Lists.newArrayListWithCapacity(results.size());
        for (Entry<byte[], SortedMap<byte[], Value>> entry : cellsByRow.entrySet()) {
            finalResults.add(RowResult.create(entry.getKey(), entry.getValue()));
        }
        byte[] nextRow = null;
        boolean mayHaveMoreResults = false;
        byte[] lastRow = range.isReverse() ? rows.first() : rows.last();
        if (!RangeRequests.isTerminalRow(range.isReverse(), lastRow)) {
            nextRow = RangeRequests.getNextStartRow(range.isReverse(), lastRow);
            mayHaveMoreResults = rows.size() == maxRows;
        }
        return SimpleTokenBackedResultsPage.create(nextRow, finalResults, mayHaveMoreResults);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(final TableReference tableRef,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        Iterable<RowResult<Set<Long>>> rows = new AbstractPagingIterable<RowResult<Set<Long>>, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getFirstPage() {
                return getTimestampsPage(tableRef, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getNextPage(
                    TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> previous) {
                byte[] newStartRow = previous.getTokenForNextPage();
                final RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(newStartRow).build();
                return getTimestampsPage(tableRef, newRange, timestamp);
            }
        };
        return ClosableIterators.wrap(rows.iterator());
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getTimestampsPage(TableReference tableRef,
                                                                                        final RangeRequest range,
                                                                                        final long timestamp) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return runRead(tableRef, new Function<DbReadTable, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
                @Override
                public TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> apply(DbReadTable table) {
                    return getTimestampsPageInternal(table, range, timestamp);
                }
            });
        } finally {
            log.info("Call to KVS.getTimestampsPage on table {} took {} ms.", tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @SuppressWarnings("deprecation")
    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getTimestampsPageInternal(DbReadTable table,
                                                                                                RangeRequest range,
                                                                                                long timestamp) {
        Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
        SortedSet<byte[]> rows = Sets.newTreeSet(comp);
        int maxRows = range.getBatchHint() == null ? 100 : (int) (1.1 * range.getBatchHint());
        ClosableIterator<AgnosticLightResultRow> rangeResults = table.getRange(range, timestamp, maxRows);
        try {
            while (rows.size() < maxRows && rangeResults.hasNext()) {
                byte[] rowName = rangeResults.next().getBytes("row_name");
                if (rowName != null) {
                    rows.add(rowName);
                }
            }
            if (rows.isEmpty()) {
                return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Set<Long>>>of(), false);
            }
        } finally {
            rangeResults.close();
        }
        ColumnSelection columns = ColumnSelection.all();
        if (!range.getColumnNames().isEmpty()) {
            columns = ColumnSelection.create(range.getColumnNames());
        }
        ClosableIterator<AgnosticLightResultRow> rowResults = table.getAllRows(rows, columns, timestamp, false);
        SetMultimap<Cell, Long> results = HashMultimap.create();
        try {
            while (rowResults.hasNext()) {
                AgnosticLightResultRow row = rowResults.next();
                Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                long ts = row.getLong("ts");
                results.put(cell, ts);
            }
        } finally {
            rowResults.close();
        }
        NavigableMap<byte[], SortedMap<byte[], Set<Long>>> cellsByRow = Cells.breakCellsUpByRow(Multimaps.asMap(results));
        if (range.isReverse()) {
            cellsByRow = cellsByRow.descendingMap();
        }
        List<RowResult<Set<Long>>> finalResults = Lists.newArrayListWithCapacity(results.size());
        for (Entry<byte[], SortedMap<byte[], Set<Long>>> entry : cellsByRow.entrySet()) {
            finalResults.add(RowResult.create(entry.getKey(), entry.getValue()));
        }
        byte[] nextRow = null;
        boolean mayHaveMoreResults = false;
        byte[] lastRow = range.isReverse() ? rows.first() : rows.last();
        if (!RangeRequests.isTerminalRow(range.isReverse(), lastRow)) {
            nextRow = RangeRequests.getNextStartRow(range.isReverse(), lastRow);
            mayHaveMoreResults = rows.size() == maxRows;
        }
        return SimpleTokenBackedResultsPage.create(nextRow, finalResults, mayHaveMoreResults);
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        runDdl(tableRef, new Function<DbDdlTable, Void>() {
            @Override
            public Void apply(DbDdlTable table) {
                table.truncate();
                return null;
            }
        });
    }

    @Override
    public void dropTable(TableReference tableRef) {
        runDdl(tableRef, new Function<DbDdlTable, Void>() {
            @Override
            public Void apply(DbDdlTable table) {
                table.drop();
                return null;
            }
        });
    }

    @Override
    public void createTable(TableReference tableRef, final byte[] tableMetadata) {
        runDdl(tableRef, new Function<DbDdlTable, Void>() {
            @Override
            public Void apply(DbDdlTable table) {
                table.create(tableMetadata);
                return null;
            }
        });
        // it would be kind of nice if this was in a transaction with the DbDdlTable create,
        // but the code currently isn't well laid out to accommodate that
        putMetadataForTable(tableRef, tableMetadata);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return run(new Function<SqlConnection, Set<TableReference>>() {
            @Override
            public Set<TableReference> apply(SqlConnection conn) {
                AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                        "SELECT table_name FROM " + AtlasDbConstants.METADATA_TABLE.getQualifiedName());
                Set<TableReference> ret = Sets.newHashSetWithExpectedSize(results.size());
                for (AgnosticResultRow row : results.rows()) {
                    ret.add(TableReference.createUnsafe(row.getString("table_name")));
                }
                return ret;
            }
        });
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return runMetadata(tableRef, new Function<DbMetadataTable, byte[]>() {
            @Override
            public byte[] apply(DbMetadataTable table) {
                return table.getMetadata();
            }
        });
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, final byte[] metadata) {
        runMetadata(tableRef, new Function<DbMetadataTable, Void>() {
            @Override
            public Void apply(DbMetadataTable table) {
                table.putMetadata(metadata);
                return null;
            }
        });
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return run(new Function<SqlConnection, Map<TableReference, byte[]>>() {
            @Override
            @SuppressWarnings("deprecation")
            public Map<TableReference, byte[]> apply(SqlConnection conn) {
                AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                        "SELECT table_name, value FROM " + AtlasDbConstants.METADATA_TABLE.getQualifiedName());
                Map<TableReference, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
                for (AgnosticResultRow row : results.rows()) {
                    ret.put(TableReference.createUnsafe(row.getString("table_name")), row.getBytes("value"));
                }
                return ret;
            }
        });
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, final Set<Cell> cells) {
        runWrite(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                table.putSentinels(cells);
                return null;
            }
        });
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, final Set<Cell> cells, final long timestamp) {
        return runRead(tableRef, new Function<DbReadTable, Multimap<Cell, Long>>() {
            @Override
            @SuppressWarnings("deprecation")
            public Multimap<Cell, Long> apply(DbReadTable table) {
                ClosableIterator<AgnosticLightResultRow> iter =
                        table.getAllCells(cells, timestamp, false);
                try {
                    Multimap<Cell, Long> results = ArrayListMultimap.create();
                    while (iter.hasNext()) {
                        AgnosticLightResultRow row = iter.next();
                        Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                        long ts = row.getLong("ts");
                        results.put(cell, ts);
                    }
                    return results;
                } finally {
                    iter.close();
                }
            }
        });
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        runDdl(tableRef, new Function<DbDdlTable, Void>() {
            @Override
            public Void apply(DbDdlTable table) {
                table.compactInternally();
                return null;
            }
        });
    }

    public void checkDatabaseVersion() {
        runDdl(TableReference.createUnsafe(""), new Function<DbDdlTable, Void>() {
            @Override
            public Void apply(DbDdlTable table) {
                table.checkDatabaseVersion();
                return null;
            }
        });
    }

    private <T> T run(Function<SqlConnection, T> runner) {
        SqlConnection conn = connections.get();
        try {
            return runner.apply(conn);
        } finally {
            try {
                conn.getUnderlyingConnection().close();
            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }
        }
    }

    private <T> T runMetadata(TableReference tableRef, Function<DbMetadataTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createMetadata(tableRef.getQualifiedName(), conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runDdl(TableReference tableRef, Function<DbDdlTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createDdl(tableRef.getQualifiedName(), conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runRead(TableReference tableRef, Function<DbReadTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createRead(tableRef.getQualifiedName(), conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runWrite(TableReference tableRef, Function<DbWriteTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createWrite(tableRef.getQualifiedName(), conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runWriteForceAutocommit(final TableReference tableRef, final Function<DbWriteTable, T> runner) {
        final ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            SqlConnection conn = conns.get();
            boolean autocommit;
            try {
                autocommit = conn.getUnderlyingConnection().getAutoCommit();
            } catch (PalantirSqlException e1) {
                throw Throwables.rewrapAndThrowUncheckedException(e1);
            } catch (SQLException e1) {
                throw Throwables.rewrapAndThrowUncheckedException(e1);
            }
            if (!autocommit) {
                return runWriteFreshConnection(conns, tableRef, runner);
            } else {
                return runner.apply(dbTables.createWrite(tableRef.getQualifiedName(), conns));
            }
        } finally {
            conns.close();
        }
    }

    /**
     * Runs with a new connection, in a new thread so we don't reuse the connection we're getting from ReentrantManagedConnectionSupplier.
     * Note that most of DbKvs reuses connections so unlike most other calls, this can block on getting a new connection if the pool is full.
     * To avoid deadlocks or long pauses, use this only when necessary.
     */
    private <T> T runWriteFreshConnection(final ConnectionSupplier conns, final TableReference tableRef, final Function<DbWriteTable, T> runner) {
        log.info("Running in a new thread to turn autocommit on for write");
        final AtomicReference<T> result = Atomics.newReference();
        Thread writeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                SqlConnection freshConn = conns.getFresh();
                try {
                    result.set(runner.apply(dbTables.createWrite(tableRef.getQualifiedName(), new ConnectionSupplier(Suppliers.ofInstance(freshConn)))));
                } finally {
                    try {
                        Connection c = freshConn.getUnderlyingConnection();
                        if (c != null) {
                            c.close();
                        }
                    } catch (SQLException e) {
                        log.error("Failed to close db connection performing write with fresh connection.", e);
                    }
                }
            }});
        writeThread.start();
        try {
            writeThread.join();
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
        return result.get();
    }
}
