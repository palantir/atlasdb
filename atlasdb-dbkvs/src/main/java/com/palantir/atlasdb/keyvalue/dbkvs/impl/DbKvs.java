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
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.AccumulatorStrategies;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.BatchingStrategies;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.BatchingTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.ImmediateSingleBatchTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.ParallelTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class DbKvs extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(DbKvs.class);

    private final DdlConfig config;
    private final DbTableFactory dbTables;
    private final SqlConnectionSupplier connections;
    private final PrefixedTableNames prefixedTableNames;
    private final BatchingTaskRunner batchingQueryRunner;
    private final DbKvsWideRowFetcher dbKvsWideRowFetcher;

    public static DbKvs create(DbKeyValueServiceConfig config, SqlConnectionSupplier sqlConnSupplier) {
        DbKvs dbKvs = new DbKvs(config.ddl(), config.ddl().tableFactorySupplier().get(), sqlConnSupplier);
        dbKvs.init();
        return dbKvs;
    }

    /**
     * Constructor for a SQL (either Postgres or Oracle) backed key value store.  This method should not
     * be used directly and is exposed to support legacy software.  Instead you should prefer the use of
     * ConnectionManagerAwareDbKvs which will instantiate a properly initialized DbKVS using the above create method
     */
    public DbKvs(DdlConfig config,
                 DbTableFactory dbTables,
                 SqlConnectionSupplier connections) {
        super(AbstractKeyValueService.createFixedThreadPool("Atlas Relational KVS", config.poolSize()));
        this.config = config;
        this.dbTables = dbTables;
        this.connections = connections;
        if (DBType.ORACLE.equals(dbTables.getDbType())) {
            prefixedTableNames = new OraclePrefixedTableNames(
                    config,
                    new ConnectionSupplier(connections),
                    ((OracleDbTableFactory) dbTables).getOracleTableNameGetter());
            batchingQueryRunner = new ImmediateSingleBatchTaskRunner();
        } else {
            prefixedTableNames = new PrefixedTableNames(config);
            batchingQueryRunner = new ParallelTaskRunner(
                    newFixedThreadPool(config.poolSize()),
                    config.fetchBatchSize());
        }
        this.dbKvsWideRowFetcher = new DbKvsWideRowFetcher(batchingQueryRunner, connections, dbTables);
    }

    private static ThreadPoolExecutor newFixedThreadPool(int maxPoolSize) {
        ThreadPoolExecutor pool = PTExecutors.newThreadPoolExecutor(maxPoolSize, maxPoolSize,
                15L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("Atlas DbKvs reader", true /* daemon */));

        pool.allowCoreThreadTimeOut(false);
        return pool;
    }

    private void init() {
        databaseSpecificInitialization();
        createMetadataTable();
    }

    private void databaseSpecificInitialization() {
        runInitialization(new Function<DbTableInitializer, Void>() {
            @Nullable
            @Override
            public Void apply(@Nonnull DbTableInitializer initializer) {
                initializer.createUtilityTables();
                return null;
            }
        });
    }

    private void createMetadataTable() {
        runInitialization(new Function<DbTableInitializer, Void>() {
            @Override
            public Void apply(@Nonnull DbTableInitializer initializer) {
                initializer.createMetadataTable(config.metadataTable().getQualifiedName());
                return null;
            }
        });
    }
    @Override
    public void close() {
        super.close();
        dbTables.close();
        connections.close();
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            long timestamp) {
        return getRowsBatching(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return batchingQueryRunner.runTask(
                timestampByCell,
                BatchingStrategies.forMap(),
                AccumulatorStrategies.forMap(),
                cellBatch -> runReadAndExtractResults(tableRef, table ->
                        table.getLatestCells(cellBatch, true)));
    }

    private Map<Cell, Value> getRowsBatching(TableReference tableRef,
                                             Iterable<byte[]> rows,
                                             ColumnSelection columnSelection,
                                             long timestamp) {
        return batchingQueryRunner.runTask(
                rows,
                BatchingStrategies.forIterable(),
                AccumulatorStrategies.forMap(),
                rowBatch -> runReadAndExtractResults(tableRef, table ->
                        table.getLatestRows(rowBatch, columnSelection, timestamp, true)));
    }

    private Map<Cell, Value> runReadAndExtractResults(
            TableReference tableRef,
            Function<DbReadTable, ClosableIterator<AgnosticLightResultRow>> query) {
        return runRead(tableRef, table -> extractResults(table, query.apply(table)));
    }

    @SuppressWarnings("deprecation")
    private Map<Cell, Value> extractResults(
            DbReadTable table,
            ClosableIterator<AgnosticLightResultRow> rows) {
        Map<Cell, Value> results = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowResults = Maps.newHashMap();
        try (ClosableIterator<AgnosticLightResultRow> iter = rows) {
            boolean hasOverflow = table.hasOverflowValues();
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
                    OverflowValue ov = ImmutableOverflowValue.of(row.getLong("ts"), overflowId);
                    OverflowValue oldOv = overflowResults.put(cell, ov);
                    if (oldOv != null && oldOv.ts() > ov.ts()) {
                        overflowResults.put(cell, oldOv);
                    }
                }
            }
        }
        fillOverflowValues(table, overflowResults, results);
        return results;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return batchingQueryRunner.runTask(
                timestampByCell,
                BatchingStrategies.forMap(),
                AccumulatorStrategies.forMap(),
                cellBatch -> runRead(tableRef, table -> doGetLatestTimestamps(table, cellBatch)));
    }

    private static Map<Cell, Long> doGetLatestTimestamps(DbReadTable table, Map<Cell, Long> timestampByCell) {
        try (ClosableIterator<AgnosticLightResultRow> iter = table.getLatestCells(timestampByCell, false)) {
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
        }
    }

    public Function<Entry<Cell, byte[]>, Long> getByteSizingFunction() {
        return entry -> Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().length;
    }

    public Function<Entry<Cell, Value>, Long> getValueSizingFunction() {
        return entry -> Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length;
    }

    private void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp, boolean idempotent) {
        Iterable<List<Entry<Cell, byte[]>>> batches = partitionByCountAndBytes(
                values.entrySet(),
                config.mutationBatchCount(),
                config.mutationBatchSizeBytes(),
                tableRef,
                getByteSizingFunction());

        runReadWrite(tableRef, (readTable, writeTable) -> {
            for (List<Entry<Cell, byte[]>> batch : batches) {
                try {
                    writeTable.put(batch, timestamp);
                } catch (KeyAlreadyExistsException e) {
                    if (idempotent) {
                        putIfNotUpdate(readTable, writeTable, batch, timestamp, e);
                    } else {
                        throw e;
                    }
                }
            }
            return null;
        });
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        put(tableRef, values, timestamp, true);
    }

    private void putIfNotUpdate(
            DbReadTable readTable,
            DbWriteTable writeTable,
            List<Entry<Cell, Value>> batch,
            KeyAlreadyExistsException ex) {
        Map<Cell, Long> timestampByCell = Maps.newHashMap();
        for (Entry<Cell, Value> entry : batch) {
            timestampByCell.put(entry.getKey(), entry.getValue().getTimestamp() + 1);
        }

        Map<Cell, Value> results = extractResults(readTable, readTable.getLatestCells(timestampByCell, true));

        ListIterator<Entry<Cell, Value>> iter = batch.listIterator();
        while (iter.hasNext()) {
            Entry<Cell, Value> entry = iter.next();
            Cell key = entry.getKey();
            Value value = entry.getValue();
            if (results.containsKey(key)) {
                if (results.get(key).equals(value)) {
                    iter.remove();
                } else {
                    throw new KeyAlreadyExistsException(
                            "primary key violation for key " + key + " with value " + value,
                            ex);
                }
            }
        }
        writeTable.put(batch);
    }

    private void putIfNotUpdate(
            DbReadTable readTable,
            DbWriteTable writeTable,
            List<Entry<Cell, byte[]>> batch,
            long timestamp,
            KeyAlreadyExistsException ex) {
        List<Entry<Cell, Value>> batchValues =
                Lists.transform(batch,
                        input -> Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp)));
        putIfNotUpdate(readTable, writeTable, batchValues, ex);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        Iterable<List<Entry<Cell, Value>>> batches = partitionByCountAndBytes(
                cellValues.entries(),
                config.mutationBatchCount(),
                config.mutationBatchSizeBytes(),
                tableRef,
                getValueSizingFunction());

        runReadWrite(tableRef, (readTable, writeTable) -> {
            for (List<Entry<Cell, Value>> batch : batches) {
                try {
                    writeTable.put(batch);
                } catch (KeyAlreadyExistsException e) {
                    putIfNotUpdate(readTable, writeTable, batch, e);
                }
            }
            return null;
        });
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        put(tableRef, values, AtlasDbConstants.TRANSACTION_TS, false);
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        if (checkAndSetRequest.oldValue().isPresent()) {
            executeCheckAndSet(checkAndSetRequest);
        } else {
            executePutUnlessExists(checkAndSetRequest);
        }
    }

    private void executeCheckAndSet(CheckAndSetRequest request) {
        Preconditions.checkArgument(request.oldValue().isPresent());

        runWrite(request.table(), table -> {
            //noinspection OptionalGetWithoutIsPresent
            table.update(request.cell(), AtlasDbConstants.TRANSACTION_TS, request.oldValue().get(), request.newValue());
            return null;
        });
    }

    private void executePutUnlessExists(CheckAndSetRequest checkAndSetRequest) {
        try {
            Map<Cell, byte[]> value = ImmutableMap.of(checkAndSetRequest.cell(), checkAndSetRequest.newValue());
            putUnlessExists(checkAndSetRequest.table(), value);
        } catch (KeyAlreadyExistsException e) {
            throw new CheckAndSetException("Value unexpectedly present when running check and set", e);
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        // QA-86494: We sort our deletes here because we have seen oracle deadlock errors here.
        ImmutableList<Entry<Cell, Long>> sorted = ORDERING.immutableSortedCopy(keys.entries());
        Iterable<List<Entry<Cell, Long>>> partitions = partitionByCountAndBytes(
                sorted,
                10000,
                getMultiPutBatchSizeBytes(),
                tableRef,
                entry -> Cells.getApproxSizeOfCell(entry.getKey()) + 8);
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
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        runWriteForceAutocommit(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                table.delete(range);
                return null;
            }
        });
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return new DbKvsGetRanges(this, dbTables.getDbType(), connections, prefixedTableNames)
                .getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        Iterable<RowResult<Value>> rows =
                new AbstractPagingIterable<RowResult<Value>, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getFirstPage() {
                        return getPage(tableRef, rangeRequest, timestamp);
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getNextPage(
                            TokenBackedBasicResultsPage<RowResult<Value>, byte[]> previous) {
                        byte[] newStartRow = previous.getTokenForNextPage();
                        RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(newStartRow).build();
                        return getPage(tableRef, newRange, timestamp);
                    }
                };
        return ClosableIterators.wrap(rows.iterator());
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPage(
            TableReference tableRef,
            RangeRequest range,
            long timestamp) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return getPageInternal(tableRef, range, timestamp);
        } finally {
            log.debug("Call to KVS.getPage on table {} took {} ms.", tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @SuppressWarnings("deprecation")
    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPageInternal(TableReference tableRef,
                                                                                  RangeRequest range,
                                                                                  long timestamp) {
        int maxRows = getMaxRowsFromBatchHint(range.getBatchHint());
        SortedSet<byte[]> rows = runRead(tableRef, table -> getRowKeysForRange(table, range, timestamp, maxRows));
        if (rows.isEmpty()) {
            return SimpleTokenBackedResultsPage.create(null, ImmutableList.of(), false);
        }

        Map<Cell, Value> results = getRowsBatching(tableRef, rows, getColumnSelectionForRange(range), timestamp);
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

    private ColumnSelection getColumnSelectionForRange(RangeRequest range) {
        if (!range.getColumnNames().isEmpty()) {
            return ColumnSelection.create(range.getColumnNames());
        } else {
            return ColumnSelection.all();
        }
    }

    private SortedSet<byte[]> getRowKeysForRange(DbReadTable table,
                                                 RangeRequest range,
                                                 long timestamp,
                                                 int maxRows) {
        Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
        SortedSet<byte[]> rows = Sets.newTreeSet(comp);
        try (ClosableIterator<AgnosticLightResultRow> rangeResults = table.getRange(range, timestamp, maxRows)) {
            while (rows.size() < maxRows && rangeResults.hasNext()) {
                byte[] rowName = rangeResults.next().getBytes("row_name");
                if (rowName != null) {
                    rows.add(rowName);
                }
            }
        }
        return rows;
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        return new TimestampRangeFetcher().invoke(tableRef, rangeRequest, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return dbKvsWideRowFetcher.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        return dbKvsWideRowFetcher.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    private void fillOverflowValues(DbReadTable table,
                                    Map<Cell, OverflowValue> overflowValues,
                                    @Output Map<Cell, Value> values) {
        Iterator<Entry<Cell, OverflowValue>> overflowIterator = overflowValues.entrySet().iterator();
        while (overflowIterator.hasNext()) {
            Entry<Cell, OverflowValue> entry = overflowIterator.next();
            Value value = values.get(entry.getKey());
            if (value != null && value.getTimestamp() > entry.getValue().ts()) {
                overflowIterator.remove();
            }
        }

        if (!overflowValues.isEmpty()) {
            Map<Long, byte[]> resolvedOverflowValues = Maps.newHashMapWithExpectedSize(overflowValues.size());
            try (ClosableIterator<AgnosticLightResultRow> overflowIter = table.getOverflow(overflowValues.values())) {
                while (overflowIter.hasNext()) {
                    AgnosticLightResultRow row = overflowIter.next();
                    // QA-94468 LONG RAW typed columns ("val" in this case) must be retrieved first from the result set
                    // see https://docs.oracle.com/cd/B19306_01/java.102/b14355/jstreams.htm#i1007581
                    byte[] val = row.getBytes("val");
                    long id = row.getLong("id");
                    resolvedOverflowValues.put(id, val);
                }
            }
            for (Entry<Cell, OverflowValue> entry : overflowValues.entrySet()) {
                Cell cell = entry.getKey();
                OverflowValue ov = entry.getValue();
                byte[] val = resolvedOverflowValues.get(ov.id());
                Preconditions.checkNotNull(val, "Failed to load overflow data: cell=%s, overflowId=%s", cell, ov.id());
                values.put(cell, Value.create(val, ov.ts()));
            }
        }
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
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
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
                        "SELECT table_name FROM " + config.metadataTable().getQualifiedName());
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
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
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
                        "SELECT table_name, value FROM " + config.metadataTable().getQualifiedName());
                Map<TableReference, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
                for (AgnosticResultRow row : results.rows()) {
                    ret.put(TableReference.createUnsafe(row.getString("table_name")), row.getBytes("value"));
                }
                return ret;
            }
        });
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        runWrite(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                table.putSentinels(cells);
                return null;
            }
        });
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        return batchingQueryRunner.runTask(
                cells,
                BatchingStrategies.forIterable(),
                AccumulatorStrategies.forListMultimap(),
                cellBatch -> runRead(tableRef, table -> doGetAllTimestamps(table, cellBatch, timestamp)));
    }

    private static Multimap<Cell, Long> doGetAllTimestamps(DbReadTable table, Iterable<Cell> cells, long timestamp) {
        try (ClosableIterator<AgnosticLightResultRow> iter = table.getAllCells(cells, timestamp, false)) {
            Multimap<Cell, Long> results = ArrayListMultimap.create();
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                long ts = row.getLong("ts");
                results.put(cell, ts);
            }
            return results;
        }
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
        runDdl(TableReference.createWithEmptyNamespace(""), new Function<DbDdlTable, Void>() {
            @Override
            public Void apply(DbDdlTable table) {
                table.checkDatabaseVersion();
                return null;
            }
        });
    }

    public String getTablePrefix() {
        return config.tablePrefix();
    }

    private <T> T run(Function<SqlConnection, T> runner) {
        SqlConnection conn = connections.get();
        try {
            return runner.apply(conn);
        } finally {
            try {
                conn.getUnderlyingConnection().close();
            } catch (Exception e) {
                log.debug("Error occurred trying to close the connection", e);
            }
        }
    }

    private <T> T runMetadata(TableReference tableRef, Function<DbMetadataTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            /* The metadata table operates only on the fully qualified table reference */
            return runner.apply(dbTables.createMetadata(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runDdl(TableReference tableRef, Function<DbDdlTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            /* The ddl actions can used both the fully qualified name and the internal name */
            return runner.apply(dbTables.createDdl(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runInitialization(Function<DbTableInitializer, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createInitializer(conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runRead(TableReference tableRef, Function<DbReadTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createRead(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runWrite(TableReference tableRef, Function<DbWriteTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createWrite(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runReadWrite(TableReference tableRef, ReadWriteTask<T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.run(
                    dbTables.createRead(tableRef, conns),
                    dbTables.createWrite(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runWriteForceAutocommit(TableReference tableRef, Function<DbWriteTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
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
                return runner.apply(dbTables.createWrite(tableRef, conns));
            }
        } finally {
            conns.close();
        }
    }

    /**
     * Runs with a new connection, in a new thread so we don't reuse the connection we're
     * getting from ReentrantManagedConnectionSupplier.
     * Note that most of DbKvs reuses connections so unlike most other calls,
     * this can block on getting a new connection if the pool is full.
     * To avoid deadlocks or long pauses, use this only when necessary.
     */
    private <T> T runWriteFreshConnection(
            ConnectionSupplier conns, TableReference tableRef, Function<DbWriteTable, T> runner) {
        log.debug("Running in a new thread to turn autocommit on for write");
        AtomicReference<T> result = Atomics.newReference();
        Thread writeThread = new Thread(() -> {
            SqlConnection freshConn = conns.getFresh();
            try {
                result.set(runner.apply(dbTables.createWrite(tableRef,
                        new ConnectionSupplier(Suppliers.ofInstance(freshConn)))));
            } finally {
                try {
                    Connection conn = freshConn.getUnderlyingConnection();
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    log.error("Failed to close db connection performing write with fresh connection.", e);
                }
            }
        });
        writeThread.start();
        try {
            writeThread.join();
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
        return result.get();
    }

    private static Integer getMaxRowsFromBatchHint(@Nullable Integer batchHint) {
        return Optional.ofNullable(batchHint)
                .map(x -> (int) (1.1 * x))
                .orElse(100);
    }

    private interface ReadWriteTask<T> {
        T run(DbReadTable readTable, DbWriteTable writeTable);
    }

    private class TimestampRangeFetcher {
        TimestampRangeFetcher() {
        }

        public ClosableIterator<RowResult<Set<Long>>> invoke(TableReference tableRef,
                RangeRequest rangeRequest,
                long timestamp) {
            Iterable<RowResult<Set<Long>>> rows = new AbstractPagingIterable<
                    RowResult<Set<Long>>,
                    TokenBackedBasicResultsPage<RowResult<Set<Long>>, TimestampsByCellToken>>() {
                @Override
                protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, TimestampsByCellToken> getFirstPage() {
                    return getTimestampsPage(tableRef, rangeRequest, timestamp);
                }

                @Override
                protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, TimestampsByCellToken> getNextPage(
                        TokenBackedBasicResultsPage<RowResult<Set<Long>>, TimestampsByCellToken> previous) {
                    // TODO if we didn't get through all the cells last time, the next page logic will look different
                    // TODO it'll be simpler! Continue to go through the iterator
                    /**
                     * If cell limit is 10, row limit is 2, results per row: 15,10,5,4,9,1,1,2, might see this:
                     * Page 1 Query 1 **********
                     * Page 2         *****@@@@@
                     * Page 3         @@@@@
                     * Page 4 Query 2 *****@@@@
                     * Page 5 Query 3 *********@
                     * Page 6 Query 4 *@@ ~fin~
                     */
                    byte[] newStartRow = previous.getTokenForNextPage().nextRow;
                    RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(newStartRow).build();
                    return getTimestampsPage(tableRef, newRange, timestamp);
                }
            };
            return ClosableIterators.wrap(rows.iterator());
        }

        private TokenBackedBasicResultsPage<RowResult<Set<Long>>, TimestampsByCellToken> getTimestampsPage(
                TableReference tableRef,
                RangeRequest range,
                long timestamp) {
            log.info("Getting a page!");
            Stopwatch watch = Stopwatch.createStarted();
            try {
                return runRead(tableRef, table -> getTimestampsPageInternal(table, range, timestamp));
            } finally {
                log.debug("Call to KVS.getTimestampsPage on table {} took {} ms.",
                        tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
            }
        }

        @SuppressWarnings("deprecation")
        private TokenBackedBasicResultsPage<RowResult<Set<Long>>, TimestampsByCellToken> getTimestampsPageInternal(
                DbReadTable table,
                RangeRequest range,
                long timestamp) {
            Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
            SortedSet<byte[]> rows = Sets.newTreeSet(comp);

            // TODO only this method is used in both DbKvs and the inner class
            int maxRows = getMaxRowsFromBatchHint(range.getBatchHint());

            log.info("DbKvs.getTimestampsPageInternal calling getRange");
            try (ClosableIterator<AgnosticLightResultRow> rangeResults = table.getRange(range, timestamp, maxRows)) {
                while (rows.size() < maxRows && rangeResults.hasNext()) {
                    byte[] rowName = rangeResults.next().getBytes("row_name");
                    if (rowName != null) {
                        rows.add(rowName);
                    }
                }
                if (rows.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Set<Long>>>of(), false);
                }
            }

            ColumnSelection columns = ColumnSelection.all();
            if (!range.getColumnNames().isEmpty()) {
                columns = ColumnSelection.create(range.getColumnNames());
            }

            log.info("DbKvs.getTimestampsPageInternal calling getTimestampsByCell");
            TimestampsByCellResult result = getTimestampsByCell(table,
                    rows,
                    columns,
                    timestamp);
            SetMultimap<Cell, Long> results = result.results; // TODO naming is bad here

            List<RowResult<Set<Long>>> finalResults = processResults(range, results);

            // INFO Here's logic for making the results token
            byte[] nextRow = null;
            boolean mayHaveMoreResults = false;
            // TODO find last row used
            byte[] lastRow = range.isReverse() ? result.rowsReturned.first() : result.rowsReturned.last();
            if (!RangeRequests.isTerminalRow(range.isReverse(), lastRow)) {
                // TODO nextRow will be different if the iterator is not empty
                nextRow = RangeRequests.getNextStartRow(range.isReverse(), lastRow);
                mayHaveMoreResults = rows.size() == maxRows || result.incomplete;
            }
            // TODO include the iterator in the results page. Now, where is that used?
            TimestampsByCellToken token = new TimestampsByCellToken(nextRow);
            return SimpleTokenBackedResultsPage.create(token, finalResults, mayHaveMoreResults);
        }

        private List<RowResult<Set<Long>>> processResults(RangeRequest range, SetMultimap<Cell, Long> results) {
            NavigableMap<byte[], SortedMap<byte[], Set<Long>>> cellsByRow
                    = Cells.breakCellsUpByRow(Multimaps.asMap(results));
            if (range.isReverse()) {
                cellsByRow = cellsByRow.descendingMap();
            }
            List<RowResult<Set<Long>>> finalResults = Lists.newArrayListWithCapacity(results.size());
            for (Entry<byte[], SortedMap<byte[], Set<Long>>> entry : cellsByRow.entrySet()) {
                finalResults.add(RowResult.create(entry.getKey(), entry.getValue()));
            }
            return finalResults;
        }

        private TimestampsByCellResult getTimestampsByCell(
                DbReadTable table,
                Iterable<byte[]> rows,
                ColumnSelection columns,
                long timestamp) {
            // TODO pass maxCells in from RangeRequest
            int maxCells = 1_000_000;
            SetMultimap<Cell, Long> results = HashMultimap.create();
            Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
            SortedSet<byte[]> rowsReturned = Sets.newTreeSet(comp);

            log.info("DbKVs.getTimestampsByCell creating iterator");
            try (ClosableIterator<AgnosticLightResultRow> rowResults =
                    table.getAllRows(rows, columns, timestamp, false)) {
                log.info("Got all rows!");
                // Great, add up to a million rows
                while (rowResults.hasNext() && results.size() < maxCells) {
                    AgnosticLightResultRow row = rowResults.next();
                    byte[] rowName = row.getBytes("row_name");
                    Cell cell = Cell.create(rowName, row.getBytes("col_name"));
                    long ts = row.getLong("ts");
                    results.put(cell, ts);
                    rowsReturned.add(rowName);
                }
                // If we hit 1M, try to fill up the last row
                if (rowResults.hasNext()) {
                    boolean rowAlreadyPresent;
                    do {
                        AgnosticLightResultRow row = rowResults.next();
                        byte[] rowName = row.getBytes("row_name");
                        rowAlreadyPresent = rowsReturned.contains(rowName);
                        if (rowAlreadyPresent) {
                            Cell cell = Cell.create(rowName, row.getBytes("col_name"));
                            long ts = row.getLong("ts");
                            results.put(cell, ts);
                        }
                    } while (rowAlreadyPresent && rowResults.hasNext());
                }

                return new TimestampsByCellResult(results, rowsReturned, rowResults.hasNext());
            }
        }

        private final class TimestampsByCellResult {
            final SetMultimap<Cell, Long> results;
            final SortedSet<byte[]> rowsReturned;
            final boolean incomplete;

            private TimestampsByCellResult(SetMultimap<Cell, Long> results,
                    SortedSet<byte[]> rowsReturned,
                    boolean incomplete) {
                this.results = results;
                this.rowsReturned = rowsReturned;
                this.incomplete = incomplete;
            }
        }

        private class TimestampsByCellToken {
            final byte[] nextRow;

            TimestampsByCellToken(byte[] nextRow) {
                this.nextRow = nextRow;
            }
        }
    }
}
