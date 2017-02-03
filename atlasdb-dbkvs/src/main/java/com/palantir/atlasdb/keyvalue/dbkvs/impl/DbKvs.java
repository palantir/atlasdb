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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.dbkvs.util.DbKvsPartitioners;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class DbKvs extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(DbKvs.class);

    private final DdlConfig config;
    private final DbTableFactory dbTables;
    private final SqlConnectionSupplier connections;

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
        return runRead(tableRef, table ->
                extractResults(table, () ->
                        table.getLatestRows(rows, columnSelection, timestamp, true)));
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return runRead(tableRef, table ->
                extractResults(table, () ->
                        table.getLatestCells(timestampByCell, true)));
    }

    @SuppressWarnings("deprecation")
    private Map<Cell, Value> extractResults(
            DbReadTable table,
            Supplier<ClosableIterator<AgnosticLightResultRow>> resultSupplier) {
        boolean hasOverflow = table.hasOverflowValues();
        Map<Cell, Value> results = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowResults = Maps.newHashMap();
        try (ClosableIterator<AgnosticLightResultRow> iter = resultSupplier.get()) {
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
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        put(tableRef, values, timestamp, true);
    }

    private void putIfNotUpdate(
            TableReference tableRef,
            DbWriteTable table,
            List<Entry<Cell, Value>> batch,
            KeyAlreadyExistsException ex) {
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
                    throw new KeyAlreadyExistsException(
                            "primary key violation for key " + key + " with value " + value,
                            ex);
                }
            }
        }
        table.put(batch);
    }

    private void putIfNotUpdate(
            TableReference tableRef,
            DbWriteTable table,
            List<Entry<Cell, byte[]>> batch,
            long timestamp,
            KeyAlreadyExistsException ex) {
        List<Entry<Cell, Value>> batchValues =
                Lists.transform(batch,
                        input -> Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp)));
        putIfNotUpdate(tableRef, table, batchValues, ex);
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
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return new DbKvsGetRanges(this, config, dbTables.getDbType(), connections)
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
            return runRead(tableRef, table -> getPageInternal(table, range, timestamp));
        } finally {
            log.debug("Call to KVS.getPage on table {} took {} ms.", tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @SuppressWarnings("deprecation")
    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPageInternal(DbReadTable table,
                                                                                  RangeRequest range,
                                                                                  long timestamp) {
        Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
        SortedSet<byte[]> rows = Sets.newTreeSet(comp);
        int maxRows = getMaxRowsFromBatchHint(range.getBatchHint());

        try (ClosableIterator<AgnosticLightResultRow> rangeResults = table.getRange(range, timestamp, maxRows)) {
            while (rows.size() < maxRows && rangeResults.hasNext()) {
                byte[] rowName = rangeResults.next().getBytes("row_name");
                if (rowName != null) {
                    rows.add(rowName);
                }
            }
            if (rows.isEmpty()) {
                return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Value>>of(), false);
            }
        }

        ColumnSelection columns;
        if (!range.getColumnNames().isEmpty()) {
            columns = ColumnSelection.create(range.getColumnNames());
        } else {
            columns = ColumnSelection.all();
        }

        Map<Cell, Value> results = extractResults(table, () -> table.getLatestRows(rows, columns, timestamp, true));
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
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        Iterable<RowResult<Set<Long>>> rows = new AbstractPagingIterable<
                RowResult<Set<Long>>,
                TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getFirstPage() {
                return getTimestampsPage(tableRef, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getNextPage(
                    TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> previous) {
                byte[] newStartRow = previous.getTokenForNextPage();
                RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(newStartRow).build();
                return getTimestampsPage(tableRef, newRange, timestamp);
            }
        };
        return ClosableIterators.wrap(rows.iterator());
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getTimestampsPage(
            TableReference tableRef,
            RangeRequest range,
            long timestamp) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return runRead(tableRef, table -> getTimestampsPageInternal(table, range, timestamp));
        } finally {
            log.debug("Call to KVS.getTimestampsPage on table {} took {} ms.",
                    tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @SuppressWarnings("deprecation")
    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getTimestampsPageInternal(DbReadTable table,
                                                                                                RangeRequest range,
                                                                                                long timestamp) {
        Comparator<byte[]> comp = UnsignedBytes.lexicographicalComparator();
        SortedSet<byte[]> rows = Sets.newTreeSet(comp);
        int maxRows = getMaxRowsFromBatchHint(range.getBatchHint());


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

        SetMultimap<Cell, Long> results = HashMultimap.create();
        try (ClosableIterator<AgnosticLightResultRow> rowResults = table.getAllRows(rows, columns, timestamp, false)) {
            while (rowResults.hasNext()) {
                AgnosticLightResultRow row = rowResults.next();
                Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                long ts = row.getLong("ts");
                results.put(cell, ts);
            }
        }

        NavigableMap<byte[], SortedMap<byte[], Set<Long>>> cellsByRow =
                Cells.breakCellsUpByRow(Multimaps.asMap(results));
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
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        List<byte[]> rowList = ImmutableList.copyOf(rows);
        Map<byte[], List<Map.Entry<Cell, Value>>> firstPage =
                getFirstRowsColumnRangePage(tableRef, rowList, batchColumnRangeSelection, timestamp);

        Map<byte[], RowColumnRangeIterator> ret = Maps.newHashMapWithExpectedSize(rowList.size());
        for (Entry<byte[], List<Map.Entry<Cell, Value>>> e : firstPage.entrySet()) {
            List<Map.Entry<Cell, Value>> results = e.getValue();
            if (results.isEmpty()) {
                ret.put(e.getKey(), new LocalRowColumnRangeIterator(e.getValue().iterator()));
                continue;
            }
            byte[] lastCol = results.get(results.size() - 1).getKey().getColumnName();
            RowColumnRangeIterator firstPageIter = new LocalRowColumnRangeIterator(e.getValue().iterator());
            if (isEndOfColumnRange(lastCol, batchColumnRangeSelection.getEndCol())) {
                ret.put(e.getKey(), firstPageIter);
            } else {
                byte[] nextCol = RangeRequests.nextLexicographicName(lastCol);
                BatchColumnRangeSelection nextColumnRangeSelection =
                        BatchColumnRangeSelection.create(
                                nextCol,
                                batchColumnRangeSelection.getEndCol(),
                                batchColumnRangeSelection.getBatchHint());
                Iterator<Map.Entry<Cell, Value>> nextPagesIter = getRowColumnRange(
                        tableRef,
                        e.getKey(),
                        nextColumnRangeSelection,
                        timestamp);
                ret.put(e.getKey(), new LocalRowColumnRangeIterator(Iterators.concat(firstPageIter, nextPagesIter)));
            }
        }
        return ret;
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        List<byte[]> rowList = ImmutableList.copyOf(rows);
        Map<Sha256Hash, byte[]> rowHashesToBytes = Maps.uniqueIndex(rowList, Sha256Hash::computeHash);

        Map<Sha256Hash, Integer> ordered =
                getColumnCounts(tableRef, rowList, columnRangeSelection, timestamp);

        Iterator<Map<Sha256Hash, Integer>> batches =
                DbKvsPartitioners.partitionByTotalCount(ordered, cellBatchHint).iterator();
        Iterator<Iterator<Map.Entry<Cell, Value>>> results =
                loadColumnsForBatches(tableRef, columnRangeSelection, timestamp, rowHashesToBytes, batches);
        return new LocalRowColumnRangeIterator(Iterators.concat(results));
    }

    private Iterator<Iterator<Map.Entry<Cell, Value>>> loadColumnsForBatches(
            TableReference tableRef,
            ColumnRangeSelection columnRangeSelection,
            long timestamp,
            Map<Sha256Hash, byte[]> rowHashesToBytes,
            Iterator<Map<Sha256Hash, Integer>> batches) {
        Iterator<Iterator<Map.Entry<Cell, Value>>> results = new AbstractIterator<Iterator<Map.Entry<Cell, Value>>>() {
            private Sha256Hash lastRowHashInPreviousBatch = null;
            private byte[] lastColumnInPreviousBatch = null;

            @Override
            protected Iterator<Map.Entry<Cell, Value>> computeNext() {
                if (!batches.hasNext()) {
                    return endOfData();
                }
                Map<Sha256Hash, Integer> currentBatch = batches.next();
                Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow =
                        getBatchColumnRangeSelectionsByRow(currentBatch);

                Map<byte[], List<Map.Entry<Cell, Value>>> resultsByRow =
                        runRead(tableRef, dbReadTable ->
                            extractRowColumnRangePage(dbReadTable, columnRangeSelectionsByRow, timestamp));
                int totalEntries = resultsByRow.values().stream().mapToInt(List::size).sum();
                if (totalEntries == 0) {
                    return Collections.emptyIterator();
                }
                // Ensure order matches that of the provided batch.
                List<Map.Entry<Cell, Value>> loadedColumns = new ArrayList<>(totalEntries);
                for (Sha256Hash rowHash : currentBatch.keySet()) {
                    byte[] row = rowHashesToBytes.get(rowHash);
                    loadedColumns.addAll(resultsByRow.get(row));
                }

                Cell lastCell = Iterables.getLast(loadedColumns).getKey();
                lastRowHashInPreviousBatch = Sha256Hash.computeHash(lastCell.getRowName());
                lastColumnInPreviousBatch = lastCell.getColumnName();

                return loadedColumns.iterator();
            }

            private Map<byte[], BatchColumnRangeSelection> getBatchColumnRangeSelectionsByRow(
                    Map<Sha256Hash, Integer> columnCountsByRowHash) {
                Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow =
                        new HashMap<>(columnCountsByRowHash.size());
                for (Map.Entry<Sha256Hash, Integer> entry : columnCountsByRowHash.entrySet()) {
                    Sha256Hash rowHash = entry.getKey();
                    byte[] startCol = Objects.equals(lastRowHashInPreviousBatch, rowHash)
                            ? RangeRequests.nextLexicographicName(lastColumnInPreviousBatch)
                            : columnRangeSelection.getStartCol();
                    BatchColumnRangeSelection batchColumnRangeSelection =
                            BatchColumnRangeSelection.create(
                                    startCol,
                                    columnRangeSelection.getEndCol(),
                                    entry.getValue());
                    columnRangeSelectionsByRow.put(rowHashesToBytes.get(rowHash), batchColumnRangeSelection);
                }
                return columnRangeSelectionsByRow;
            }
        };
        return results;
    }

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRange(
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        List<byte[]> rowList = ImmutableList.of(row);
        return ClosableIterators.wrap(new AbstractPagingIterable<
                Entry<Cell, Value>,
                TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> getFirstPage() throws Exception {
                return page(batchColumnRangeSelection.getStartCol());
            }

            @Override
            protected TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> getNextPage(
                    TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> previous) throws Exception {
                return page(previous.getTokenForNextPage());
            }

            TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> page(byte[] startCol) throws Exception {
                BatchColumnRangeSelection range = BatchColumnRangeSelection.create(
                                startCol,
                                batchColumnRangeSelection.getEndCol(),
                                batchColumnRangeSelection.getBatchHint());
                List<Map.Entry<Cell, Value>> nextPage = runRead(tableRef, table -> Iterables.getOnlyElement(
                        extractRowColumnRangePage(table, range, timestamp, rowList).values()));
                if (nextPage.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.<Entry<Cell, Value>>of(), false);
                }
                byte[] lastCol = nextPage.get(nextPage.size() - 1).getKey().getColumnName();
                if (isEndOfColumnRange(lastCol, batchColumnRangeSelection.getEndCol())) {
                    return SimpleTokenBackedResultsPage.create(lastCol, nextPage, false);
                }
                byte[] nextCol = RangeRequests.nextLexicographicName(lastCol);
                return SimpleTokenBackedResultsPage.create(nextCol, nextPage, true);
            }

        }.iterator());
    }

    private boolean isEndOfColumnRange(byte[] lastCol, byte[] endCol) {
        return RangeRequests.isLastRowName(lastCol)
                || Arrays.equals(RangeRequests.nextLexicographicName(lastCol), endCol);
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> getFirstRowsColumnRangePage(
            TableReference tableRef,
            List<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long ts) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return runRead(tableRef, table -> extractRowColumnRangePage(table, columnRangeSelection, ts, rows));
        } finally {
            log.debug("Call to KVS.getFirstRowColumnRangePage on table {} took {} ms.",
                    tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private Map<byte[], List<Entry<Cell, Value>>> extractRowColumnRangePage(
            DbReadTable table,
            BatchColumnRangeSelection columnRangeSelection,
            long ts,
            List<byte[]> rows) {
        return extractRowColumnRangePage(table, Maps.toMap(rows, Functions.constant(columnRangeSelection)), ts);
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> extractRowColumnRangePage(
            DbReadTable table,
            Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow,
            long ts) {
        Map<Sha256Hash, byte[]> hashesToBytes = Maps.newHashMapWithExpectedSize(columnRangeSelectionsByRow.size());
        Map<Sha256Hash, List<Cell>> cellsByRow = Maps.newHashMap();
        for (byte[] row : columnRangeSelectionsByRow.keySet()) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            hashesToBytes.put(rowHash, row);
            cellsByRow.put(rowHash, Lists.newArrayList());
        }

        boolean hasOverflow = table.hasOverflowValues();
        Map<Cell, Value> values = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowValues = Maps.newHashMap();

        try (ClosableIterator<AgnosticLightResultRow> iter = table.getRowsColumnRange(columnRangeSelectionsByRow, ts)) {
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                Sha256Hash rowHash = Sha256Hash.computeHash(cell.getRowName());
                cellsByRow.get(rowHash).add(cell);
                Long overflowId = hasOverflow ? row.getLongObject("overflow") : null;
                if (overflowId == null) {
                    Value value = Value.create(row.getBytes("val"), row.getLong("ts"));
                    Value oldValue = values.put(cell, value);
                    if (oldValue != null && oldValue.getTimestamp() > value.getTimestamp()) {
                        values.put(cell, oldValue);
                    }
                } else {
                    OverflowValue ov = ImmutableOverflowValue.of(row.getLong("ts"), overflowId);
                    OverflowValue oldOv = overflowValues.put(cell, ov);
                    if (oldOv != null && oldOv.ts() > ov.ts()) {
                        overflowValues.put(cell, oldOv);
                    }
                }
            }
        }

        fillOverflowValues(table, overflowValues, values);

        Map<byte[], List<Map.Entry<Cell, Value>>> results =
                Maps.newHashMapWithExpectedSize(columnRangeSelectionsByRow.size());
        for (Entry<Sha256Hash, List<Cell>> e : cellsByRow.entrySet()) {
            List<Map.Entry<Cell, Value>> fullResults = Lists.newArrayListWithExpectedSize(e.getValue().size());
            for (Cell c : e.getValue()) {
                fullResults.add(Iterables.getOnlyElement(ImmutableMap.of(c, values.get(c)).entrySet()));
            }
            results.put(hashesToBytes.get(e.getKey()), fullResults);
        }
        return results;
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

    private Map<Sha256Hash, Integer> getColumnCounts(TableReference tableRef,
                                                     List<byte[]> rowList,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     long timestamp) {
        Map<Sha256Hash, Integer> countsByRow = runRead(tableRef, dbReadTable -> {
            Map<Sha256Hash, Integer> counts = new HashMap<>(rowList.size());
            try (ClosableIterator<AgnosticLightResultRow> iter =
                    dbReadTable.getRowsColumnRangeCounts(rowList, timestamp, columnRangeSelection)) {
                while (iter.hasNext()) {
                    AgnosticLightResultRow row = iter.next();
                    Sha256Hash rowHash = Sha256Hash.computeHash(row.getBytes("row_name"));
                    counts.put(rowHash, row.getInteger("column_count"));
                }
            }
            return counts;
        });
        // Make iteration order of the returned map match the provided list.
        Map<Sha256Hash, Integer> ordered = new LinkedHashMap<>(countsByRow.size());
        for (byte[] row : rowList) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            ordered.put(rowHash, countsByRow.getOrDefault(rowHash, 0));
        }
        return ordered;
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
                log.debug(e.getMessage(), e);
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
}
