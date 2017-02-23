/**
 * Copyright 2017 Palantir Technologies
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.AccumulatorStrategies;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.BatchingStrategies;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.BatchingTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.util.DbKvsPartitioners;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

class DbKvsWideRowFetcher {
    private static final Logger log = LoggerFactory.getLogger(DbKvsWideRowFetcher.class);

    private final BatchingTaskRunner batchingQueryRunner;
    private final SqlConnectionSupplier connections;
    private final DbTableFactory dbTables;

    DbKvsWideRowFetcher(
            BatchingTaskRunner batchingQueryRunner,
            SqlConnectionSupplier connections,
            DbTableFactory dbTables) {
        this.batchingQueryRunner = batchingQueryRunner;
        this.connections = connections;
        this.dbTables = dbTables;
    }

    Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        List<byte[]> rowList = ImmutableList.copyOf(rows);
        Map<byte[], List<Map.Entry<Cell, Value>>> firstPage =
                getFirstRowsColumnRangePage(tableRef, rowList, batchColumnRangeSelection, timestamp);

        Map<byte[], RowColumnRangeIterator> ret = Maps.newHashMapWithExpectedSize(rowList.size());
        for (Map.Entry<byte[], List<Map.Entry<Cell, Value>>> e : firstPage.entrySet()) {
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

    // TODO: Make this also able to return Iterator<Map.Entry<Cell, Long>> (currently, Iterator<Map.Entry<Cell, Value>>
    // TODO: Lots of methods here should possibly be generified
    RowColumnRangeIterator getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int cellBatchHint, long timestamp) {
        List<byte[]> rowList = ImmutableList.copyOf(rows);
        Map<Sha256Hash, byte[]> rowHashesToBytes = Maps.uniqueIndex(rowList, Sha256Hash::computeHash);

        Map<Sha256Hash, Integer> columnCountByRowHash =
                getColumnCounts(tableRef, rowList, columnRangeSelection, timestamp);

        Iterator<Map<Sha256Hash, Integer>> batches =
                DbKvsPartitioners.partitionByTotalCount(columnCountByRowHash, cellBatchHint).iterator();
        Iterator<Iterator<Map.Entry<Cell, Value>>> results =
                loadColumnsForBatches(tableRef,
                        columnRangeSelection,
                        timestamp,
                        rowHashesToBytes,
                        batches,
                        columnCountByRowHash);
        return new LocalRowColumnRangeIterator(Iterators.concat(results));
    }

    private Iterator<Iterator<Map.Entry<Cell, Value>>> loadColumnsForBatches(
            TableReference tableRef,
            ColumnRangeSelection columnRangeSelection,
            long timestamp,
            Map<Sha256Hash, byte[]> rowHashesToBytes,
            Iterator<Map<Sha256Hash, Integer>> batches,
            Map<Sha256Hash, Integer> columnCountByRowHash) {
        Iterator<Iterator<Map.Entry<Cell, Value>>> results = new AbstractIterator<Iterator<Map.Entry<Cell, Value>>>() {
            private Sha256Hash lastRowHashInPreviousBatch = null;
            private byte[] lastColumnInPreviousBatch = null;

            @Override
            protected Iterator<Map.Entry<Cell, Value>> computeNext() {
                if (!batches.hasNext()) {
                    return endOfData();
                }
                Map<Sha256Hash, Integer> currentBatch = batches.next();
                RowsColumnRangeBatchRequest columnRangeSelectionsByRow =
                        getBatchColumnRangeSelectionsByRow(currentBatch, columnCountByRowHash);

                Map<byte[], List<Map.Entry<Cell, Value>>> resultsByRow =
                        extractRowColumnRangePage(tableRef, columnRangeSelectionsByRow, timestamp);
                int totalEntries = resultsByRow.values().stream().mapToInt(List::size).sum();
                if (totalEntries == 0) {
                    return Collections.emptyIterator();
                }
                // Ensure order matches that of the provided batch.
                List<Map.Entry<Cell, Value>> loadedColumns = new ArrayList<Map.Entry<Cell, Value>>(totalEntries);
                for (Sha256Hash rowHash : currentBatch.keySet()) {
                    byte[] row = rowHashesToBytes.get(rowHash);
                    loadedColumns.addAll(resultsByRow.get(row));
                }

                Cell lastCell = Iterables.getLast(loadedColumns).getKey();
                lastRowHashInPreviousBatch = Sha256Hash.computeHash(lastCell.getRowName());
                lastColumnInPreviousBatch = lastCell.getColumnName();

                return loadedColumns.iterator();
            }

            private RowsColumnRangeBatchRequest getBatchColumnRangeSelectionsByRow(
                    Map<Sha256Hash, Integer> columnCountsByRowHashInBatch,
                    Map<Sha256Hash, Integer> totalColumnCountsByRowHash) {
                ImmutableRowsColumnRangeBatchRequest.Builder rowsColumnRangeBatch =
                        ImmutableRowsColumnRangeBatchRequest.builder().columnRangeSelection(columnRangeSelection);
                Iterator<Map.Entry<Sha256Hash, Integer>> entries = columnCountsByRowHashInBatch.entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<Sha256Hash, Integer> entry = entries.next();
                    Sha256Hash rowHash = entry.getKey();
                    byte[] row = rowHashesToBytes.get(rowHash);
                    boolean isPartialFirstRow = Objects.equals(lastRowHashInPreviousBatch, rowHash);
                    if (isPartialFirstRow) {
                        byte[] startCol = RangeRequests.nextLexicographicName(lastColumnInPreviousBatch);
                        BatchColumnRangeSelection columnRange =
                                BatchColumnRangeSelection.create(
                                        startCol,
                                        columnRangeSelection.getEndCol(),
                                        entry.getValue());
                        rowsColumnRangeBatch.partialFirstRow(Maps.immutableEntry(row, columnRange));
                        continue;
                    }
                    boolean isFullyLoadedRow = totalColumnCountsByRowHash.get(rowHash).equals(entry.getValue());
                    if (isFullyLoadedRow) {
                        rowsColumnRangeBatch.addRowsToLoadFully(row);
                    } else {
                        Preconditions.checkArgument(!entries.hasNext(), "Only the last row should be partial.");
                        BatchColumnRangeSelection columnRange =
                                BatchColumnRangeSelection.create(columnRangeSelection, entry.getValue());
                        rowsColumnRangeBatch.partialLastRow(Maps.immutableEntry(row, columnRange));
                    }
                }
                return rowsColumnRangeBatch.build();
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
                Map.Entry<Cell, Value>, TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> getFirstPage()
                            throws Exception {
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
                        List<Map.Entry<Cell, Value>> nextPage = Iterables.getOnlyElement(
                                extractRowColumnRangePage(tableRef, range, timestamp, rowList).values());
                        if (nextPage.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol,
                                    ImmutableList.<Map.Entry<Cell, Value>>of(), false);
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
            return extractRowColumnRangePage(tableRef, columnRangeSelection, ts, rows);
        } finally {
            log.debug("Call to KVS.getFirstRowColumnRangePage on table {} took {} ms.",
                    tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> extractRowColumnRangePage(
            TableReference tableRef,
            BatchColumnRangeSelection columnRangeSelection,
            long ts,
            List<byte[]> rows) {
        return extractRowColumnRangePage(tableRef, Maps.toMap(rows, Functions.constant(columnRangeSelection)), ts);
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> extractRowColumnRangePage(
            TableReference tableRef,
            Map<byte[], BatchColumnRangeSelection> columnRangeSelection,
            long ts) {
        return batchingQueryRunner.runTask(
                columnRangeSelection,
                BatchingStrategies.forMap(),
                AccumulatorStrategies.forMap(),
                batch -> runRead(tableRef, table ->
                        extractRowColumnRangePageInternal(
                                table,
                                () -> table.getRowsColumnRange(batch, ts),
                                batch.keySet())));
    }

    // TODO Generify
    private Map<byte[], List<Map.Entry<Cell, Value>>> extractRowColumnRangePage(
            TableReference tableRef,
            RowsColumnRangeBatchRequest rowsColumnRangeBatch,
            long ts) {
        return batchingQueryRunner.runTask(
                rowsColumnRangeBatch,
                RowsColumnRangeBatchRequests::partition,
                AccumulatorStrategies.forMap(),
                batch -> runRead(tableRef, table ->
                        extractRowColumnRangePageInternal(
                                table,
                                () -> table.getRowsColumnRange(batch, ts), // TODO different method for timestamps
                                RowsColumnRangeBatchRequests.getAllRowsInOrder(batch))));
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> extractRowColumnRangePageInternal(
            DbReadTable table,
            Supplier<ClosableIterator<AgnosticLightResultRow>> rowLoader,
            Collection<byte[]> allRows) {
        Map<Sha256Hash, byte[]> hashesToBytes = Maps.newHashMapWithExpectedSize(allRows.size());
        Map<Sha256Hash, List<Cell>> cellsByRow = Maps.newHashMap();
        for (byte[] row : allRows) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            hashesToBytes.put(rowHash, row);
            cellsByRow.put(rowHash, Lists.newArrayList());
        }

        boolean hasOverflow = table.hasOverflowValues();
        Map<Cell, Value> values = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowValues = Maps.newHashMap();

        try (ClosableIterator<AgnosticLightResultRow> iter = rowLoader.get()) {
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                Sha256Hash rowHash = Sha256Hash.computeHash(cell.getRowName());
                cellsByRow.get(rowHash).add(cell);

                // TODO prob no need for overflow logic for timestamps
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
                Maps.newHashMapWithExpectedSize(allRows.size());
        for (Map.Entry<Sha256Hash, List<Cell>> e : cellsByRow.entrySet()) {
            List<Map.Entry<Cell, Value>> fullResults = Lists.newArrayListWithExpectedSize(e.getValue().size());
            for (Cell c : e.getValue()) {
                fullResults.add(Iterables.getOnlyElement(ImmutableMap.of(c, values.get(c)).entrySet()));
            }
            results.put(hashesToBytes.get(e.getKey()), fullResults);
        }
        return results;
    }

    // TODO also present in DbKvs - find a better place for it
    private void fillOverflowValues(DbReadTable table,
            Map<Cell, OverflowValue> overflowValues,
            @Output Map<Cell, Value> values) {
        Iterator<Map.Entry<Cell, OverflowValue>> overflowIterator = overflowValues.entrySet().iterator();
        while (overflowIterator.hasNext()) {
            Map.Entry<Cell, OverflowValue> entry = overflowIterator.next();
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
            for (Map.Entry<Cell, OverflowValue> entry : overflowValues.entrySet()) {
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
        Map<Sha256Hash, Integer> countsByRow = batchingQueryRunner.runTask(
                rowList,
                BatchingStrategies.forList(),
                AccumulatorStrategies.forMap(),
                partition -> getColumnCountsUnordered(tableRef, partition, columnRangeSelection, timestamp));
        // Make iteration order of the returned map match the provided list.
        Map<Sha256Hash, Integer> ordered = new LinkedHashMap<Sha256Hash, Integer>(countsByRow.size());
        for (byte[] row : rowList) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            ordered.put(rowHash, countsByRow.getOrDefault(rowHash, 0));
        }
        return ordered;
    }

    private Map<Sha256Hash, Integer> getColumnCountsUnordered(TableReference tableRef,
            List<byte[]> rowList,
            ColumnRangeSelection columnRangeSelection,
            long timestamp) {
        return runRead(tableRef, dbReadTable -> {
            Map<Sha256Hash, Integer> counts = new HashMap<Sha256Hash, Integer>(rowList.size());
            try (ClosableIterator<AgnosticLightResultRow> iter =
                    // TODO does this exclude old values?
                    dbReadTable.getRowsColumnRangeCounts(rowList, timestamp, columnRangeSelection)) {
                while (iter.hasNext()) {
                    AgnosticLightResultRow row = iter.next();
                    Sha256Hash rowHash = Sha256Hash.computeHash(row.getBytes("row_name"));
                    counts.put(rowHash, row.getInteger("column_count"));
                }
            }
            return counts;
        });
    }

    // TODO - duplicated from DbKvs - should find a single place for this logic
    private <T> T runRead(TableReference tableRef, Function<DbReadTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createRead(tableRef, conns));
        } finally {
            conns.close();
        }
    }

}
