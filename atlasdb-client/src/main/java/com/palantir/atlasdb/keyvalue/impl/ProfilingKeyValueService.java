/*
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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class ProfilingKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(ProfilingKeyValueService.class);

    private static <T> long byteSize(Map<Cell, T> values) {
        long sizeInBytes = 0;
        for (Entry<Cell, T> valueEntry : values.entrySet()) {
            sizeInBytes += Cells.getApproxSizeOfCell(valueEntry.getKey());
            T value = valueEntry.getValue();
            if (value instanceof byte[]) {
                sizeInBytes += ((byte[]) value).length;
            } else if (value instanceof Long) {
                sizeInBytes += Longs.BYTES;
            }
        }
        return sizeInBytes;
    }

    private static <T> long byteSize(Multimap<Cell, T> values) {
        long sizeInBytes = 0;
        for (Cell cell : values.keySet()) {
            sizeInBytes += Cells.getApproxSizeOfCell(cell) + values.get(cell).size();
        }
        return sizeInBytes;
    }

    public static ProfilingKeyValueService create(KeyValueService delegate) {
        return new ProfilingKeyValueService(delegate);
    }

    private static void logCellsAndSize(String method, String tableName, int numCells, long sizeInBytes, Stopwatch stopwatch) {
        log.trace("Call to KVS.{} on table {} for {} cells of overall size {} bytes took {} ms.",
                method, tableName, numCells, sizeInBytes, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTime(String method, Stopwatch stopwatch) {
        log.trace("Call to KVS.{} took {} ms.", method, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTimeAndTable(String method, String tableName, Stopwatch stopwatch) {
        log.trace("Call to KVS.{} on table {} took {} ms.",
                method, tableName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTimeAndTableCount(String method, int tableCount, Stopwatch stopwatch) {
        log.trace("Call to KVS.{} for {} tables took {} ms.",
                method, tableCount, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTimeAndTableRange(String method, String tableName, RangeRequest range, Stopwatch stopwatch) {
        log.trace("Call to KVS.{} on table {} with range {} took {} ms.",
                method, tableName, range, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private final KeyValueService delegate;

    private ProfilingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.addGarbageCollectionSentinelValues(tableRef, cells);
            log.trace("Call to KVS.addGarbageCollectionSentinelValues on table {} over {} cells took {} ms.",
                    tableRef, cells.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            delegate.addGarbageCollectionSentinelValues(tableRef, cells);
        }
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.createTable(tableRef, tableMetadata);
            logTimeAndTable("createTable", tableRef.getQualifiedName(), stopwatch);
        } else {
            delegate.createTable(tableRef, tableMetadata);
        }
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.createTables(tableRefToTableMetadata);
            logTimeAndTableCount("createTables", tableRefToTableMetadata.keySet().size(), stopwatch);
        } else {
            delegate.createTables(tableRefToTableMetadata);
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.delete(tableRef, keys);
            logCellsAndSize("delete", tableRef.getQualifiedName(), keys.keySet().size(), byteSize(keys), stopwatch);
        } else {
            delegate.delete(tableRef, keys);
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.deleteRange(tableRef, range);
            logTimeAndTableRange("deleteRange", tableRef.getQualifiedName(), range, stopwatch);
        } else {
            delegate.deleteRange(tableRef, range);
        }
    }

    @Override
    public void dropTable(TableReference tableRef) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.dropTable(tableRef);
            logTimeAndTable("dropTable", tableRef.getQualifiedName(), stopwatch);
        } else {
            delegate.dropTable(tableRef);
        }
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.dropTables(tableRefs);
            logTimeAndTableCount("dropTable", tableRefs.size(), stopwatch);
        } else {
            delegate.dropTables(tableRefs);
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long sizeInBytes = 0;
            Map<Cell, Value> result = delegate.get(tableRef, timestampByCell);
            for (Entry<Cell, Value> entry : result.entrySet()) {
                sizeInBytes += Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length + 4L;
            }
            log.trace("Call to KVS.get on table {}, requesting {} cells took {} ms and returned {} bytes.",
                    tableRef, timestampByCell.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), sizeInBytes);
            return result;
        } else {
            return delegate.get(tableRef, timestampByCell);
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Set<TableReference> result = delegate.getAllTableNames();
            logTime("getAllTableNames", stopwatch);
            return result;
        } else {
            return delegate.getAllTableNames();
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Multimap<Cell, Long> result = delegate.getAllTimestamps(tableRef, cells, timestamp);
            logCellsAndSize("getAllTimestamps", tableRef.getQualifiedName(), cells.size(), cells.size() * Longs.BYTES, stopwatch);
            return result;
        } else {
            return delegate.getAllTimestamps(tableRef, cells, timestamp);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> result = delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
            logTimeAndTable("getFirstBatchForRanges", tableRef.getQualifiedName(), stopwatch);
            return result;
        } else {
            return delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<Cell, Long> result = delegate.getLatestTimestamps(tableRef, timestampByCell);
            logCellsAndSize("getLatestTimestamps", tableRef.getQualifiedName(), timestampByCell.size(), byteSize(timestampByCell), stopwatch);
            return result;
        } else {
            return delegate.getLatestTimestamps(tableRef, timestampByCell);
        }
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            byte[] result = delegate.getMetadataForTable(tableRef);
            logTimeAndTable("getMetadataForTable", tableRef.getQualifiedName(), stopwatch);
            return result;
        } else {
            return delegate.getMetadataForTable(tableRef);
        }
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<TableReference, byte[]> result = delegate.getMetadataForTables();
            logTime("getMetadataForTables", stopwatch);
            return result;
        } else {
            return delegate.getMetadataForTables();
        }
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            ClosableIterator<RowResult<Value>> result = delegate.getRange(tableRef, rangeRequest, timestamp);
            logTimeAndTableRange("getRange", tableRef.getQualifiedName(), rangeRequest, stopwatch);
            return result;
        } else {
            return delegate.getRange(tableRef, rangeRequest, timestamp);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            ClosableIterator<RowResult<Set<Long>>> result = delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
            logTimeAndTableRange("getRangeOfTimestamps", tableRef.getQualifiedName(), rangeRequest, stopwatch);
            return result;
        } else {
            return delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
        }
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long sizeInBytes = 0;
            Map<Cell, Value> result = delegate.getRows(tableRef, rows, columnSelection, timestamp);
            for (Entry<Cell, Value> entry : result.entrySet()) {
                sizeInBytes += Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length;
            }
            log.trace("Call to KVS.getRows on table {} requesting {} columns from {} rows took {} ms and returned {} bytes.",
                    tableRef,
                    columnSelection.allColumnsSelected() ? "all" : Iterables.size(columnSelection.getSelectedColumns()),
                    Iterables.size(rows),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS),
                    sizeInBytes);
            return result;
        } else {
            return delegate.getRows(tableRef, rows, columnSelection, timestamp);
        }
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.multiPut(valuesByTable, timestamp);
            int totalCells = 0;
            long totalBytes = 0;
            for (Map<Cell, byte[]> values : valuesByTable.values()) {
                totalCells += values.size();
                totalBytes += byteSize(values);
            }
            log.trace("Call to KVS.multiPut on {} tables putting {} total cells of {} total bytes took {} ms.",
                    valuesByTable.keySet().size(), totalCells, totalBytes, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            delegate.multiPut(valuesByTable, timestamp);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.put(tableRef, values, timestamp);
            logCellsAndSize("put", tableRef.getQualifiedName(), values.keySet().size(), byteSize(values), stopwatch);
        } else {
            delegate.put(tableRef, values, timestamp);
        }
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putMetadataForTable(tableRef, metadata);
            logTimeAndTable("putMetadataForTable", tableRef.getQualifiedName(), stopwatch);
        } else {
            delegate.putMetadataForTable(tableRef, metadata);
        }
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putMetadataForTables(tableRefToMetadata);
            logTimeAndTableCount("putMetadataForTables", tableRefToMetadata.keySet().size(), stopwatch);
        } else {
            delegate.putMetadataForTables(tableRefToMetadata);
        }
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putUnlessExists(tableRef, values);
            logCellsAndSize("putUnlessExists", tableRef.getQualifiedName(), values.keySet().size(), byteSize(values), stopwatch);
        } else {
            delegate.putUnlessExists(tableRef, values);
        }
    }

    @Override
    public boolean supportsCheckAndSet() {
        return delegate.supportsCheckAndSet();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.checkAndSet(request);
            logCellsAndSize("checkAndSet", request.table().getQualifiedName(), 1, request.newValue().length, stopwatch);
        } else {
            delegate.checkAndSet(request);
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putWithTimestamps(tableRef, values);
            logCellsAndSize("putWithTimestamps", tableRef.getQualifiedName(), values.keySet().size(), byteSize(values), stopwatch);
        } else {
            delegate.putWithTimestamps(tableRef, values);
        }
    }

    @Override
    public void close() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.close();
            logTime("close", stopwatch);
        } else {
            delegate.close();
        }
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.truncateTable(tableRef);
            logTimeAndTable("truncateTable", tableRef.getQualifiedName(), stopwatch);
        } else {
            delegate.truncateTable(tableRef);
        }
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.truncateTables(tableRefs);
            logTimeAndTableCount("truncateTables", tableRefs.size(), stopwatch);
        } else {
            delegate.truncateTables(tableRefs);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.compactInternally(tableRef);
            logTimeAndTable("compactInternally", tableRef.getQualifiedName(), stopwatch);
        } else {
            delegate.compactInternally(tableRef);
        }
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
                                                                  BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<byte[], RowColumnRangeIterator> result = delegate.getRowsColumnRange(tableRef, rows,
                    batchColumnRangeSelection, timestamp);
            log.trace("Call to KVS.getRowsColumnRange on table {} for {} rows with range {} took {} ms.",
                    tableRef.getQualifiedName(), Iterables.size(rows), batchColumnRangeSelection, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            logTimeAndTable("getRowsColumnRange", tableRef.getQualifiedName(), stopwatch);
            return result;
        } else {
            return delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
        }
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            RowColumnRangeIterator result =
                    delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
            log.trace("Call to KVS.getRowsColumnRangeCellBatch on table {} for {} rows with range {} and batch hint {} took {} ms.",
                      tableRef.getQualifiedName(),
                      Iterables.size(rows),
                      columnRangeSelection,
                      cellBatchHint,
                      stopwatch.elapsed(TimeUnit.MILLISECONDS));
            logTimeAndTable("getRowsColumnRangeCellBatch", tableRef.getQualifiedName(), stopwatch);
            return result;
        } else {
            return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
        }
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate.shouldTriggerCompactions();
    }
}
