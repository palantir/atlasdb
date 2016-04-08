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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
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

    private final KeyValueService delegate;

    private ProfilingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.addGarbageCollectionSentinelValues(tableName, cells);
            log.trace("Call to KVS.addGarbageCollectionSentinelValues on table {} over {} cells took {} ms.",
                    tableName, cells.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            delegate.addGarbageCollectionSentinelValues(tableName, cells);
        }
    }

    @Override
    public void createTable(String tableName, byte[] tableMetadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.createTable(tableName, tableMetadata);
            logTimeAndTable("createTable", tableName, stopwatch);
        } else {
            delegate.createTable(tableName, tableMetadata);
        }
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.createTables(tableNameToTableMetadata);
            logTimeAndTableCount("createTables", tableNameToTableMetadata.keySet().size(), stopwatch);
        } else {
            delegate.createTables(tableNameToTableMetadata);
        }
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.delete(tableName, keys);
            logCellsAndSize("delete", tableName, keys.keySet().size(), byteSize(keys), stopwatch);
        } else {
            delegate.delete(tableName, keys);
        }
    }

    @Override
    public void dropTable(String tableName) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.dropTable(tableName);
            logTimeAndTable("dropTable", tableName, stopwatch);
        } else {
            delegate.dropTable(tableName);
        }
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.dropTables(tableNames);
            logTimeAndTableCount("dropTable", tableNames.size(), stopwatch);
        } else {
            delegate.dropTables(tableNames);
        }
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long sizeInBytes = 0;
            Map<Cell, Value> result = delegate.get(tableName, timestampByCell);
            for (Entry<Cell, Value> entry : result.entrySet()) {
                sizeInBytes += Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length + 4L;
            }
            log.trace("Call to KVS.get on table {}, requesting {} cells took {} ms and returned {} bytes.",
                    tableName, timestampByCell.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), sizeInBytes);
            return result;
        } else {
            return delegate.get(tableName, timestampByCell);
        }
    }

    @Override
    public Set<String> getAllTableNames() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Set<String> result = delegate.getAllTableNames();
            logTime("getAllTableNames", stopwatch);
            return result;
        } else {
            return delegate.getAllTableNames();
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Multimap<Cell, Long> result = delegate.getAllTimestamps(tableName, cells, timestamp);
            logCellsAndSize("getAllTimestamps", tableName, cells.size(), cells.size() * Longs.BYTES, stopwatch);
            return result;
        } else {
            return delegate.getAllTimestamps(tableName, cells, timestamp);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName, Iterable<RangeRequest> rangeRequests, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> result = delegate.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
            logTimeAndTable("getFirstBatchForRanges", tableName, stopwatch);
            return result;
        } else {
            return delegate.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<Cell, Long> result = delegate.getLatestTimestamps(tableName, timestampByCell);
            logCellsAndSize("getLatestTimestamps", tableName, timestampByCell.size(), byteSize(timestampByCell), stopwatch);
            return result;
        } else {
            return delegate.getLatestTimestamps(tableName, timestampByCell);
        }
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            byte[] result = delegate.getMetadataForTable(tableName);
            logTimeAndTable("getMetadataForTable", tableName, stopwatch);
            return result;
        } else {
            return delegate.getMetadataForTable(tableName);
        }
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<String, byte[]> result = delegate.getMetadataForTables();
            logTime("getMetadataForTables", stopwatch);
            return result;
        } else {
            return delegate.getMetadataForTables();
        }
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName, RangeRequest rangeRequest, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            ClosableIterator<RowResult<Value>> result = delegate.getRange(tableName, rangeRequest, timestamp);
            logTimeAndTable("getRange", tableName, stopwatch);
            return result;
        } else {
            return delegate.getRange(tableName, rangeRequest, timestamp);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName, RangeRequest rangeRequest, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            ClosableIterator<RowResult<Set<Long>>> result = delegate.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
            logTimeAndTable("getRangeOfTimestamps", tableName, stopwatch);
            return result;
        } else {
            return delegate.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName, RangeRequest rangeRequest, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            ClosableIterator<RowResult<Set<Value>>> result = delegate.getRangeWithHistory(tableName, rangeRequest, timestamp);
            logTimeAndTable("getRangeWithHistory", tableName, stopwatch);
            return result;
        } else {
            return delegate.getRangeWithHistory(tableName, rangeRequest, timestamp);
        }
    }

    @Override
    public Map<Cell, Value> getRows(String tableName, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long sizeInBytes = 0;
            Map<Cell, Value> result = delegate.getRows(tableName, rows, columnSelection, timestamp);
            for (Entry<Cell, Value> entry : result.entrySet()) {
                sizeInBytes += Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length;
            }
            log.trace("Call to KVS.getRows on table {} requesting {} columns from {} rows took {} ms and returned {} bytes.",
                    tableName,
                    columnSelection.allColumnsSelected() ? "all" : Iterables.size(columnSelection.getSelectedColumns()),
                    Iterables.size(rows),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS),
                    sizeInBytes);
            return result;
        } else {
            return delegate.getRows(tableName, rows, columnSelection, timestamp);
        }
    }

    @Override
    public void initializeFromFreshInstance() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.initializeFromFreshInstance();
            logTime("initializeFromFreshInstance", stopwatch);
        } else {
            delegate.initializeFromFreshInstance();
        }
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
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
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.put(tableName, values, timestamp);
            logCellsAndSize("put", tableName, values.keySet().size(), byteSize(values), stopwatch);
        } else {
            delegate.put(tableName, values, timestamp);
        }
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putMetadataForTable(tableName, metadata);
            logTimeAndTable("putMetadataForTable", tableName, stopwatch);
        } else {
            delegate.putMetadataForTable(tableName, metadata);
        }
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putMetadataForTables(tableNameToMetadata);
            logTimeAndTableCount("putMetadataForTables", tableNameToMetadata.keySet().size(), stopwatch);
        } else {
            delegate.putMetadataForTables(tableNameToMetadata);
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putUnlessExists(tableName, values);
            logCellsAndSize("putUnlessExists", tableName, values.keySet().size(), byteSize(values), stopwatch);
        } else {
            delegate.putUnlessExists(tableName, values);
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.putWithTimestamps(tableName, values);
            logCellsAndSize("putWithTimestamps", tableName, values.keySet().size(), byteSize(values), stopwatch);
        } else {
            delegate.putWithTimestamps(tableName, values);
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
    public void teardown() {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.teardown();
            logTime("teardown", stopwatch);
        } else {
            delegate.teardown();
        }
    }

    @Override
    public void truncateTable(String tableName) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.truncateTable(tableName);
            logTimeAndTable("truncateTable", tableName, stopwatch);
        } else {
            delegate.truncateTable(tableName);
        }
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.truncateTables(tableNames);
            logTimeAndTableCount("truncateTables", tableNames.size(), stopwatch);
        } else {
            delegate.truncateTables(tableNames);
        }
    }

    @Override
    public void compactInternally(String tableName) {
        if (log.isTraceEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            delegate.compactInternally(tableName);
            logTimeAndTable("compactInternally", tableName, stopwatch);
        } else {
            delegate.compactInternally(tableName);
        }
    }
}
