/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
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

public final class ProfilingKeyValueService implements KeyValueService {
    @VisibleForTesting
    static final String SLOW_LOGGER_NAME = "kvs-slow-log";

    private static final Logger slowlogger = LoggerFactory.getLogger(SLOW_LOGGER_NAME);
    private static final Logger log = LoggerFactory.getLogger(ProfilingKeyValueService.class);

    private final KeyValueService delegate;

    private final Predicate<Stopwatch> slowLogPredicate;

    /**
     * @deprecated in favour of ProfilingKeyValueService#create(KeyValueService delegate, long slowLogThresholdMillis).
     * @param delegate the KeyValueService to be profiled
     * @return ProfilingKeyValueService that profiles the delegate KeyValueService
     */
    @Deprecated
    public static ProfilingKeyValueService create(KeyValueService delegate) {
        return new ProfilingKeyValueService(delegate, 1000);
    }

    public static ProfilingKeyValueService create(KeyValueService delegate, long slowLogThresholdMillis) {
        return new ProfilingKeyValueService(delegate, slowLogThresholdMillis);
    }

    @FunctionalInterface
    interface LoggingFunction {
        void log(String fmt, Object... args);
    }


    private static void logCellsAndSize(LoggingFunction logger, String method, TableReference tableRef, int numCells,
            long sizeInBytes, Stopwatch stopwatch) {
        logger.log("Call to KVS.{} on table {} for {} cells of overall size {} bytes took {} ms.",
                method, tableRef, numCells, sizeInBytes, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTime(LoggingFunction logger, String method, Stopwatch stopwatch) {
        logger.log("Call to KVS.{} took {} ms.", method, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTimeAndTable(LoggingFunction logger, String method, TableReference tableRef,
            Stopwatch stopwatch) {
        logger.log("Call to KVS.{} on table {} took {} ms.",
                method, tableRef, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTimeAndTableCount(LoggingFunction logger, String method, int tableCount,
            Stopwatch stopwatch) {
        logger.log("Call to KVS.{} for {} tables took {} ms.",
                method, tableCount, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void logTimeAndTableRange(LoggingFunction logger, String method, TableReference tableRef,
            RangeRequest range, Stopwatch stopwatch) {
        logger.log("Call to KVS.{} on table {} with range {} took {} ms.",
                method, tableRef, range, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private void maybeLog(Runnable runnable, BiConsumer<LoggingFunction, Stopwatch> logger) {
        maybeLog(() -> {
            runnable.run();
            return (Void) null;
        }, logger);
    }

    private <T> T maybeLog(Supplier<T> supplier, BiConsumer<LoggingFunction, Stopwatch> logger) {
        return maybeLog(supplier, logger, (loggingFunction, result) -> {
        });
    }

    private <T> T maybeLog(Supplier<T> supplier, BiConsumer<LoggingFunction, Stopwatch> logger,
            BiConsumer<LoggingFunction, T> additonalLoggerWithAccessToResult) {
        if (log.isTraceEnabled() || slowlogger.isWarnEnabled()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Optional<T> result = Optional.empty();
            Optional<Exception> exception = Optional.empty();
            try {
                T res = supplier.get();
                result = Optional.ofNullable(res);
                return res;
            } catch (Exception ex) {
                exception = Optional.of(ex);
                throw ex;
            } finally {
                stopwatch.stop();
                final Optional<T> finalResult = result;
                final Optional<Exception> finalException = exception;
                Consumer<LoggingFunction> doLogging = loggingFunction -> {
                    logger.accept(loggingFunction, stopwatch);
                    finalResult.ifPresent(res -> additonalLoggerWithAccessToResult.accept(loggingFunction, res));
                    finalException.ifPresent(
                            ex -> loggingFunction.log("This operation has thrown an exception {}", ex));
                };
                if (log.isTraceEnabled()) {
                    doLogging.accept(log::trace);
                }
                if (slowlogger.isWarnEnabled() && this.slowLogPredicate.test(stopwatch)) {
                    doLogging.accept(slowlogger::warn);
                }
            }
        } else {
            return supplier.get();
        }
    }

    private ProfilingKeyValueService(KeyValueService delegate, long slowLogThresholdMillis) {
        this.delegate = delegate;
        slowLogPredicate = stopwatch -> stopwatch.elapsed(TimeUnit.MILLISECONDS) > slowLogThresholdMillis;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        maybeLog(() -> delegate.addGarbageCollectionSentinelValues(tableRef, cells),
                (logger, stopwatch) -> logger.log(
                        "Call to KVS.addGarbageCollectionSentinelValues on table {} over {} cells took {} ms.",
                        tableRef, Iterables.size(cells), stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        maybeLog(() -> delegate.createTable(tableRef, tableMetadata),
                (logger, stopwatch) -> logTimeAndTable(logger, "createTable", tableRef, stopwatch));

    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        maybeLog(() -> delegate.createTables(tableRefToTableMetadata),
                (logger, stopwatch) -> logTimeAndTableCount(logger, "createTables",
                        tableRefToTableMetadata.keySet().size(),
                        stopwatch));
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        maybeLog(() -> delegate.delete(tableRef, keys),
                (logger, stopwatch) -> logCellsAndSize(logger, "delete", tableRef, keys.keySet().size(),
                        byteSize(keys), stopwatch));
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        maybeLog(() -> delegate.deleteRange(tableRef, range),
                (logger, stopwatch) ->
                        logTimeAndTableRange(logger, "deleteRange", tableRef, range, stopwatch));
    }

    @Override
    public void dropTable(TableReference tableRef) {
        maybeLog(() -> delegate.dropTable(tableRef),
                (logger, stopwatch) ->
                        logTimeAndTable(logger, "dropTable", tableRef, stopwatch));
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        maybeLog(() -> delegate.dropTables(tableRefs),
                (logger, stopwatch) ->
                        logTimeAndTableCount(logger, "dropTable", tableRefs.size(), stopwatch));
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return maybeLog(() -> delegate.get(tableRef, timestampByCell),
                (logger, stopwatch) ->
                        logger.log("Call to KVS.get on table {}, requesting {} cells took {} ms ",
                                tableRef, timestampByCell.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS)),
                (logger, result) -> {
                    long sizeInBytes = 0;
                    for (Entry<Cell, Value> entry : result.entrySet()) {
                        sizeInBytes +=
                                Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length + 4L;
                    }
                    logger.log("and returned {} bytes.", sizeInBytes);
                });
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return maybeLog(() -> delegate.getAllTableNames(),
                (logger, stopwatch) ->
                        logTime(logger, "getAllTableNames", stopwatch));
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        return maybeLog(() -> delegate.getAllTimestamps(tableRef, cells, timestamp),
                (logger, stopwatch) ->
                        logCellsAndSize(logger, "getAllTimestamps", tableRef, cells.size(), cells.size() * Longs.BYTES,
                                stopwatch));
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return maybeLog(() -> delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp),
                (logger, stopwatch) ->
                        logTimeAndTable(logger, "getFirstBatchForRanges", tableRef, stopwatch));
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return maybeLog(() -> delegate.getLatestTimestamps(tableRef, timestampByCell),
                (logger, stopwatch) ->
                        logCellsAndSize(logger, "getLatestTimestamps", tableRef, timestampByCell.size(),
                                byteSize(timestampByCell), stopwatch));
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return maybeLog(() -> delegate.getMetadataForTable(tableRef),
                (logger, stopwatch) ->
                        logTimeAndTable(logger, "getMetadataForTable", tableRef, stopwatch));
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return maybeLog(() -> delegate.getMetadataForTables(),
                (logger, stopwatch) ->
                        logTime(logger, "getMetadataForTables", stopwatch));
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest,
            long timestamp) {
        return maybeLog(() -> delegate.getRange(tableRef, rangeRequest, timestamp),
                (logger, stopwatch) ->
                        logTimeAndTableRange(logger, "getRange", tableRef, rangeRequest, stopwatch));
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
            RangeRequest rangeRequest, long timestamp) {
        return maybeLog(() -> delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp),
                (logger, stopwatch) ->
                        logTimeAndTableRange(logger, "getRangeOfTimestamps", tableRef, rangeRequest, stopwatch));
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection,
            long timestamp) {
        return maybeLog(() -> delegate.getRows(tableRef, rows, columnSelection, timestamp),
                (logger, stopwatch) ->
                        logger.log(
                                "Call to KVS.getRows on table {} requesting {} columns from {} rows took {} ms ",
                                tableRef,
                                columnSelection.allColumnsSelected() ? "all"
                                        : Iterables.size(columnSelection.getSelectedColumns()),
                                Iterables.size(rows),
                                stopwatch.elapsed(TimeUnit.MILLISECONDS)),
                (logger, result) -> {
                    long sizeInBytes = 0;
                    for (Entry<Cell, Value> entry : result.entrySet()) {
                        sizeInBytes +=
                                Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length;
                    }
                    logger.log("and returned {} bytes.", sizeInBytes);
                });
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        maybeLog(() -> delegate.multiPut(valuesByTable, timestamp),
                (logger, stopwatch) -> {
                    int totalCells = 0;
                    long totalBytes = 0;
                    for (Map<Cell, byte[]> values : valuesByTable.values()) {
                        totalCells += values.size();
                        totalBytes += byteSize(values);
                    }
                    logger.log(
                            "Call to KVS.multiPut on {} tables putting {} total cells of {} total bytes took {} ms.",
                            valuesByTable.keySet().size(), totalCells, totalBytes,
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                });
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        maybeLog(() -> delegate.put(tableRef, values, timestamp),
                (logger, stopwatch) -> logCellsAndSize(logger,
                        "put", tableRef, values.keySet().size(), byteSize(values), stopwatch));
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        maybeLog(() -> delegate.putMetadataForTable(tableRef, metadata),
                (logger, stopwatch) -> logTimeAndTable(logger,
                        "putMetadataForTable", tableRef, stopwatch));
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        maybeLog(() -> delegate.putMetadataForTables(tableRefToMetadata),
                (logger, stopwatch) -> logTimeAndTableCount(logger,
                        "putMetadataForTables", tableRefToMetadata.keySet().size(), stopwatch));
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        maybeLog(() -> delegate.putUnlessExists(tableRef, values),
                (logger, stopwatch) -> logCellsAndSize(logger,
                        "putUnlessExists", tableRef, values.keySet().size(), byteSize(values), stopwatch));
    }

    @Override
    public boolean supportsCheckAndSet() {
        return delegate.supportsCheckAndSet();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        maybeLog(() -> delegate.checkAndSet(request),
                (logger, stopwatch) -> logCellsAndSize(logger,
                        "checkAndSet", request.table(), 1, request.newValue().length, stopwatch));
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        maybeLog(() -> delegate.putWithTimestamps(tableRef, values),
                (logger, stopwatch) -> logCellsAndSize(logger,
                        "putWithTimestamps", tableRef, values.keySet().size(), byteSize(values), stopwatch));
    }

    @Override
    public void close() {
        maybeLog(() -> delegate.close(),
                (logger, stopwatch) -> logTime(logger, "close", stopwatch));
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        maybeLog(() -> delegate.truncateTable(tableRef),
                (logger, stopwatch) -> logTimeAndTable(logger,
                        "truncateTable", tableRef, stopwatch));
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        maybeLog(() -> delegate.truncateTables(tableRefs),
                (logger, stopwatch) -> logTimeAndTableCount(logger,
                        "truncateTables", tableRefs.size(), stopwatch));
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        maybeLog(() -> delegate.compactInternally(tableRef),
                (logger, stopwatch) -> logTimeAndTable(logger,
                        "compactInternally", tableRef, stopwatch));
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return maybeLog(() -> delegate.getClusterAvailabilityStatus(),
                (logger, stopwatch) -> logTime(logger,
                        "getClusterAvailabilityStatus", stopwatch));
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        return maybeLog(() -> delegate.getRowsColumnRange(tableRef, rows,
                batchColumnRangeSelection, timestamp),
                (logger, stopwatch) -> {
                    logger.log("Call to KVS.getRowsColumnRange on table {} for {} rows with range {} took {} ms.",
                            tableRef, Iterables.size(rows), batchColumnRangeSelection,
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                    logTimeAndTable(logger, "getRowsColumnRange", tableRef, stopwatch);
                });
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return maybeLog(() ->
                        delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp),
                (logger, stopwatch) -> {
                    logger.log(
                            "Call to KVS.getRowsColumnRangeCellBatch on table {} for {} rows with range {} "
                                    + "and batch hint {} took {} ms.",
                            tableRef,
                            Iterables.size(rows),
                            columnRangeSelection,
                            cellBatchHint,
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                    logTimeAndTable(logger, "getRowsColumnRangeCellBatch", tableRef, stopwatch);
                });
    }

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
}
