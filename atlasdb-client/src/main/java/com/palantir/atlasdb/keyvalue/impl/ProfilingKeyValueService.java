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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
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
     * Defaults to using a 1 second slowlog.
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


    private static BiConsumer<LoggingFunction, Stopwatch> logCellsAndSize(String method,
            TableReference tableRef,
            int numCells, long sizeInBytes) {
        return (logger, stopwatch) ->
                logger.log("Call to KVS.{} on table {} for {} cells of overall size {} bytes took {} ms.",
                        method, tableRef, numCells, sizeInBytes, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static BiConsumer<LoggingFunction, Stopwatch> logTime(String method) {
        return (logger, stopwatch) ->
                logger.log("Call to KVS.{} took {} ms.", method, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static BiConsumer<LoggingFunction, Stopwatch> logTimeAndTable(String method, TableReference tableRef) {
        return (logger, stopwatch) ->
                logger.log("Call to KVS.{} on table {} took {} ms.",
                        method, tableRef, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static BiConsumer<LoggingFunction, Stopwatch> logTimeAndTableCount(String method, int tableCount) {
        return (logger, stopwatch) ->
                logger.log("Call to KVS.{} for {} tables took {} ms.",
                        method, tableCount, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static BiConsumer<LoggingFunction, Stopwatch> logTimeAndTableRange(String method,
            TableReference tableRef,
            RangeRequest range) {
        return (logger, stopwatch) ->
                logger.log("Call to KVS.{} on table {} with range {} took {} ms.",
                        method, tableRef, range, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static BiConsumer<LoggingFunction, Map<Cell, Value>> logCellResultSize(long overhead) {
        return (logger, result) -> {
            long sizeInBytes = 0;
            for (Entry<Cell, Value> entry : result.entrySet()) {
                sizeInBytes +=
                        Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length + overhead;
            }
            logger.log("and returned {} bytes.", sizeInBytes);
        };
    }


    private void maybeLog(Runnable runnable, BiConsumer<LoggingFunction, Stopwatch> logger) {
        maybeLog(() -> {
            runnable.run();
            return null;
        }, logger);
    }

    private <T> T maybeLog(Supplier<T> action, BiConsumer<LoggingFunction, Stopwatch> logger) {
        return maybeLog(action, logger, (loggingFunction, result) -> { });
    }

    private static class Monitor<R> {
        private final Stopwatch stopwatch;
        private final BiConsumer<LoggingFunction, Stopwatch> primaryLogger;
        private final BiConsumer<LoggingFunction, R> additionalLoggerWithAccessToResult;
        private final Predicate<Stopwatch> slowLogPredicate;

        private R result;
        private Exception exception;

        private Monitor(Stopwatch stopwatch,
                BiConsumer<LoggingFunction, Stopwatch> primaryLogger,
                BiConsumer<LoggingFunction, R> additionalLoggerWithAccessToResult,
                Predicate<Stopwatch> slowLogPredicate) {
            this.stopwatch = stopwatch;
            this.primaryLogger = primaryLogger;
            this.additionalLoggerWithAccessToResult = additionalLoggerWithAccessToResult;
            this.slowLogPredicate = slowLogPredicate;
        }

        static <V> Monitor<V> createMonitor(BiConsumer<LoggingFunction,
                Stopwatch> primaryLogger,
                BiConsumer<LoggingFunction, V> additionalLoggerWithAccessToResult,
                Predicate<Stopwatch> slowLogPredicate) {
            return new Monitor<>(Stopwatch.createStarted(),
                    primaryLogger,
                    additionalLoggerWithAccessToResult,
                    slowLogPredicate);
        }

        void registerResult(R res) {
            this.result = res;
        }

        void registerException(Exception ex) {
            this.exception = ex;
        }

        void log() {
            stopwatch.stop();
            Consumer<LoggingFunction> logger = (loggingMethod) -> {
                primaryLogger.accept(loggingMethod, stopwatch);
                if (result != null) additionalLoggerWithAccessToResult.accept(loggingMethod, result);
                else if (exception != null) loggingMethod.log("This operation has thrown an exception {}", exception);
            };

            if (log.isTraceEnabled()) {
                logger.accept(log::trace);
            }
            if (slowlogger.isWarnEnabled() && slowLogPredicate.test(stopwatch)) {
                logger.accept(slowlogger::warn);
            }
        }
    }

    private <T> T maybeLog(Supplier<T> action, BiConsumer<LoggingFunction, Stopwatch> primaryLogger,
            BiConsumer<LoggingFunction, T> additonalLoggerWithAccessToResult) {
        if (log.isTraceEnabled() || slowlogger.isWarnEnabled()) {
            Monitor<T> monitor = Monitor.createMonitor(
                    primaryLogger,
                    additonalLoggerWithAccessToResult,
                    slowLogPredicate);
            try {
                T res = action.get();
                monitor.registerResult(res);
                return res;
            } catch (Exception ex) {
                monitor.registerException(ex);
                throw ex;
            } finally {
                monitor.log();
            }
        } else {
            return action.get();
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
                logTimeAndTable("createTable", tableRef));

    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        maybeLog(() -> delegate.createTables(tableRefToTableMetadata),
                logTimeAndTableCount("createTables", tableRefToTableMetadata.keySet().size()));
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        maybeLog(() -> delegate.delete(tableRef, keys),
                logCellsAndSize("delete", tableRef, keys.keySet().size(), byteSize(keys)));
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        maybeLog(() -> delegate.deleteRange(tableRef, range),
                logTimeAndTableRange("deleteRange", tableRef, range));
    }

    @Override
    public void dropTable(TableReference tableRef) {
        maybeLog(() -> delegate.dropTable(tableRef),
                logTimeAndTable("dropTable", tableRef));
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        maybeLog(() -> delegate.dropTables(tableRefs),
                logTimeAndTableCount("dropTable", tableRefs.size()));
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return maybeLog(() -> delegate.get(tableRef, timestampByCell),
                (logger, stopwatch) ->
                        logger.log("Call to KVS.get on table {}, requesting {} cells took {} ms ",
                                tableRef, timestampByCell.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS)),
                logCellResultSize(4L));
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return maybeLog(delegate::getAllTableNames,
                logTime("getAllTableNames"));
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        return maybeLog(() -> delegate.getAllTimestamps(tableRef, cells, timestamp),
                logCellsAndSize("getAllTimestamps", tableRef, cells.size(), cells.size() * Longs.BYTES));
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return maybeLog(() -> delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp),
                logTimeAndTable("getFirstBatchForRanges", tableRef));
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return maybeLog(() -> delegate.getLatestTimestamps(tableRef, timestampByCell),
                logCellsAndSize("getLatestTimestamps", tableRef, timestampByCell.size(), byteSize(timestampByCell)));
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return maybeLog(() -> delegate.getMetadataForTable(tableRef),
                logTimeAndTable("getMetadataForTable", tableRef));
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return maybeLog(delegate::getMetadataForTables,
                logTime("getMetadataForTables"));
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest,
            long timestamp) {
        return maybeLog(() -> delegate.getRange(tableRef, rangeRequest, timestamp),
                logTimeAndTableRange("getRange", tableRef, rangeRequest));
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
            RangeRequest rangeRequest, long timestamp) {
        return maybeLog(() -> delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp),
                logTimeAndTableRange("getRangeOfTimestamps", tableRef, rangeRequest));
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return maybeLog(() -> delegate.getCandidateCellsForSweeping(tableRef, request),
                logTime("getCandidateCellsForSweeping"));
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
                logCellResultSize(0L));
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
                logCellsAndSize("put", tableRef, values.keySet().size(), byteSize(values)));
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        maybeLog(() -> delegate.putMetadataForTable(tableRef, metadata),
                logTimeAndTable("putMetadataForTable", tableRef));
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        maybeLog(() -> delegate.putMetadataForTables(tableRefToMetadata),
                logTimeAndTableCount("putMetadataForTables", tableRefToMetadata.keySet().size()));
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        maybeLog(() -> delegate.putUnlessExists(tableRef, values),
                logCellsAndSize("putUnlessExists", tableRef, values.keySet().size(), byteSize(values)));
    }

    @Override
    public boolean supportsCheckAndSet() {
        return delegate.supportsCheckAndSet();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        maybeLog(() -> delegate.checkAndSet(request),
                logCellsAndSize("checkAndSet", request.table(), 1, request.newValue().length));
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        maybeLog(() -> delegate.putWithTimestamps(tableRef, values),
                logCellsAndSize("putWithTimestamps", tableRef, values.keySet().size(), byteSize(values)));
    }

    @Override
    public void close() {
        maybeLog(delegate::close,
                logTime("close"));
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        maybeLog(() -> delegate.truncateTable(tableRef),
                logTimeAndTable("truncateTable", tableRef));
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        maybeLog(() -> delegate.truncateTables(tableRefs),
                logTimeAndTableCount("truncateTables", tableRefs.size()));
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        maybeLog(() -> delegate.compactInternally(tableRef),
                logTimeAndTable("compactInternally", tableRef));
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return maybeLog(delegate::getClusterAvailabilityStatus,
                logTime("getClusterAvailabilityStatus"));
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        return maybeLog(() -> delegate.getRowsColumnRange(tableRef, rows,
                batchColumnRangeSelection, timestamp),
                (logger, stopwatch) ->
                        logger.log("Call to KVS.getRowsColumnRange on table {} for {} rows with range {} took {} ms.",
                        tableRef, Iterables.size(rows), batchColumnRangeSelection,
                        stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return maybeLog(() ->
                        delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp),
                (logger, stopwatch) -> logger.log(
                        "Call to KVS.getRowsColumnRange - CellBatch on table {} for {} rows with range {} "
                                + "and batch hint {} took {} ms.",
                        tableRef,
                        Iterables.size(rows),
                        columnRangeSelection,
                        cellBatchHint,
                        stopwatch.elapsed(TimeUnit.MILLISECONDS)));
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
