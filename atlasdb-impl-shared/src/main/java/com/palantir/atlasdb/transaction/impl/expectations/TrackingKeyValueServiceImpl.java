/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.expectations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.atlasdb.util.MeasuringUtils;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

public class TrackingKeyValueServiceImpl extends ForwardingKeyValueService implements TrackingKeyValueService {
    private static final SafeLogger log = SafeLoggerFactory.get(TrackingKeyValueServiceImpl.class);

    private final KeyValueService delegate;
    private final KeyValueServiceDataTracker tracker = new KeyValueServiceDataTracker();

    public TrackingKeyValueServiceImpl(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public TransactionReadInfo getOverallReadInfo() {
        return tracker.getReadInfo();
    }

    @Override
    public ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        return tracker.getReadInfoByTable();
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return Futures.transform(
                delegate.getAsync(tableRef, timestampByCell),
                result -> {
                    runWithExceptionHandling(
                            () -> tracker.recordReadForTable(tableRef, "getAsync", MeasuringUtils.sizeOf(result)));
                    return result;
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        Map<Cell, Value> result = delegate.getRows(tableRef, rows, columnSelection, timestamp);

        runWithExceptionHandling(() -> tracker.recordReadForTable(tableRef, "getRows", MeasuringUtils.sizeOf(result)));

        return result;
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        Map<byte[], RowColumnRangeIterator> result =
                delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);

        try {
            Consumer<Long> callTracker = tracker.recordCallForTable(tableRef);
            // throws if the map implementation does not support replaceAll
            result.replaceAll((rowsRead, iterator) -> {
                callTracker.accept((long) rowsRead.length);
                return new TrackingRowColumnRangeIterator(iterator, callTracker, MeasuringUtils::sizeOf);
            });
        } catch (Exception exception) {
            log.warn("Map<byte[] RowColumnRangeIterator> wrapping failed for tracking", exception);
        }

        return result;
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return wrapIteratorWithExceptionHandling(
                tableRef, delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp));
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        Map<Cell, Value> result = delegate.get(tableRef, timestampByCell);

        runWithExceptionHandling(() -> tracker.recordReadForTable(tableRef, "get", MeasuringUtils.sizeOf(result)));

        return result;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        Map<Cell, Long> result = delegate.getLatestTimestamps(tableRef, timestampByCell);

        runWithExceptionHandling(() -> tracker.recordReadForTable(
                tableRef, "getLatestTimestamps", MeasuringUtils.sizeOfMeasurableLongMap(result)));

        return result;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        try (ClosableIterator<RowResult<Value>> result = delegate.getRange(tableRef, rangeRequest, timestamp)) {
            return wrapIteratorWithExceptionHandling(tableRef, result, MeasuringUtils::sizeOf);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp)
            throws InsufficientConsistencyException {
        try (ClosableIterator<RowResult<Set<Long>>> result =
                delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp)) {
            return wrapIteratorWithExceptionHandling(tableRef, result, MeasuringUtils::sizeOfLongSetRowResult);
        }
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        try (ClosableIterator<List<CandidateCellForSweeping>> result =
                delegate.getCandidateCellsForSweeping(tableRef, request)) {
            return wrapIteratorWithExceptionHandling(tableRef, result, MeasuringUtils::sizeOf);
        }
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> result =
                delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);

        runWithExceptionHandling(() -> tracker.recordReadForTable(
                tableRef, "getFirstBatchForRanges", MeasuringUtils.sizeOfPageByRangeRequestMap(result)));

        return result;
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        Set<TableReference> result = delegate.getAllTableNames();

        runWithExceptionHandling(
                () -> tracker.recordTableAgnosticRead("getAllTableNames", MeasuringUtils.sizeOf(result)));

        return result;
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        byte[] result = delegate.getMetadataForTable(tableRef);

        runWithExceptionHandling(() -> tracker.recordTableAgnosticRead("getMetadataForTable", result.length));

        return result;
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> result = delegate.getMetadataForTables();

        runWithExceptionHandling(() -> tracker.recordTableAgnosticRead(
                "getMetadataForTables", MeasuringUtils.sizeOfMeasurableByteMap(result)));

        return result;
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp)
            throws AtlasDbDependencyException {
        Multimap<Cell, Long> result = delegate.getAllTimestamps(tableRef, cells, timestamp);

        runWithExceptionHandling(
                () -> tracker.recordReadForTable(tableRef, "getAllTimestamps", MeasuringUtils.sizeOf(result)));

        return result;
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        List<byte[]> result = delegate.getRowKeysInRange(tableRef, startRow, endRow, maxResults);

        runWithExceptionHandling(() ->
                tracker.recordReadForTable(tableRef, "getRowKeysInRange", MeasuringUtils.sizeOfByteCollection(result)));

        return result;
    }

    private static void runWithExceptionHandling(Runnable trackingRunnable) {
        try {
            trackingRunnable.run();
        } catch (Exception exception) {
            log.warn("Key value service tracking failed", exception);
        }
    }

    private <T> ClosableIterator<T> wrapIteratorWithExceptionHandling(
            TableReference tableRef, ClosableIterator<T> iterator, ToLongFunction<T> measurer) {
        ClosableIterator<T> result = iterator;
        try {
            result = new TrackingClosableIterator<>(iterator, tracker.recordCallForTable(tableRef), measurer);
        } catch (Exception exception) {
            log.warn("TrackingClosableIterator wrapping failed for tracking", exception);
        }
        return result;
    }

    private RowColumnRangeIterator wrapIteratorWithExceptionHandling(
            TableReference tableRef, RowColumnRangeIterator iterator) {
        RowColumnRangeIterator result = iterator;
        try {
            result = new TrackingRowColumnRangeIterator(
                    iterator, tracker.recordCallForTable(tableRef), MeasuringUtils::sizeOf);
        } catch (Exception exception) {
            log.warn("RowColumnRangeIterator wrapping failed for tracking", exception);
        }
        return result;
    }
}
