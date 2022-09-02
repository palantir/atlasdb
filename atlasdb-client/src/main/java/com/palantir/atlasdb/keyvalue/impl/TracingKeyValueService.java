/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.tracing.FunctionalTagTranslator;
import com.palantir.atlasdb.tracing.TagConsumer;
import com.palantir.atlasdb.tracing.TraceStatistic;
import com.palantir.atlasdb.tracing.TraceStatistics;
import com.palantir.atlasdb.tracing.Tracing;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.Tracer;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Wraps a {@link KeyValueService}'s methods with {@link com.palantir.tracing.Tracer}
 * instrumentation.
 */
public final class TracingKeyValueService extends ForwardingObject implements KeyValueService {

    private static final String SERVICE_NAME = "atlasdb-kvs";

    private final KeyValueService delegate;
    private final ExecutorService tracingExecutorService = PTExecutors.newSingleThreadScheduledExecutor();

    private TracingKeyValueService(KeyValueService delegate) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    }

    public static KeyValueService create(KeyValueService keyValueService) {
        return new TracingKeyValueService(keyValueService);
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.addGarbageCollectionSentinelValues", sink -> {
            sink.tableRef(tableRef);
            sink.size("cells", cells);
        })) {
            delegate().addGarbageCollectionSentinelValues(tableRef, cells);
        }
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.checkAndSet", checkAndSetRequest.table())) {
            delegate().checkAndSet(checkAndSetRequest);
        }
    }

    @Override
    public void multiCheckAndSet(MultiCheckAndSetRequest multiCheckAndSetRequest) throws MultiCheckAndSetException {
        try (CloseableTracer trace =
                startLocalTrace("atlasdb-kvs.multiCheckAndSet", multiCheckAndSetRequest.tableRef())) {
            delegate().multiCheckAndSet(multiCheckAndSetRequest);
        }
    }

    @Override
    public void close() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.close")) {
            delegate().close();
        }
        tracingExecutorService.shutdown();
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.compactInternally", tableRef)) {
            delegate().compactInternally(tableRef);
        }
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getClusterAvailabilityStatus")) {
            return delegate().getClusterAvailabilityStatus();
        }
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.compactInternally", tableRef)) {
            delegate().compactInternally(tableRef, inMaintenanceMode);
        }
    }

    @Override
    public boolean isInitialized() {
        return delegate().isInitialized();
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.createTable", tableRef)) {
            delegate().createTable(tableRef, tableMetadata);
        }
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableNamesToTableMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.createTables", tableNamesToTableMetadata.keySet())) {
            delegate().createTables(tableNamesToTableMetadata);
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.delete", sink -> {
            sink.tableRef(tableRef);
            sink.size("keys", keys);
        })) {
            delegate().delete(tableRef, keys);
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.deleteRange", tableRef)) {
            delegate().deleteRange(tableRef, range);
        }
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.deleteRows", tableRef)) {
            delegate().deleteRows(tableRef, rows);
        }
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.deleteAllTimestamps", tableRef)) {
            delegate().deleteAllTimestamps(tableRef, deletes);
        }
    }

    @Override
    public void dropTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.dropTable", tableRef)) {
            delegate().dropTable(tableRef);
        }
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.dropTables", tableRefs)) {
            delegate().dropTables(tableRefs);
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.get", sink -> {
            sink.tableRef(tableRef);
            sink.size("cells", timestampByCell);
        })) {
            return delegate().get(tableRef, timestampByCell);
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getAllTableNames")) {
            return delegate().getAllTableNames();
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> keys, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getAllTimestamps", sink -> {
            sink.tableRef(tableRef);
            sink.size("keys", keys);
            sink.timestamp(timestamp);
        })) {
            return delegate().getAllTimestamps(tableRef, keys, timestamp);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getDelegates")) {
            return ImmutableList.copyOf(
                    Iterables.concat(ImmutableList.of(delegate()), delegate().getDelegates()));
        }
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getFirstBatchForRanges", sink -> {
            sink.tableRef(tableRef);
            sink.size("ranges", rangeRequests);
            sink.timestamp(timestamp);
        })) {
            return delegate().getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getLatestTimestamps", sink -> {
            sink.tableRef(tableRef);
            sink.size("cells", timestampByCell);
        })) {
            return delegate().getLatestTimestamps(tableRef, timestampByCell);
        }
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getMetadataForTable", tableRef)) {
            return delegate().getMetadataForTable(tableRef);
        }
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getMetadataForTables")) {
            return delegate().getMetadataForTables();
        }
    }

    @Override
    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        DetachedSpan detachedSpan = DetachedSpan.start("atlasdb-kvs.getRange");

        ClosableIterator<RowResult<Value>> result = delegate().getRange(tableRef, rangeRequest, timestamp);

        // Only instrument observable traces
        if (Tracer.isTraceObservable()) {
            LongAdder rowCount = new LongAdder();

            result = result.map(value -> {
                rowCount.increment();
                return value;
            });
            result = attachDetachedSpanCompletion(detachedSpan, result, sink -> {
                sink.tableRef(tableRef);
                sink.longValue("rows", rowCount.sum());
            });
        }

        return result;
    }

    @Override
    @MustBeClosed
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        DetachedSpan detachedSpan = DetachedSpan.start("atlasdb-kvs.getRangeOfTimestamps");

        @SuppressWarnings("MustBeClosedChecker")
        ClosableIterator<RowResult<Set<Long>>> result =
                delegate().getRangeOfTimestamps(tableRef, rangeRequest, timestamp);

        return attachDetachedSpanCompletion(detachedSpan, result, sink -> sink.tableRef(tableRef));
    }

    @Override
    @MustBeClosed
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        DetachedSpan detachedSpan = DetachedSpan.start("atlasdb-kvs.getCandidateCellsForSweeping");

        @SuppressWarnings("MustBeClosedChecker")
        ClosableIterator<List<CandidateCellForSweeping>> result =
                delegate().getCandidateCellsForSweeping(tableRef, request);

        return attachDetachedSpanCompletion(detachedSpan, result, sink -> sink.tableRef(tableRef));
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getRows", sink -> {
            sink.tableRef(tableRef);
            sink.size("rows", rows);
            sink.timestamp(timestamp);
        })) {
            return delegate().getRows(tableRef, rows, columnSelection, timestamp);
        }
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        TraceStatistic original = TraceStatistics.getCurrentAndClear();

        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getRowsColumnRange", sink -> {
            sink.statistics(TraceStatistics.getCopyAndRestoreOriginal(original));
            sink.tableRef(tableRef);
            sink.size("rows", rows);
            sink.timestamp(timestamp);
        })) {
            return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, timestamp);
        }
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        // No tracing, as we just return a lazy iterator and don't perform any calls to the backing KVS.
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.multiPut", sink -> {
            sink.size("values", valuesByTable);
            sink.timestamp(timestamp);
        })) {
            delegate().multiPut(valuesByTable, timestamp);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.put", sink -> {
            sink.tableRef(tableRef);
            sink.size("values", values);
            sink.timestamp(timestamp);
        })) {
            delegate().put(tableRef, values, timestamp);
        }
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.putMetadataForTable", sink -> {
            sink.tableRef(tableRef);
            int size = (metadata == null) ? 0 : metadata.length;
            sink.integer("bytes", size);
        })) {
            delegate().putMetadataForTable(tableRef, metadata);
        }
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.putMetadataForTables", tableRefToMetadata.keySet())) {
            delegate().putMetadataForTables(tableRefToMetadata);
        }
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.putUnlessExists", sink -> {
            sink.tableRef(tableRef);
            sink.size("values", values);
        })) {
            delegate().putUnlessExists(tableRef, values);
        }
    }

    @Override
    public void setOnce(TableReference tableRef, Map<Cell, byte[]> values) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.setOnce", sink -> {
            sink.tableRef(tableRef);
            sink.size("values", values);
        })) {
            delegate().setOnce(tableRef, values);
        }
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return delegate().getCheckAndSetCompatibility();
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.putWithTimestamps", sink -> {
            sink.tableRef(tableRef);
            sink.size("values", values);
        })) {
            delegate().putWithTimestamps(tableRef, values);
        }
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.truncateTable", tableRef)) {
            delegate().truncateTable(tableRef);
        }
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.truncateTables", tableRefs)) {
            delegate().truncateTables(tableRefs);
        }
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate().shouldTriggerCompactions();
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        try (CloseableTracer trace = startLocalTrace("atlasdb-kvs.getRowKeysInRange", sink -> {
            sink.tableRef(tableRef);
            sink.integer("maxResults", maxResults);
        })) {
            return delegate().getRowKeysInRange(tableRef, startRow, endRow, maxResults);
        }
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        // We'll be completing in a different unrelated thread, so we have no chance to restore the original; this is a
        // known limitation of the async trace statistics. We still want to clear the current statistics.
        TraceStatistics.getCurrentAndClear();

        // As the trace executor will close out this span, we need to keep a reference to the current mutable statistics
        TraceStatistic current = TraceStatistics.getReferenceToCurrent();

        DetachedSpan detachedSpan = DetachedSpan.start("atlasdb-kvs.getAsync");

        // Note that the trace statistics rely on the executor being compatible with
        // {@link ExecutorInheritableThreadLocal} to function correctly
        ListenableFuture<Map<Cell, Value>> future = delegate().getAsync(tableRef, timestampByCell);

        return attachDetachedSpanCompletion(detachedSpan, future, tracingExecutorService, sink -> {
            sink.statistics(current);
            sink.tableRef(tableRef);
            sink.size("cells", timestampByCell);
        });
    }

    @MustBeClosed
    private static CloseableTracer startLocalTrace(@CompileTimeConstant final String operation) {
        TraceStatistic original = TraceStatistics.getCurrentAndClear();

        return Tracing.startLocalTrace(
                operation, sink -> sink.statistics(TraceStatistics.getCopyAndRestoreOriginal(original)));
    }

    @MustBeClosed
    private static CloseableTracer startLocalTrace(
            @CompileTimeConstant final String operation, TableReference tableReference) {
        TraceStatistic original = TraceStatistics.getCurrentAndClear();

        return Tracing.startLocalTrace(operation, sink -> {
            sink.statistics(TraceStatistics.getCopyAndRestoreOriginal(original));
            sink.tableRef(tableReference);
        });
    }

    @MustBeClosed
    private static CloseableTracer startLocalTrace(
            @CompileTimeConstant final String operation, Consumer<TagConsumer> tagTranslator) {
        TraceStatistic original = TraceStatistics.getCurrentAndClear();

        return Tracing.startLocalTrace(operation, sink -> {
            sink.statistics(TraceStatistics.getCopyAndRestoreOriginal(original));
            tagTranslator.accept(sink);
        });
    }

    @MustBeClosed
    private static CloseableTracer startLocalTrace(
            @CompileTimeConstant final String operation, Collection<TableReference> tableReferences) {
        TraceStatistic original = TraceStatistics.getCurrentAndClear();

        return Tracing.startLocalTrace(operation, sink -> {
            sink.statistics(TraceStatistics.getCopyAndRestoreOriginal(original));
            sink.tableRefs(tableReferences);
        });
    }

    /**
     * Attach a detached span to a future completing.
     */
    private static <V> ListenableFuture<V> attachDetachedSpanCompletion(
            DetachedSpan detachedSpan,
            ListenableFuture<V> future,
            Executor tracingExecutorService,
            Consumer<TagConsumer> tagTranslator) {
        if (!Tracer.isTraceObservable()) {
            return future;
        }

        Futures.addCallback(
                future,
                new FutureCallback<V>() {
                    @Override
                    public void onSuccess(V result) {
                        complete();
                    }

                    @Override
                    public void onFailure(Throwable _throwable) {
                        complete();
                    }

                    private void complete() {
                        detachedSpan.complete(FunctionalTagTranslator.INSTANCE, tagTranslator);
                    }
                },
                tracingExecutorService);
        return future;
    }

    /**
     * Attach a detached span to the close of a closable iterator.
     *
     * Note: due to legacy code this span is not guaranteed to close. This will mean that some logs will be lost/missed.
     */
    @MustBeClosed
    private static <V> ClosableIterator<V> attachDetachedSpanCompletion(
            DetachedSpan detachedSpan, ClosableIterator<V> closableIterator, Consumer<TagConsumer> tagTranslator) {
        if (!Tracer.isTraceObservable()) {
            return closableIterator;
        }

        // We'll possibly be completing in a different unrelated thread, so we have no chance to restore the original;
        // this is a known limitation of the async trace statistics. We still want to clear the current statistics.
        TraceStatistic original = TraceStatistics.getCurrentAndClear();
        Thread originalThread = Thread.currentThread();

        // As the trace executor will close out this span, we need to keep a reference to the current mutable statistics
        TraceStatistic current = TraceStatistics.getReferenceToCurrent();

        return ClosableIterators.appendOnClose(closableIterator, () -> {
            detachedSpan.complete(FunctionalTagTranslator.INSTANCE, sink -> {
                sink.statistics(current);
                tagTranslator.accept(sink);

                if (Thread.currentThread() == originalThread) {
                    // Lucky, we can clean up the threadlocals
                    TraceStatistics.getCopyAndRestoreOriginal(original);
                }
            });
        });
    }
}
