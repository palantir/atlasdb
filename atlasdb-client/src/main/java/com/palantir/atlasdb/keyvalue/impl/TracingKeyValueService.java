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
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.DetachedSpan;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

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
        try (CloseableTrace trace = startLocalTrace(
                "addGarbageCollectionSentinelValues({}, {} cells)",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                Iterables.size(cells))) {
            delegate().addGarbageCollectionSentinelValues(tableRef, cells);
        }
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("checkAndSet({})", LoggingArgs.safeTableOrPlaceholder(checkAndSetRequest.table()))) {
            delegate().checkAndSet(checkAndSetRequest);
        }
    }

    @Override
    public void close() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("close()")) {
            delegate().close();
        }
        tracingExecutorService.shutdown();
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("compactInternally({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().compactInternally(tableRef);
        }
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getClusterAvailabilityStatus()")) {
            return delegate().getClusterAvailabilityStatus();
        }
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("compactInternally({})", tableRef)) {
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
        try (CloseableTrace trace = startLocalTrace("createTable({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().createTable(tableRef, tableMetadata);
        }
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableNamesToTableMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "createTables({})", LoggingArgs.safeTablesOrPlaceholder(tableNamesToTableMetadata.keySet()))) {
            delegate().createTables(tableNamesToTableMetadata);
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("delete({}, {} keys)", LoggingArgs.safeTableOrPlaceholder(tableRef), keys.size())) {
            delegate().delete(tableRef, keys);
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("deleteRange({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().deleteRange(tableRef, range);
        }
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("deleteRows({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().deleteRows(tableRef, rows);
        }
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("deleteAllTimestamps({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().deleteAllTimestamps(tableRef, deletes);
        }
    }

    @Override
    public void dropTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("dropTable({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().dropTable(tableRef);
        }
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("dropTables({})", LoggingArgs.safeTablesOrPlaceholder(tableRefs))) {
            delegate().dropTables(tableRefs);
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "get({}, {} cells)", LoggingArgs.safeTableOrPlaceholder(tableRef), timestampByCell.size())) {
            return delegate().get(tableRef, timestampByCell);
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getAllTableNames()")) {
            return delegate().getAllTableNames();
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> keys, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "getAllTimestamps({}, {} keys, ts {})",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                keys.size(),
                timestamp)) {
            return delegate().getAllTimestamps(tableRef, keys, timestamp);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getDelegates()")) {
            return ImmutableList.copyOf(
                    Iterables.concat(ImmutableList.of(delegate()), delegate().getDelegates()));
        }
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "getFirstBatchForRanges({}, {} ranges, ts {})",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                Iterables.size(rangeRequests),
                timestamp)) {
            return delegate().getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "getLatestTimestamps({}, {} cells)",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                timestampByCell.size())) {
            return delegate().getLatestTimestamps(tableRef, timestampByCell);
        }
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("getMetadataForTable({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            return delegate().getMetadataForTable(tableRef);
        }
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getMetadataForTables()")) {
            return delegate().getMetadataForTables();
        }
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        // No tracing, as we just return a lazy iterator and don't perform any calls to the backing KVS.
        return delegate().getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        // No tracing, as we just return a lazy iterator and don't perform any calls to the backing KVS.
        return delegate().getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        // No tracing, as we just return a lazy iterator and don't perform any calls to the backing KVS.
        return delegate().getCandidateCellsForSweeping(tableRef, request);
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "getRows({}, {} rows, ts {})",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                Iterables.size(rows),
                timestamp)) {
            return delegate().getRows(tableRef, rows, columnSelection, timestamp);
        }
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "getRowsColumnRange({}, {} rows, ts {})",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                Iterables.size(rows),
                timestamp)) {
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
        try (CloseableTrace trace = startLocalTrace("multiPut({} values, ts {})", valuesByTable.size(), timestamp)) {
            delegate().multiPut(valuesByTable, timestamp);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "put({}, {} values, ts {})", LoggingArgs.safeTableOrPlaceholder(tableRef), values.size(), timestamp)) {
            delegate().put(tableRef, values, timestamp);
        }
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "putMetadataForTable({}, {} bytes)",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                (metadata == null) ? 0 : metadata.length)) {
            delegate().putMetadataForTable(tableRef, metadata);
        }
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "putMetadataForTables({})", LoggingArgs.safeTablesOrPlaceholder(tableRefToMetadata.keySet()))) {
            delegate().putMetadataForTables(tableRefToMetadata);
        }
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "putUnlessExists({}, {} values)", LoggingArgs.safeTableOrPlaceholder(tableRef), values.size())) {
            delegate().putUnlessExists(tableRef, values);
        }
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return delegate().getCheckAndSetCompatibility();
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "putWithTimestamps({}, {} values)", LoggingArgs.safeTableOrPlaceholder(tableRef), values.size())) {
            delegate().putWithTimestamps(tableRef, values);
        }
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("truncateTable({})", LoggingArgs.safeTableOrPlaceholder(tableRef))) {
            delegate().truncateTable(tableRef);
        }
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace =
                startLocalTrace("truncateTables({})", LoggingArgs.safeTablesOrPlaceholder(tableRefs))) {
            delegate().truncateTables(tableRefs);
        }
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate().shouldTriggerCompactions();
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        try (CloseableTrace trace = startLocalTrace(
                "getRowKeysInRange({}, {})", LoggingArgs.safeTableOrPlaceholder(tableRef), maxResults)) {
            return delegate().getRowKeysInRange(tableRef, startRow, endRow, maxResults);
        }
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        DetachedSpan detachedSpan = DetachedSpan.start(String.format(
                "getAsync(%s, %s cells)", LoggingArgs.safeTableOrPlaceholder(tableRef), timestampByCell.size()));

        ListenableFuture<Map<Cell, Value>> future = delegate().getAsync(tableRef, timestampByCell);
        return attachDetachedSpanCompletion(detachedSpan, future, tracingExecutorService);
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        return CloseableTrace.startLocalTrace(SERVICE_NAME, operationFormat, formatArguments);
    }

    private static <V> ListenableFuture<V> attachDetachedSpanCompletion(
            DetachedSpan detachedSpan, ListenableFuture<V> future, Executor tracingExecutorService) {
        Futures.addCallback(
                future,
                new FutureCallback<V>() {
                    @Override
                    public void onSuccess(V result) {
                        detachedSpan.complete();
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        detachedSpan.complete();
                    }
                },
                tracingExecutorService);
        return future;
    }
}
