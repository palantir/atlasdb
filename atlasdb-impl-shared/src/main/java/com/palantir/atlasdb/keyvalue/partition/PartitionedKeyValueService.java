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
package com.palantir.atlasdb.keyvalue.partition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumTracker;
import com.palantir.atlasdb.keyvalue.partition.util.AutoRetryingClosableIterator;
import com.palantir.atlasdb.keyvalue.partition.util.ClosablePeekingIterator;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.EndpointRequestExecutor;
import com.palantir.atlasdb.keyvalue.partition.util.EndpointRequestExecutor.EndpointRequestCompletionService;
import com.palantir.atlasdb.keyvalue.partition.util.MergeResults;
import com.palantir.atlasdb.keyvalue.partition.util.PartitionedRangedIterator;
import com.palantir.atlasdb.keyvalue.partition.util.RequestCompletions;
import com.palantir.atlasdb.keyvalue.partition.util.RowResults;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.proxy.VersionCheckProxy;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * Do not let any partition map or key value service references
 * "escape" from runWithPartitionMap function!
 * Otherwise the auto-update feature on VersionTooOldException
 * will not work.
 *
 * @author htarasiuk
 *
 */
public class PartitionedKeyValueService extends PartitionMapProvider implements KeyValueService {

    private static final Logger log = LoggerFactory.getLogger(PartitionedKeyValueService.class);
    private final QuorumParameters quorumParameters;
    private final ExecutorService executor;

    // *** Read requests *************************************************************************
    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final TableReference tableRef, final Iterable<byte[]> rowsArg,
                                    final ColumnSelection columnSelection, final long timestamp) {
        // This is necessary to ensure consistent hashes for the rows arrays
        // which is necessary for the quorum tracker.
        final Set<byte[]> rows = Sets.newHashSet(rowsArg);
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DynamicPartitionMap input) {
                Map<Cell, Value> overallResult = Maps.newHashMap();
                EndpointRequestCompletionService<Map<Cell, Value>> execSvc =
                        EndpointRequestExecutor.newService(executor);
                QuorumTracker<Map<Cell, Value>, byte[]> tracker =
                        QuorumTracker.of(rows, input.getReadRowsParameters(rows));

                // Schedule tasks for execution
                input.runForRowsRead(tableRef.getQualifiedName(), rows, kvsAndRowsPair -> {
                    Future<Map<Cell, Value>> future = execSvc.submit(() ->
                            kvsAndRowsPair.lhSide.getRows(
                                    tableRef, kvsAndRowsPair.rhSide, columnSelection, timestamp),
                            kvsAndRowsPair.lhSide);
                    tracker.registerRef(future, kvsAndRowsPair.rhSide);
                    return null;
                });

                RequestCompletions.completeReadRequest(
                        tracker,
                        execSvc,
                        MergeResults.newCellValueMapMerger(overallResult));
                return overallResult;
            }
        });
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        return KeyValueServices.filterGetRowsToColumnRange(this, tableRef, rows, columnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        return KeyValueServices.mergeGetRowsColumnRangeIntoSingleIterator(this,
                                                                          tableRef,
                                                                          rows,
                                                                          columnRangeSelection,
                                                                          cellBatchHint,
                                                                          timestamp);
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final TableReference tableRef, final Map<Cell, Long> timestampByCell) {
        return runWithPartitionMapRetryable(partitionMap -> {
            EndpointRequestCompletionService<Map<Cell, Value>> execSvc =
                    EndpointRequestExecutor.newService(executor);
            QuorumTracker<Map<Cell, Value>, Cell> tracker = QuorumTracker.of(
                    timestampByCell.keySet(),
                    partitionMap.getReadCellsParameters(timestampByCell.keySet()));
            Map<Cell, Value> globalResult = Maps.newHashMap();

            // Schedule the tasks
            partitionMap.runForCellsRead(tableRef.getQualifiedName(), timestampByCell, kvsAndCellsPair -> {
                Future<Map<Cell, Value>> future = execSvc.submit(() ->
                        kvsAndCellsPair.lhSide.get(tableRef, kvsAndCellsPair.rhSide), kvsAndCellsPair.lhSide);
                tracker.registerRef(future, kvsAndCellsPair.rhSide.keySet());
                return null;
            });

            RequestCompletions.completeReadRequest(
                    tracker,
                    execSvc,
                    MergeResults.newCellValueMapMerger(globalResult));
            return globalResult;
        });
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final TableReference tableRef,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {

        return runWithPartitionMapRetryable(partitionMap -> {
            EndpointRequestCompletionService<Multimap<Cell, Long>> execSvc =
                    EndpointRequestExecutor.newService(executor);
            QuorumTracker<Multimap<Cell, Long>, Cell> tracker = QuorumTracker.of(
                    cells,
                    partitionMap.getReadCellsParameters(cells));
            final Multimap<Cell, Long> globalResult = HashMultimap.create();
            partitionMap.runForCellsRead(tableRef.getQualifiedName(), cells, kvsAndCellsPair -> {
                Future<Multimap<Cell, Long>> future = execSvc.submit(() ->
                        kvsAndCellsPair.lhSide.getAllTimestamps(tableRef, cells, timestamp), kvsAndCellsPair.lhSide);
                tracker.registerRef(future, kvsAndCellsPair.rhSide);
                return null;
            });

            RequestCompletions.completeReadRequest(
                    tracker,
                    execSvc,
                    MergeResults.newAllTimestampsMapMerger(globalResult));
            return globalResult;
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final TableReference tableRef,
                                               final Map<Cell, Long> timestampByCell) {
        return runWithPartitionMapRetryable(input -> {
            final Map<Cell, Long> globalResult = Maps.newHashMap();
            final QuorumTracker<Map<Cell, Long>, Cell> tracker = QuorumTracker.of(
                    timestampByCell.keySet(),
                    input.getReadCellsParameters(timestampByCell.keySet()));
            final EndpointRequestCompletionService<Map<Cell, Long>> execSvc =
                    EndpointRequestExecutor.newService(executor);
            input.runForCellsRead(tableRef.getQualifiedName(), timestampByCell, kvsAndCellsPair -> {
                Future<Map<Cell, Long>> future = execSvc.submit(() ->
                        kvsAndCellsPair.lhSide.getLatestTimestamps(tableRef, kvsAndCellsPair.rhSide),
                        kvsAndCellsPair.lhSide);
                tracker.registerRef(future, kvsAndCellsPair.rhSide.keySet());
                return null;
            });

            RequestCompletions.completeReadRequest(
                    tracker,
                    execSvc,
                    MergeResults.newLatestTimestampMapMerger(globalResult));
            return globalResult;
        });
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableRef, rangeRequests, timestamp);
    }

    // *** Read range requests ***
    @Override
    public ClosableIterator<RowResult<Value>> getRange(final TableReference tableRef,
            final RangeRequest rangeRequest, final long timestamp) {
        return AutoRetryingClosableIterator.of(rangeRequest, range ->
                invalidateOnVersionChangeIterator(getRangeInternal(tableRef, range, timestamp)));
    }

    private ClosableIterator<RowResult<Value>> getRangeInternal(final TableReference tableRef,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {

        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services = runWithPartitionMapRetryable(
                partitionMap -> partitionMap.getServicesForRangeRead(tableRef.getQualifiedName(), rangeRequest));

        return new PartitionedRangedIterator<Value>(services.keySet()) {
            @Override
            public RowResult<Value> computeNext() {
                Preconditions.checkState(hasNext());
                // This is NOT retryable
                return runWithPartitionMap(new Function<DynamicPartitionMap, RowResult<Value>>() {
                    @Override
                    public RowResult<Value> apply(DynamicPartitionMap input) {
                        return RowResults.mergeResults(getRowIterator(), quorumParameters.getReadRequestParameters());
                    }
                });
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Value>>> computeNextRange(
                    ConsistentRingRangeRequest range) {
                // This is NOT retryable
                return runWithPartitionMap(partitionMap -> {
                    Set<ClosablePeekingIterator<RowResult<Value>>> result = Sets.newHashSet();

                    // We need at least one iterator for each range in order for the
                    // quorum exception mechanism to work properly. If all iterator
                    // requests failed we need to throw immediately to avoid silent failure.
                    RuntimeException lastSuppressedException = null;

                    for (KeyValueEndpoint vkve : services.get(range)) {
                        try {
                            ClosableIterator<RowResult<Value>> it = vkve.keyValueService()
                                    .getRange(tableRef, range.get(), timestamp);
                            result.add(ClosablePeekingIterator.of(it));
                        } catch (ClientVersionTooOldException e) {
                            throw e;
                        } catch (RuntimeException e) {
                            // If this failure is fatal for the range, the exception will be thrown when
                            // retrieving data from the iterators.
                            log.warn("Failed to getRange in table " + tableRef.getQualifiedName());

                            // The only exception is if all iterators for given range fail.
                            // In such case it will rethrow last encountered exception immediately.
                            lastSuppressedException = e;
                        }
                    }

                    // TODO: Do NOT throw it now. Perhaps this subrange will never
                    // be reached by the client. (?)
                    if (result.isEmpty()) {
                        throw lastSuppressedException;
                    }
                    return result;
                });
            }
        };
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
            final TableReference tableRef, RangeRequest rangeRequest, final long timestamp) {
        return AutoRetryingClosableIterator.of(rangeRequest, range ->
                invalidateOnVersionChangeIterator(getRangeWithHistoryInternal(tableRef, range, timestamp)));
    }

    private ClosableIterator<RowResult<Set<Value>>> getRangeWithHistoryInternal(final TableReference tableRef,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
                runWithPartitionMapRetryable(partitionMap ->
                        partitionMap.getServicesForRangeRead(tableRef.getQualifiedName(), rangeRequest));

        return new PartitionedRangedIterator<Set<Value>>(services.keySet()) {
            @Override
            public RowResult<Set<Value>> computeNext() {
                Preconditions.checkState(hasNext());
                // This is NOT retryable
                return runWithPartitionMap(partitionMap -> RowResults.allResults(getRowIterator()));
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Value>>>> computeNextRange(
                    ConsistentRingRangeRequest range) {
                // This is NOT retryable
                return runWithPartitionMap(partitionMap -> {
                    Set<ClosablePeekingIterator<RowResult<Set<Value>>>> result = Sets.newHashSet();

                    // This method has stronger consistency guarantees. It has to talk to all endpoints
                    // and thus must throw immediately on any failure encountered.
                    for (KeyValueEndpoint vkve : services.get(range)) {
                        ClosableIterator<RowResult<Set<Value>>> it = vkve.keyValueService().getRangeWithHistory(
                                tableRef, range.get(), timestamp);
                        result.add(ClosablePeekingIterator.of(it));
                    }
                    return result;
                });
            }
        };
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp)
            throws InsufficientConsistencyException {
        return AutoRetryingClosableIterator.of(rangeRequest, range ->
                invalidateOnVersionChangeIterator(getRangeOfTimestampsInternal(tableRef, range, timestamp)));
    }

    private ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestampsInternal(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp)
            throws InsufficientConsistencyException {
        Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services = runWithPartitionMapRetryable(range ->
                range.getServicesForRangeRead(tableRef.getQualifiedName(), rangeRequest));

        return new PartitionedRangedIterator<Set<Long>>(services.keySet()) {
            @Override
            public RowResult<Set<Long>> computeNext() {
                Preconditions.checkState(hasNext());
                // This is NOT retryable
                return runWithPartitionMap(input -> RowResults.allTimestamps(getRowIterator()));
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Long>>>> computeNextRange(
                    ConsistentRingRangeRequest range) {
                // This is NOT retryable
                return runWithPartitionMap(partitionMap -> {
                    final Set<ClosablePeekingIterator<RowResult<Set<Long>>>> result = Sets.newHashSet();

                    // This method has stronger consistency guarantees. It has to talk to all endpoints
                    // and thus must throw immediately on any failure encountered.
                    for (KeyValueEndpoint vkve : services.get(range)) {
                        ClosableIterator<RowResult<Set<Long>>> it = vkve.keyValueService().getRangeOfTimestamps(
                                tableRef, range.get(), timestamp);
                        result.add(ClosablePeekingIterator.of(it));
                    }

                    return result;
                });
            }
        };
    }

    // *** Write requests *************************************************************************
    @Override
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp) {
        runWithPartitionMap(partitionMap -> {
            EndpointRequestCompletionService<Void> writeService = EndpointRequestExecutor.newService(executor);
            final QuorumTracker<Void, Cell> tracker =
                    QuorumTracker.of(values.keySet(), partitionMap.getWriteCellsParameters(values.keySet()));

            partitionMap.runForCellsWrite(tableRef.getQualifiedName(), values, kvsAndCellsPair -> {
                Future<Void> future = writeService.submit(() -> {
                    kvsAndCellsPair.lhSide.put(tableRef, kvsAndCellsPair.rhSide, timestamp);
                    return null;
                }, kvsAndCellsPair.lhSide);
                tracker.registerRef(future, kvsAndCellsPair.rhSide.keySet());
                return null;
            });

            RequestCompletions.completeWriteRequest(tracker, writeService);
            return null;
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final TableReference tableRef, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {

        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(final DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Map.Entry<Cell, Value>> tracker = QuorumTracker.of(
                        cellValues.entries(), input.getWriteEntriesParameters(cellValues));
                final Semaphore semaphore = new Semaphore(EndpointRequestExecutor.MAX_TASKS_PER_ENDPOINT);

                input.runForCellsWrite(tableRef.getQualifiedName(), cellValues, kvsAndCellsPair -> {
                    Future<Void> future = execSvc.submit(() -> {
                        semaphore.acquire();
                        try {
                            kvsAndCellsPair.lhSide.putWithTimestamps(tableRef, kvsAndCellsPair.rhSide);
                        } finally {
                            semaphore.release();
                        }
                        return null;
                    }, kvsAndCellsPair.lhSide);
                    tracker.registerRef(future, kvsAndCellsPair.rhSide.entries());
                    return null;
                });

                RequestCompletions.completeWriteRequest(tracker, execSvc);
                try {
                    semaphore.acquire(EndpointRequestExecutor.MAX_TASKS_PER_ENDPOINT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }
        });
    }

    /**
     * This operation is not supported for <code>PartitionedKeyValueService</code>.
     *
     * @deprecated This method is not supported. Please do not use.
     */
    @Override
    @Deprecated
    public void putUnlessExists(TableReference tableRef, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        // TODO: This should eventually throw new UnsupportedOperationException().
        // For some testing purposes it does not do it now and calls putUnlessExists
        // on all relevant delegates which is NOT a correct solution!
        runWithPartitionMap(partitionMap -> {
            EndpointRequestCompletionService<Void> writeService = EndpointRequestExecutor.newService(executor);
            QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                    values.keySet(), quorumParameters.getWriteRequestParameters());

            partitionMap.runForCellsWrite(tableRef.getQualifiedName(), values, kvsAndCellsPair -> {
                Future<Void> future = writeService.submit(() -> {
                    kvsAndCellsPair.lhSide.putUnlessExists(tableRef, kvsAndCellsPair.rhSide);
                    return null;
                }, kvsAndCellsPair.lhSide);
                tracker.registerRef(future, kvsAndCellsPair.rhSide.keySet());
                return null;
            });

            RequestCompletions.completeWriteRequest(tracker, writeService);
            return null;
        });
    }

    @Override
    @Idempotent
    public void delete(final TableReference tableRef, final Multimap<Cell, Long> keys) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Map.Entry<Cell, Long>> tracker = QuorumTracker.of(
                        keys.entries(), input.getWriteEntriesParameters(keys));

                input.runForCellsWrite(tableRef.getQualifiedName(), keys, kvsAndCellsPair -> {
                    final Future<Void> future = execSvc.submit(() -> {
                        kvsAndCellsPair.lhSide.delete(tableRef, kvsAndCellsPair.rhSide);
                        return null;
                    }, kvsAndCellsPair.lhSide);
                    tracker.registerRef(future, kvsAndCellsPair.rhSide.entries());
                    return null;
                });

                RequestCompletions.completeWriteRequest(tracker, execSvc);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final TableReference tableRef, final Set<Cell> cells) {
        runWithPartitionMapRetryable(partitionMap -> {
            EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
            QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                    cells, partitionMap.getWriteCellsParameters(cells));

            partitionMap.runForCellsWrite(tableRef.getQualifiedName(), cells, kvsAndCellsPair -> {
                Future<Void> future = execSvc.submit(() -> {
                    kvsAndCellsPair.lhSide.addGarbageCollectionSentinelValues(tableRef, kvsAndCellsPair.rhSide);
                    return null;
                }, kvsAndCellsPair.lhSide);
                tracker.registerRef(future, kvsAndCellsPair.rhSide);
                return null;
            });

            RequestCompletions.completeWriteRequest(tracker, execSvc);
            return null;
        });
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final TableReference tableName = e.getKey();
            final Map<Cell, byte[]> values = e.getValue();
            put(tableName, values, timestamp);
        }
    }

    // *** Table stuff
    // ***********************************************************************************
    @Override
    @Idempotent
    public void dropTable(final TableReference tableRef) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.dropTable(tableRef);
                }
                return null;
            }
        });
    }

    @Override
    public void dropTables(final Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.dropTables(tableRefs);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final TableReference tableRef, final byte[] tableMetadata)
            throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.createTable(tableRef, tableMetadata);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTable(final TableReference tableRef) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.truncateTable(tableRef);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTables(final Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.truncateTables(tableRefs);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTables(final Map<TableReference, byte[]> tableRefToTableMetadata)
            throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.createTables(tableRefToTableMetadata);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<TableReference> getAllTableNames() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Set<TableReference>>() {
            @Override
            public Set<TableReference> apply(@Nullable DynamicPartitionMap input) {
                return RequestCompletions.retryUntilSuccess(
                        input.getDelegates().iterator(),
                        new Function<KeyValueService, Set<TableReference>>() {
                            @Override
                            @Nullable
                            public Set<TableReference> apply(@Nullable KeyValueService kvs) {
                                return kvs.getAllTableNames();
                            }
                        });
                    }
        });
    }

    // *** Metadata
    // ****************************************************************************************
    @Override
    @Idempotent
    public byte[] getMetadataForTable(final TableReference tableRef) {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, byte[]>() {
            @Override
            public byte[] apply(@Nullable DynamicPartitionMap input) {
                return RequestCompletions.retryUntilSuccess(
                        input.getDelegates().iterator(),
                        new Function<KeyValueService, byte[]>() {
                            @Override
                            public byte[] apply(@Nullable KeyValueService kvs) {
                                return kvs.getMetadataForTable(tableRef);
                            }
                        });
            }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final TableReference tableRef, final byte[] metadata) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.putMetadataForTable(tableRef, metadata);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Map<TableReference, byte[]> getMetadataForTables() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Map<TableReference, byte[]>>() {
            @Override
            public Map<TableReference, byte[]> apply(@Nullable DynamicPartitionMap input) {
                return RequestCompletions.retryUntilSuccess(
                        input.getDelegates().iterator(),
                        new Function<KeyValueService, Map<TableReference, byte[]>>() {
                            @Override
                            public Map<TableReference, byte[]> apply(KeyValueService kvs) {
                                return kvs.getMetadataForTables();
                            }
                        });
                    }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.putMetadataForTables(tableRefToMetadata);
                }
                return null;
            }
        });
    }

    // *** Simple forward methods ***************************************************************
    @Override
    public void compactInternally(final TableReference tableRef) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.compactInternally(tableRef);
                }
                return null;
            }
        });
    }

    @Override
    public void close() {
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.close();
                }
                return null;
            }
        });
    }

    @Override
    public void teardown() {
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.teardown();
                }
                return null;
            }
        });
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Collection<? extends KeyValueService>>() {
            @Override
            public Collection<? extends KeyValueService> apply(
                    DynamicPartitionMap input) {
                return input.getDelegates();
            }
        });
    }

    @Override
    public void initializeFromFreshInstance() {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.initializeFromFreshInstance();
                }
                return null;
            }
        });
    }

    // *** Creation *******************************************************************************
    protected PartitionedKeyValueService(ExecutorService executor, QuorumParameters quorumParameters,
            ImmutableList<PartitionMapService> partitionMapProviders, int partitionMapProvidersReadFactor) {
        super(partitionMapProviders, partitionMapProvidersReadFactor);
        this.executor = executor;
        this.quorumParameters = quorumParameters;
    }

    public static PartitionedKeyValueService create(
            QuorumParameters quorumParameters,
            List<PartitionMapService> mapServices) {
        ExecutorService executor = PTExecutors.newCachedThreadPool();
        return new PartitionedKeyValueService(executor, quorumParameters, ImmutableList.copyOf(mapServices), 1);
    }

    public static PartitionedKeyValueService create(PartitionedKeyValueConfiguration config) {
        Builder<PartitionMapService> builder = ImmutableList.builder();
        for (String provider : config.getPartitionMapProviders()) {
            builder.add(RemotingPartitionMapService.createClientSide(provider));
        }
        ExecutorService executor = PTExecutors.newCachedThreadPool();
        return new PartitionedKeyValueService(executor, config.getQuorumParameters(),
                builder.build(), config.getPartitionMapProvidersReadFactor());
    }

    // *** Helper methods *************************************************************************

    /**
     * @deprecated Only for testing. Do not use.
     */
    @Deprecated
    public DynamicPartitionMapImpl getPartitionMap() {
        return runWithPartitionMapRetryable(partitionMap -> (DynamicPartitionMapImpl) partitionMap);
    }

    private long getMapVersion() {
        return runWithPartitionMapRetryable(DynamicPartitionMap::getVersion);
    }

    private <T> ClosableIterator<RowResult<T>> invalidateOnVersionChangeIterator(ClosableIterator<RowResult<T>> it) {
        return VersionCheckProxy.invalidateOnVersionChangeProxy(it, this::getMapVersion);
    }
}
