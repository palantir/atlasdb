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

import static com.palantir.atlasdb.keyvalue.partition.util.RequestCompletions.completeReadRequest;
import static com.palantir.atlasdb.keyvalue.partition.util.RequestCompletions.completeWriteRequest;
import static com.palantir.atlasdb.keyvalue.partition.util.RequestCompletions.retryUntilSuccess;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
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
import com.palantir.atlasdb.keyvalue.partition.util.RowResults;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.proxy.VersionCheckProxy;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Pair;
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
    public Map<Cell, Value> getRows(final String tableName, final Iterable<byte[]> rowsArg,
                                    final ColumnSelection columnSelection, final long timestamp) {
        // This is necessary to ensure consistent hashes for the rows arrays
        // which is necessary for the quorum tracker.
        final Set<byte[]> rows = Sets.newHashSet(rowsArg);
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DynamicPartitionMap input) {
                final Map<Cell, Value> overallResult = Maps.newHashMap();
                final EndpointRequestCompletionService<Map<Cell, Value>> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Map<Cell, Value>, byte[]> tracker = QuorumTracker.of(
                        rows, input.getReadRowsParameters(rows));

                // Schedule tasks for execution
                input.runForRowsRead(tableName, rows, new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Iterable<byte[]>> e) {
                        Future<Map<Cell, Value>> future = execSvc.submit(new Callable<Map<Cell, Value>>() {
                            @Override
                            public Map<Cell, Value> call() throws Exception {
                                return e.lhSide.getRows(tableName, e.rhSide, columnSelection, timestamp);
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide);
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResults.newCellValueMapMerger(overallResult));
                return overallResult;
            }
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, final Map<Cell, Long> timestampByCell) {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(@Nullable DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Map<Cell, Value>> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Map<Cell, Value>, Cell> tracker = QuorumTracker.of(
                        timestampByCell.keySet(),
                        input.getReadCellsParameters(timestampByCell.keySet()));
                final Map<Cell, Value> globalResult = Maps.newHashMap();

                // Schedule the tasks
                input.runForCellsRead(tableName, timestampByCell, new Function<Pair<KeyValueService, Map<Cell, Long>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Map<Cell, Long>> e) {
                        Future<Map<Cell, Value>> future = execSvc.submit(new Callable<Map<Cell, Value>>() {
                            @Override
                            public Map<Cell, Value> call() throws Exception {
                                return e.lhSide.get(tableName, e.rhSide);
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide.keySet());
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResults.newCellValueMapMerger(globalResult));
                return globalResult;
            }
        });
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {

        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Multimap<Cell, Long>>() {
            @Override
            public Multimap<Cell, Long> apply(DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Multimap<Cell, Long>> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Multimap<Cell, Long>, Cell> tracker = QuorumTracker.of(
                        cells,
                        input.getReadCellsParameters(cells));
                final Multimap<Cell, Long> globalResult = HashMultimap.create();
                input.runForCellsRead(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
                    @Override @Nullable
                    public Void apply(@Nullable final Pair<KeyValueService, Set<Cell>> e) {
                        Future<Multimap<Cell, Long>> future = execSvc.submit(new Callable<Multimap<Cell, Long>>() {
                            @Override
                            public Multimap<Cell, Long> call() throws Exception {
                                return e.lhSide.getAllTimestamps(tableName, cells, timestamp);
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide);
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResults.newAllTimestampsMapMerger(globalResult));
                return globalResult;
            }
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final String tableName,
                                               final Map<Cell, Long> timestampByCell) {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Map<Cell, Long>>() {
            @Override
            public Map<Cell, Long> apply(DynamicPartitionMap input) {
                final Map<Cell, Long> globalResult = Maps.newHashMap();
                final QuorumTracker<Map<Cell, Long>, Cell> tracker = QuorumTracker.of(
                        timestampByCell.keySet(),
                        input.getReadCellsParameters(timestampByCell.keySet()));
                final EndpointRequestCompletionService<Map<Cell, Long>> execSvc = EndpointRequestExecutor.newService(executor);
                input.runForCellsRead(tableName, timestampByCell, new Function<Pair<KeyValueService, Map<Cell, Long>>, Void>() {
                    @Override @Nullable
                    public Void apply(@Nullable final Pair<KeyValueService, Map<Cell, Long>> e) {
                        Future<Map<Cell, Long>> future = execSvc.submit(new Callable<Map<Cell, Long>>() {
                            @Override
                            public Map<Cell, Long> call() throws Exception {
                                return e.lhSide.getLatestTimestamps(tableName, e.rhSide);
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide.keySet());
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResults.newLatestTimestampMapMerger(globalResult));
                return globalResult;
            }
        });
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
    }

    // *** Read range requests ***
    @Override
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
            final RangeRequest rangeRequest, final long timestamp) {
        return AutoRetryingClosableIterator.of(rangeRequest, new Function<RangeRequest, ClosableIterator<RowResult<Value>>>() {
            @Override
            public ClosableIterator<RowResult<Value>> apply(
                    RangeRequest input) {
                return invalidateOnVersionChangeIterator(getRangeInternal(tableName, input, timestamp));
            }
        });
    }

    private ClosableIterator<RowResult<Value>> getRangeInternal(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {

        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
                runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Multimap<ConsistentRingRangeRequest, KeyValueEndpoint>>() {
            @Override
            public Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> apply(
                    DynamicPartitionMap input) {
                return input.getServicesForRangeRead(tableName, rangeRequest);
            }
        });

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
            protected Set<ClosablePeekingIterator<RowResult<Value>>> computeNextRange(final ConsistentRingRangeRequest range) {
                // This is NOT retryable
                return runWithPartitionMap(new Function<DynamicPartitionMap, Set<ClosablePeekingIterator<RowResult<Value>>>>() {
                    @Override
                    public Set<ClosablePeekingIterator<RowResult<Value>>> apply(
                            @Nullable DynamicPartitionMap input) {
                        final Set<ClosablePeekingIterator<RowResult<Value>>> result = Sets.newHashSet();

                        // We need at least one iterator for each range in order for the
                        // quorum exception mechanism to work properly. If all iterator
                        // requests failed we need to throw immediately to avoid silent failure.
                        RuntimeException lastSuppressedException = null;

                        for (KeyValueEndpoint vkve : services.get(range)) {
                            try {
                                ClosableIterator<RowResult<Value>> it = vkve.keyValueService().getRange(tableName, range.get(), timestamp);
                                result.add(ClosablePeekingIterator.of(it));
                            } catch (ClientVersionTooOldException e) {
                                throw e;
                            } catch (RuntimeException e) {
                                // If this failure is fatal for the range, the exception will be thrown when
                                // retrieving data from the iterators.
                                log.warn("Failed to getRange in table " + tableName);

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
                    }
                });
            }
        };
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
            final String tableName, RangeRequest rangeRequest, final long timestamp) {
        return AutoRetryingClosableIterator.of(rangeRequest, new Function<RangeRequest, ClosableIterator<RowResult<Set<Value>>>>() {
            @Override
            public ClosableIterator<RowResult<Set<Value>>> apply(
                    RangeRequest input) {
                return invalidateOnVersionChangeIterator(getRangeWithHistoryInternal(tableName, input, timestamp));
            }
        });
    }

    private ClosableIterator<RowResult<Set<Value>>> getRangeWithHistoryInternal(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
                runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Multimap<ConsistentRingRangeRequest, KeyValueEndpoint>>() {
                    @Override
                    public Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> apply(
                            DynamicPartitionMap input) {
                        return input.getServicesForRangeRead(tableName, rangeRequest);
                    }
                });

        return new PartitionedRangedIterator<Set<Value>>(services.keySet()) {
            @Override
            public RowResult<Set<Value>> computeNext() {
                Preconditions.checkState(hasNext());
                // This is NOT retryable
                return runWithPartitionMap(new Function<DynamicPartitionMap, RowResult<Set<Value>>>() {
                    @Override
                    public RowResult<Set<Value>> apply(DynamicPartitionMap input) {
                        return RowResults.allResults(getRowIterator());
                    }});
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Value>>>> computeNextRange(final ConsistentRingRangeRequest range) {
                // This is NOT retryable
                return runWithPartitionMap(new Function<DynamicPartitionMap, Set<ClosablePeekingIterator<RowResult<Set<Value>>>>>() {
                    @Override
                    public Set<ClosablePeekingIterator<RowResult<Set<Value>>>> apply(DynamicPartitionMap input) {
                        final Set<ClosablePeekingIterator<RowResult<Set<Value>>>> result = Sets.newHashSet();

                        // This method has stronger consistency guarantees. It has to talk to all endpoints
                        // and thus must throw immediately on any failure encountered.
                        for (KeyValueEndpoint vkve : services.get(range)) {
                            ClosableIterator<RowResult<Set<Value>>> it = vkve.keyValueService().getRangeWithHistory(
                                    tableName, range.get(), timestamp);
                            result.add(ClosablePeekingIterator.of(it));
                        }
                        return result;
                    }
                });
            }
        };
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            final String tableName, RangeRequest rangeRequest, final long timestamp)
                    throws InsufficientConsistencyException {
        return AutoRetryingClosableIterator.of(rangeRequest, new Function<RangeRequest, ClosableIterator<RowResult<Set<Long>>>>() {
            @Override
            public ClosableIterator<RowResult<Set<Long>>> apply(
                    RangeRequest input) {
                return invalidateOnVersionChangeIterator(getRangeOfTimestampsInternal(tableName, input, timestamp));
            }
        });
    }

    private ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestampsInternal(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp)
            throws InsufficientConsistencyException {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
                runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Multimap<ConsistentRingRangeRequest, KeyValueEndpoint>>() {
                    @Override
                    public Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> apply(DynamicPartitionMap input) {
                        return input.getServicesForRangeRead(tableName, rangeRequest);
                    }
                });

        return new PartitionedRangedIterator<Set<Long>>(services.keySet()) {
            @Override
            public RowResult<Set<Long>> computeNext() {
                Preconditions.checkState(hasNext());
                // This is NOT retryable
                return runWithPartitionMap(new Function<DynamicPartitionMap, RowResult<Set<Long>>>() {
                    @Override
                    public RowResult<Set<Long>> apply(DynamicPartitionMap input) {
                        return RowResults.allTimestamps(getRowIterator());
                    }
                });
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Long>>>> computeNextRange(final ConsistentRingRangeRequest range) {
                // This is NOT retryable
                return runWithPartitionMap(new Function<DynamicPartitionMap, Set<ClosablePeekingIterator<RowResult<Set<Long>>>>>() {
                    @Override
                    public Set<ClosablePeekingIterator<RowResult<Set<Long>>>> apply(DynamicPartitionMap input) {
                        final Set<ClosablePeekingIterator<RowResult<Set<Long>>>> result = Sets.newHashSet();

                        // This method has stronger consistency guarantees. It has to talk to all endpoints
                        // and thus must throw immediately on any failure encountered.
                        for (KeyValueEndpoint vkve : services.get(range)) {
                            ClosableIterator<RowResult<Set<Long>>> it = vkve.keyValueService().getRangeOfTimestamps(
                                    tableName, range.get(), timestamp);
                            result.add(ClosablePeekingIterator.of(it));
                        }

                        return result;
                    }
                });
            }
        };
    }

    // *** Write requests *************************************************************************
    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp) {
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> writeService = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Cell> tracker =
                        QuorumTracker.of(values.keySet(), input.getWriteCellsParameters(values.keySet()));

                input.runForCellsWrite(tableName, values, new Function<Pair<KeyValueService, Map<Cell, byte[]>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Map<Cell, byte[]>> e) {
                        Future<Void> future = writeService.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.put(tableName, e.rhSide, timestamp);
                                return null;
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide.keySet());
                        return null;
                    }
                });

                completeWriteRequest(tracker, writeService);
                return null;
            }
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {

        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(final DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Map.Entry<Cell, Value>> tracker = QuorumTracker.of(
                        cellValues.entries(), input.getWriteEntriesParameters(cellValues));

                input.runForCellsWrite(tableName, cellValues, new Function<Pair<KeyValueService, Multimap<Cell, Value>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Multimap<Cell, Value>> e) {
                        Future<Void> future = execSvc.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.putWithTimestamps(tableName, e.rhSide);
                                return null;
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide.entries());
                        return null;
                    }
                });

                completeWriteRequest(tracker, execSvc);
                return null;
            }
        });
    }

    /**
     * This operation is not supported for <code>PartitionedKeyValueService</code>.
     *
     */
    @Override
    @Deprecated
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        // TODO: This should eventually throw new UnsupportedOperationException().
        // For some testing purposes it does not do it now and calls putUnlessExists
        // on all relevant delegates which is NOT a correct solution!
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> writeService = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                        values.keySet(), quorumParameters.getWriteRequestParameters());

                input.runForCellsWrite(tableName, values, new Function<Pair<KeyValueService, Map<Cell, byte[]>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Map<Cell, byte[]>> e) {
                        Future<Void> future = writeService.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.putUnlessExists(tableName, e.rhSide);
                                return null;
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide.keySet());
                        return null;
                    }
                });

                completeWriteRequest(tracker, writeService);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Map.Entry<Cell, Long>> tracker = QuorumTracker.of(
                        keys.entries(), input.getWriteEntriesParameters(keys));

                input.runForCellsWrite(tableName, keys, new Function<Pair<KeyValueService, Multimap<Cell, Long>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Multimap<Cell, Long>> e) {
                        final Future<Void> future = execSvc.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.delete(tableName, e.rhSide);
                                return null;
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide.entries());
                        return null;
                    }
                });

                completeWriteRequest(tracker, execSvc);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName, final Set<Cell> cells) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(DynamicPartitionMap input) {
                final EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
                final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                        cells, input.getWriteCellsParameters(cells));

                input.runForCellsWrite(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
                    @Override
                    public Void apply(final Pair<KeyValueService, Set<Cell>> e) {
                        Future<Void> future = execSvc.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.addGarbageCollectionSentinelValues(tableName, e.rhSide);
                                return null;
                            }
                        }, e.lhSide);
                        tracker.registerRef(future, e.rhSide);
                        return null;
                    }
                });

                completeWriteRequest(tracker, execSvc);
                return null;
            }
        });
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        for (Map.Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final String tableName = e.getKey();
            final Map<Cell, byte[]> values = e.getValue();
            put(tableName, values, timestamp);
        }
    }

    // *** Table stuff
    // ***********************************************************************************
    @Override
    @Idempotent
    public void dropTable(final String tableName) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.dropTable(tableName);
                }
                return null;
            }
        });
    }

    @Override
    public void dropTables(final Set<String> tableNames) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.dropTables(tableNames);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final String tableName, final byte[] tableMetadata)
            throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.createTable(tableName, tableMetadata);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTable(final String tableName) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.truncateTable(tableName);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTables(final Set<String> tableNames) throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.truncateTables(tableNames);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTables(final Map<String, byte[]> tableNameToTableMetadata)
            throws InsufficientConsistencyException {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.createTables(tableNameToTableMetadata);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Set<String>>() {
            @Override
            public Set<String> apply(@Nullable DynamicPartitionMap input) {
                return retryUntilSuccess(
                        input.getDelegates().iterator(),
                        new Function<KeyValueService, Set<String>>() {
                            @Override
                            @Nullable
                            public Set<String> apply(@Nullable KeyValueService kvs) {
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
    public byte[] getMetadataForTable(final String tableName) {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, byte[]>() {
            @Override
            public byte[] apply(@Nullable DynamicPartitionMap input) {
                return retryUntilSuccess(
                        input.getDelegates().iterator(),
                        new Function<KeyValueService, byte[]>() {
                            @Override
                            public byte[] apply(@Nullable KeyValueService kvs) {
                                return kvs.getMetadataForTable(tableName);
                            }
                        });
            }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final String tableName, final byte[] metadata) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.putMetadataForTable(tableName, metadata);
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> apply(@Nullable DynamicPartitionMap input) {
                return retryUntilSuccess(
                        input.getDelegates().iterator(),
                        new Function<KeyValueService, Map<String, byte[]>>() {
                            @Override
                            public Map<String, byte[]> apply(KeyValueService kvs) {
                                return kvs.getMetadataForTables();
                            }
                        });
                    }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.putMetadataForTables(tableNameToMetadata);
                }
                return null;
            }
        });
    }

    // *** Simple forward methods ***************************************************************
    @Override
    public void compactInternally(final String tableName) {
        runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Void>() {
            @Override
            public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.compactInternally(tableName);
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

    public static PartitionedKeyValueService create(QuorumParameters quorumParameters, List<PartitionMapService> mapServices) {
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
    @VisibleForTesting @Deprecated
    public DynamicPartitionMapImpl getPartitionMap() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, DynamicPartitionMapImpl>() {
            @Override
            public DynamicPartitionMapImpl apply(DynamicPartitionMap input) {
                return (DynamicPartitionMapImpl) input;
            }
        });
    }

    private long getMapVersion() {
        return runWithPartitionMapRetryable(new Function<DynamicPartitionMap, Long>() {
            @Override
            public Long apply(DynamicPartitionMap input) {
                return input.getVersion();
            }
        });
    }

    private <T> ClosableIterator<RowResult<T>> invalidateOnVersionChangeIterator(ClosableIterator<RowResult<T>> it) {
        return VersionCheckProxy.invalidateOnVersionChangeProxy(it, new Supplier<Long>() {
            @Override
            public Long get() {
                return getMapVersion();
            }
        });
    }
}
