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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
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
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.merge.MergeResultsUtils;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumTracker;
import com.palantir.atlasdb.keyvalue.partition.util.ClosablePeekingIterator;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.PartitionedRangedIterator;
import com.palantir.atlasdb.keyvalue.partition.util.RowResultUtil;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
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

    /**
     * This will block until success or failure of the request can be concluded.
     * In case of failure it will rethrow the last encountered exception.
     *
     * TODO: Is it ok if the remaining futures are not beign taken after tracker
     * is finished? Zombie threads etc.
     *
     * @param tracker
     * @param execSvc
     * @param mergeFunction
     */
    <TrackingUnit, FutureReturnType> void completeRequest(QuorumTracker<FutureReturnType, TrackingUnit> tracker,
                                                          ExecutorCompletionService<FutureReturnType> execSvc,
                                                          Function<FutureReturnType, Void> mergeFunction) {
        try {
            // Wait until we can conclude success or failure
            while (!tracker.finished()) {
                Future<FutureReturnType> future = execSvc.take();
                try {
                    FutureReturnType result = future.get();
                    mergeFunction.apply(result);
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    // These two exceptions should be thrown immediately
                    if (cause instanceof KeyAlreadyExistsException || cause instanceof VersionTooOldException) {
                        Throwables.throwUncheckedException(cause);
                    }
                    tracker.handleFailure(future);
                    // Check if the failure is fatal
                    if (tracker.failed()) {
                        Throwables.rewrapAndThrowUncheckedException(cause);
                    }
                }
            }
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    /**
     * In case of read request we can cancel all remaining threads as soon as completeRequests
     * returns - it means that either success or failure has been concluded.
     *
     * @param tracker
     * @param execSvc
     * @param mergeFunction
     */
    <TrackingUnit, FutureReturnType> void completeReadRequest(QuorumTracker<FutureReturnType, TrackingUnit> tracker,
                                                              ExecutorCompletionService<FutureReturnType> execSvc,
                                                              Function<FutureReturnType, Void> mergeFunction) {
        try {
            completeRequest(tracker, execSvc, mergeFunction);
        } finally {
            tracker.cancel(true);
        }
    }

    /**
     * In case of write requests we should only cancel all the threads if a failure can be
     * concluded.
     * Otherwise we just return as soon as success is concluded but we leave other write
     * tasks running in the background.
     *
     * @param tracker
     * @param execSvc
     */
    <TrackingUnit> void completeWriteRequest(QuorumTracker<Void, TrackingUnit> tracker,
                                             ExecutorCompletionService<Void> execSvc) {

        try {
            completeRequest(tracker, execSvc, Functions.<Void> identity());
        } catch (RuntimeException e) {
            tracker.cancel(true);
            throw e;
        }
    }

    // *** Read requests *************************************************************************
    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName, final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection, final long timestamp) {
        return runWithPartitionMap(new Function<DynamicPartitionMap, Map<Cell, Value>>() {
			@Override
			public Map<Cell, Value> apply(DynamicPartitionMap input) {
                final Map<Cell, Value> overallResult = Maps.newHashMap();
                final ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                        executor);
                final QuorumTracker<Map<Cell, Value>, byte[]> tracker = QuorumTracker.of(
                        rows,
                        quorumParameters.getReadRequestParameters());

                // Schedule tasks for execution
                input.runForRowsRead(tableName, rows, new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
                    @Override
                    public Void apply(@Nullable final Pair<KeyValueService, Iterable<byte[]>> e) {
                        Future<Map<Cell, Value>> future = execSvc.submit(new Callable<Map<Cell, Value>>() {
                            @Override
                            public Map<Cell, Value> call() throws Exception {
                                return e.lhSide.getRows(tableName, e.rhSide, columnSelection, timestamp);
                            }
                        });
                        tracker.registerRef(future, e.rhSide);
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResultsUtils.newCellValueMapMerger(overallResult));
                return overallResult;
			}
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, final Map<Cell, Long> timestampByCell) {
        return runWithPartitionMap(new Function<DynamicPartitionMap, Map<Cell, Value>>() {
			@Override
			public Map<Cell, Value> apply(@Nullable DynamicPartitionMap input) {
                final ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                        executor);
                final QuorumTracker<Map<Cell, Value>, Cell> tracker = QuorumTracker.of(
                        timestampByCell.keySet(),
                        quorumParameters.getReadRequestParameters());
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
                        });
                        tracker.registerRef(future, e.rhSide.keySet());
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResultsUtils.newCellValueMapMerger(globalResult));
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

        return runWithPartitionMap(new Function<DynamicPartitionMap, Multimap<Cell, Long>>() {
			@Override
			public Multimap<Cell, Long> apply(DynamicPartitionMap input) {
                final ExecutorCompletionService<Multimap<Cell, Long>> execSvc = new ExecutorCompletionService<Multimap<Cell, Long>>(
                        executor);
                final QuorumTracker<Multimap<Cell, Long>, Cell> tracker = QuorumTracker.of(
                        cells,
                        quorumParameters.getNoFailureRequestParameters());
                final Multimap<Cell, Long> globalResult = HashMultimap.create();
                input.runForCellsRead(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
                    @Override @Nullable
                    public Void apply(@Nullable final Pair<KeyValueService, Set<Cell>> e) {
                        Future<Multimap<Cell, Long>> future = execSvc.submit(new Callable<Multimap<Cell, Long>>() {
                            @Override
                            public Multimap<Cell, Long> call() throws Exception {
                                return e.lhSide.getAllTimestamps(tableName, cells, timestamp);
                            }
                        });
                        tracker.registerRef(future, e.rhSide);
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResultsUtils.newAllTimestampsMapMerger(globalResult));
                return globalResult;
			}
		});
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final String tableName,
                                               final Map<Cell, Long> timestampByCell) {
        return runWithPartitionMap(new Function<DynamicPartitionMap, Map<Cell, Long>>() {
			@Override
			public Map<Cell, Long> apply(DynamicPartitionMap input) {
                final Map<Cell, Long> globalResult = Maps.newHashMap();
                final QuorumTracker<Map<Cell, Long>, Cell> tracker = QuorumTracker.of(
                        timestampByCell.keySet(),
                        quorumParameters.getReadRequestParameters());
                final ExecutorCompletionService<Map<Cell, Long>> execSvc = new ExecutorCompletionService<Map<Cell, Long>>(
                        executor);
                input.runForCellsRead(tableName, timestampByCell, new Function<Pair<KeyValueService, Map<Cell, Long>>, Void>() {
                    @Override @Nullable
                    public Void apply(@Nullable final Pair<KeyValueService, Map<Cell, Long>> e) {
                        Future<Map<Cell, Long>> future = execSvc.submit(new Callable<Map<Cell, Long>>() {
                            @Override
                            public Map<Cell, Long> call() throws Exception {
                                return e.lhSide.getLatestTimestamps(tableName, e.rhSide);
                            }
                        });
                        tracker.registerRef(future, e.rhSide.keySet());
                        return null;
                    }
                });

                completeReadRequest(tracker, execSvc, MergeResultsUtils.newLatestTimestampMapMerger(globalResult));
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
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {

        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
                runWithPartitionMap(new Function<DynamicPartitionMap, Multimap<ConsistentRingRangeRequest, KeyValueEndpoint>>() {
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
                return runWithPartitionMap(new Function<DynamicPartitionMap, RowResult<Value>>() {
                    @Override
                    public RowResult<Value> apply(DynamicPartitionMap input) {
                        return RowResultUtil.mergeResults(getRowIterator(), quorumParameters.getReadRequestParameters());
                    }
                });
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Value>>> computeNextRange(final ConsistentRingRangeRequest range) {
                return runWithPartitionMap(new Function<DynamicPartitionMap, Set<ClosablePeekingIterator<RowResult<Value>>>>() {
                    @Override
                    public Set<ClosablePeekingIterator<RowResult<Value>>> apply(
                            @Nullable DynamicPartitionMap input) {
                        final Set<ClosablePeekingIterator<RowResult<Value>>> result = Sets.newHashSet();
                        for (KeyValueEndpoint vkve : services.get(range)) {
                            try {
                                ClosableIterator<RowResult<Value>> it = vkve.keyValueService().getRange(tableName, range.get(), timestamp);
                                result.add(ClosablePeekingIterator.of(it));
                            } catch (VersionTooOldException e) {
                                throw e;
                            } catch (RuntimeException e) {
                                // If this failure is fatal for the range, the exception will be thrown when
                                // retrieving data from the iterators.
                                //
                                // TODO: This is not true if there are no elements to be retrieved!
                                log.warn("Failed to getRange in table " + tableName);
                            } finally {
                            }
                        }
                        return result;
                    }
                });
            }
        };
}

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
        		runWithPartitionMap(new Function<DynamicPartitionMap, Multimap<ConsistentRingRangeRequest, KeyValueEndpoint>>() {
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
                return runWithPartitionMap(new Function<DynamicPartitionMap, RowResult<Set<Value>>>() {
                    @Override
                    public RowResult<Set<Value>> apply(DynamicPartitionMap input) {
                        return RowResultUtil.allResults(getRowIterator());
                    }});
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Value>>>> computeNextRange(final ConsistentRingRangeRequest range) {
                return runWithPartitionMap(new Function<DynamicPartitionMap, Set<ClosablePeekingIterator<RowResult<Set<Value>>>>>() {
                    @Override
                    public Set<ClosablePeekingIterator<RowResult<Set<Value>>>> apply(DynamicPartitionMap input) {
                        final Set<ClosablePeekingIterator<RowResult<Set<Value>>>> result = Sets.newHashSet();
                        for (KeyValueEndpoint vkve : services.get(range)) {
                            try {
                                ClosableIterator<RowResult<Set<Value>>> it = vkve.keyValueService().getRangeWithHistory(
                                        tableName, range.get(), timestamp);
                                result.add(ClosablePeekingIterator.of(it));
                            } catch (VersionTooOldException e) {
                                throw e;
                            } catch (RuntimeException e) {
                                // If this failure is fatal for the range, the exception will be thrown when
                                // retrieving data from the iterators.
                                // TODO
                                log.warn("Failed to getRangeWithHistory in table " + tableName);
                            }
                        }
                        return result;
                    }
                });
            }
        };
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp)
            throws InsufficientConsistencyException {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services =
        		runWithPartitionMap(new Function<DynamicPartitionMap, Multimap<ConsistentRingRangeRequest, KeyValueEndpoint>>() {
					@Override
					public Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> apply(DynamicPartitionMap input) {
                        return input.getServicesForRangeRead(tableName, rangeRequest);
                    }
				});

        return new PartitionedRangedIterator<Set<Long>>(services.keySet()) {
            @Override
            public RowResult<Set<Long>> computeNext() {
                Preconditions.checkState(hasNext());
                return runWithPartitionMap(new Function<DynamicPartitionMap, RowResult<Set<Long>>>() {
                    @Override
                    public RowResult<Set<Long>> apply(DynamicPartitionMap input) {
                        return RowResultUtil.allTimestamps(getRowIterator());
                    }
                });
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Long>>>> computeNextRange(final ConsistentRingRangeRequest range) {
                return runWithPartitionMap(new Function<DynamicPartitionMap, Set<ClosablePeekingIterator<RowResult<Set<Long>>>>>() {
                    @Override
                    public Set<ClosablePeekingIterator<RowResult<Set<Long>>>> apply(DynamicPartitionMap input) {
                        final Set<ClosablePeekingIterator<RowResult<Set<Long>>>> result = Sets.newHashSet();
                        for (KeyValueEndpoint vkve : services.get(range)) {
                            try {
                                ClosableIterator<RowResult<Set<Long>>> it = vkve.keyValueService().getRangeOfTimestamps(
                                        tableName, range.get(), timestamp);
                                result.add(ClosablePeekingIterator.of(it));
                            } catch (VersionTooOldException e) {
                                throw e;
                            } catch (RuntimeException e) {
                                // TODO:
                                // If this failure is fatal for the range, the exception will be thrown when
                                // retrieving data from the iterators.
                                log.warn("Failed to getRangeOfTimestamps in table " + tableName);
                            }
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
			public Void apply(@Nullable DynamicPartitionMap input) {
                final ExecutorCompletionService<Void> writeService = new ExecutorCompletionService<Void>(
                        executor);
                final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                        values.keySet(),
                quorumParameters.getWriteRequestParameters());
				input.runForCellsWrite( tableName, values, new Function<Pair<KeyValueService, Map<Cell, byte[]>>, Void>() {
							@Override
                            public Void apply(final Pair<KeyValueService, Map<Cell, byte[]>> e) {
                                Future<Void> future = writeService
										.submit(new Callable<Void>() {
											@Override
											public Void call() throws Exception {
												e.lhSide.put(tableName, e.rhSide, timestamp);
												return null;
											}
										});
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
                final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(
                        executor);
                final QuorumTracker<Void, Map.Entry<Cell, Value>> tracker = QuorumTracker.of(
                        cellValues.entries(),
                quorumParameters.getWriteRequestParameters());
                input.runForCellsWrite(tableName, cellValues, new Function<Pair<KeyValueService, Multimap<Cell, Value>>, Void>() {
                    @Override @Nullable
                    public Void apply(@Nullable final Pair<KeyValueService, Multimap<Cell, Value>> e) {
                        Future<Void> future = execSvc.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.putWithTimestamps(tableName, e.rhSide);
                                return null;
                            }
                        });
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
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        // TODO
        // put(tableName, values, 0);
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
			@Override
			public Void apply(@Nullable DynamicPartitionMap input) {
                final ExecutorCompletionService<Void> writeService = new ExecutorCompletionService<Void>(
                        executor);
                final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                        values.keySet(),
                quorumParameters.getWriteRequestParameters());
                input.runForCellsWrite(tableName, values, new Function<Pair<KeyValueService, Map<Cell, byte[]>>, Void>() {
                    @Override
                    public Void apply(@Nullable final Pair<KeyValueService, Map<Cell, byte[]>> e) {
                        Future<Void> future = writeService.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.putUnlessExists(tableName, e.rhSide);
                                return null;
                            }
                        });
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
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
			@Override
			public Void apply(DynamicPartitionMap input) {
                final QuorumTracker<Void, Map.Entry<Cell, Long>> tracker = QuorumTracker.of(
                        keys.entries(),
                        quorumParameters.getNoFailureRequestParameters());
                final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(
                        executor);
                input.runForCellsWrite(tableName, keys, new Function<Pair<KeyValueService, Multimap<Cell, Long>>, Void>() {
                    @Override
                    public Void apply(@Nullable final Pair<KeyValueService, Multimap<Cell, Long>> e) {
                        final Future<Void> future = execSvc.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.delete(tableName, e.rhSide);
                                return null;
                            }
                        });
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
        runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
			@Override
			public Void apply(DynamicPartitionMap input) {
                final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(executor);
                final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                        cells,
                        quorumParameters.getWriteRequestParameters());

                input.runForCellsWrite(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
                    @Override @Nullable
                    public Void apply(@Nullable final Pair<KeyValueService, Set<Cell>> e) {
                        Future<Void> future = execSvc.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                e.lhSide.addGarbageCollectionSentinelValues(tableName, e.rhSide);
                                return null;
                            }
                        });
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
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    @Idempotent
    public void createTable(final String tableName, final int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
			@Override
			public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.createTable(tableName, maxValueSizeInBytes);
                }
				return null;
			}
		});
    }

    @Override
    @Idempotent
	public void truncateTable(final String tableName) throws InsufficientConsistencyException {
		runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    public void createTables(final Map<String, Integer> tableNamesToMaxValueSizeInBytes)
    		throws InsufficientConsistencyException {
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
			@Override
			public Void apply(@Nullable DynamicPartitionMap input) {
                for (KeyValueService kvs : input.getDelegates()) {
                    kvs.createTables(tableNamesToMaxValueSizeInBytes);
                }
				return null;
			}
		});
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
    	return runWithPartitionMap(new Function<DynamicPartitionMap, Set<String>>() {
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
    	return runWithPartitionMap(new Function<DynamicPartitionMap, byte[]>() {
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
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    	return runWithPartitionMap(new Function<DynamicPartitionMap, Map<String, byte[]>>() {
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
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    	return runWithPartitionMap(new Function<DynamicPartitionMap, Collection<? extends KeyValueService>>() {
			@Override
			public Collection<? extends KeyValueService> apply(
					DynamicPartitionMap input) {
				return input.getDelegates();
			}
		});
    }

    @Override
    public void initializeFromFreshInstance() {
    	runWithPartitionMap(new Function<DynamicPartitionMap, Void>() {
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
    protected PartitionedKeyValueService(ExecutorService executor, QuorumParameters quorumParameters, DynamicPartitionMap partitionMap) {
    	super(partitionMap);
        this.executor = executor;
        this.quorumParameters = quorumParameters;
    }

    public static PartitionedKeyValueService create(QuorumParameters quorumParameters, DynamicPartitionMap partitionMap) {
        return new PartitionedKeyValueService(PTExecutors.newCachedThreadPool(), quorumParameters, partitionMap);
    }

    public static PartitionedKeyValueService create(DynamicPartitionMap partitionMap) {
        return create(new QuorumParameters(3, 2, 2), partitionMap);
    }

    // *** Helper methods *************************************************************************
    private static <T, U, V extends Iterator<? extends U>> T retryUntilSuccess(V iterator, Function<U, T> fun) {

        while (iterator.hasNext()) {
            U service = iterator.next();
            try {
                return fun.apply(service);
            } catch (RuntimeException e) {
                log.warn("retryUntilSuccess: " + e.getMessage());
                if (!iterator.hasNext()) {
                    Throwables.rewrapAndThrowUncheckedException("retryUntilSuccess", e);
                }
            }
        }
        throw new RuntimeException("This should never happen!");
    }

    public DynamicPartitionMapImpl getPartitionMap() {
        return runWithPartitionMap(new Function<DynamicPartitionMap, DynamicPartitionMapImpl>() {
            @Override
            public DynamicPartitionMapImpl apply(DynamicPartitionMap input) {
                return (DynamicPartitionMapImpl) input;
            }
        });
    }
}
