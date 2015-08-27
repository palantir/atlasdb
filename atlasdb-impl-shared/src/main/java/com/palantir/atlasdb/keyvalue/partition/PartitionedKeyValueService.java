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
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.ClosablePeekingIterator;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.PartitionedRangedIterator;
import com.palantir.atlasdb.keyvalue.partition.util.RowResultUtil;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public abstract class PartitionedKeyValueService implements KeyValueService {

    // Thread-safe
    private static final Logger log = LoggerFactory.getLogger(PartitionedKeyValueService.class);

    // Immutable
    private final QuorumParameters quorumParameters;

    // Thread-safe
    private final ExecutorService executor;

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
                    tracker.handleFailure(future);
                    // Check if the failure is fatal
                    if (tracker.failed()) {
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    <TrackingUnit, FutureReturnType> void completeReadRequest(QuorumTracker<FutureReturnType, TrackingUnit> tracker,
                                                              ExecutorCompletionService<FutureReturnType> execSvc,
                                                              Function<FutureReturnType, Void> mergeFunction) {
        try {
            completeRequest(tracker, execSvc, mergeFunction);
        } finally {
            tracker.cancel(true);
        }
    }

    <TrackingUnit> void completeWriteRequest(QuorumTracker<Void, TrackingUnit> tracker,
                                             ExecutorCompletionService<Void> execSvc) {

        try {
            completeRequest(tracker, execSvc, Functions.<Void> identity());
        } catch (RuntimeException e) {
            tracker.cancel(true);
        }
    }

    // *** Read requests *************************************************************************
    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        final Map<Cell, Value> overallResult = Maps.newHashMap();
        final ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                executor);
        final QuorumTracker<Map<Cell, Value>, byte[]> tracker = QuorumTracker.of(
                rows,
                quorumParameters.getReadRequestParameters());

        // Schedule tasks for execution
        getPartitionMap().runForRowsRead(tableName, rows, new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
            @Override @Nullable
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

        completeReadRequest(tracker, execSvc, new Function<Map<Cell, Value>, Void>() {
            @Override
            @Nullable
            public Void apply(@Nullable Map<Cell, Value> input) {
                mergeCellValueMapIntoMap(overallResult, input);
                return null;
            }
        });
        return overallResult;
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, Map<Cell, Long> timestampByCell) {
        final ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                executor);
        final QuorumTracker<Map<Cell, Value>, Cell> tracker = QuorumTracker.of(
                timestampByCell.keySet(),
                quorumParameters.getReadRequestParameters());
        final Map<Cell, Value> globalResult = Maps.newHashMap();

        // Schedule the tasks
        getPartitionMap().runForCellsRead(tableName, timestampByCell, new Function<Pair<KeyValueService, Map<Cell, Long>>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable final Pair<KeyValueService, Map<Cell, Long>> e) {
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

        completeRequest(tracker, execSvc, new Function<Map<Cell, Value>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Map<Cell, Value> input) {
                mergeCellValueMapIntoMap(globalResult, input);
                return null;
            }
        });
        return globalResult;
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {
        final ExecutorCompletionService<Multimap<Cell, Long>> execSvc = new ExecutorCompletionService<Multimap<Cell, Long>>(
                executor);
        final QuorumTracker<Multimap<Cell, Long>, Cell> tracker = QuorumTracker.of(
                cells,
                quorumParameters.getNoFailureRequestParameters());
        final Multimap<Cell, Long> globalResult = HashMultimap.create();

        getPartitionMap().runForCellsRead(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
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

        completeReadRequest(tracker, execSvc, new Function<Multimap<Cell, Long>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable Multimap<Cell, Long> input) {
                mergeAllTimestampsMapIntoMap(globalResult, input);
                return null;
            }
        });
        return globalResult;
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final String tableName,
                                               Map<Cell, Long> timestampByCell) {
        final Map<Cell, Long> globalResult = Maps.newHashMap();
        final QuorumTracker<Map<Cell, Long>, Cell> tracker = QuorumTracker.of(
                timestampByCell.keySet(),
                quorumParameters.getReadRequestParameters());
        final ExecutorCompletionService<Map<Cell, Long>> execSvc = new ExecutorCompletionService<Map<Cell, Long>>(
                executor);

        getPartitionMap().runForCellsRead(tableName, timestampByCell, new Function<Pair<KeyValueService, Map<Cell, Long>>, Void>() {
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

        completeReadRequest(tracker, execSvc, new Function<Map<Cell, Long>, Void>() {
            @Override
            @Nullable
            public Void apply(@Nullable Map<Cell, Long> input) {
                mergeLatestTimestampMapIntoMap(globalResult, input);
                return null;
            }
        });
        return globalResult;
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(
                this,
                tableName,
                rangeRequests,
                timestamp);
    }

    // *** Read range requests ***
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       RangeRequest rangeRequest,
                                                       final long timestamp) {

        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services = getPartitionMap().getServicesForRangeRead(
                tableName,
                rangeRequest);

        return new PartitionedRangedIterator<Value>(services.keySet()) {
            @Override
            public RowResult<Value> computeNext() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.mergeResults(
                        getRowIterator(),
                        quorumParameters.getReadRequestParameters());
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Value>>> computeNextRange(final ConsistentRingRangeRequest range) {
                final Set<ClosablePeekingIterator<RowResult<Value>>> result = Sets.newHashSet();
                for (KeyValueEndpoint vkve : services.get(range)) {
                    try {
                        ClosableIterator<RowResult<Value>> it = vkve.keyValueService().getRange(tableName, range.get(), timestamp);
                        result.add(ClosablePeekingIterator.of(it));
                    } catch (RuntimeException e) {
                        // TODO:
                        // If this failure is fatal for the range, the exception will be thrown when
                        // retrieving data from the iterators.
                        log.warn("Failed to getRange in table " + tableName);
                    } finally {
                    }
                }
                return result;
            }
        };
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(final String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       final long timestamp) {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services = getPartitionMap().getServicesForRangeRead(
                tableName,
                rangeRequest);

        return new PartitionedRangedIterator<Set<Value>>(services.keySet()) {
            @Override
            public RowResult<Set<Value>> computeNext() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.allResults(getRowIterator());
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Value>>>> computeNextRange(final ConsistentRingRangeRequest range) {
                final Set<ClosablePeekingIterator<RowResult<Set<Value>>>> result = Sets.newHashSet();
                for (KeyValueEndpoint vkve : services.get(range)) {
                    try {
                        ClosableIterator<RowResult<Set<Value>>> it = vkve.keyValueService().getRangeWithHistory(
                                tableName, range.get(), timestamp);
                        result.add(ClosablePeekingIterator.of(it));
                    } catch (RuntimeException e) {
                        // TODO:
                        // If this failure is fatal for the range, the exception will be thrown when
                        // retrieving data from the iterators.
                        log.warn("Failed to getRangeWithHistory in table " + tableName);
                    }
                }
                return result;
            }
        };
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(final String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       final long timestamp)
            throws InsufficientConsistencyException {
        final Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> services = getPartitionMap().getServicesForRangeRead(
                tableName,
                rangeRequest);

        return new PartitionedRangedIterator<Set<Long>>(services.keySet()) {
            @Override
            public RowResult<Set<Long>> computeNext() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.allTimestamps(getRowIterator());
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Long>>>> computeNextRange(final ConsistentRingRangeRequest range) {
                final Set<ClosablePeekingIterator<RowResult<Set<Long>>>> result = Sets.newHashSet();
                for (KeyValueEndpoint vkve : services.get(range)) {
                    try {
                        ClosableIterator<RowResult<Set<Long>>> it = vkve.keyValueService().getRangeOfTimestamps(
                                tableName, range.get(), timestamp);
                        result.add(ClosablePeekingIterator.of(it));
                    } catch (RuntimeException e) {
                        // TODO:
                        // If this failure is fatal for the range, the exception will be thrown when
                        // retrieving data from the iterators.
                        log.warn("Failed to getRangeOfTimestamps in table " + tableName);
                    }
                }
                return result;
            }
        };
    }

    // *** Write requests *************************************************************************
    @Override
    public void put(final String tableName, Map<Cell, byte[]> values, final long timestamp) {
        final ExecutorCompletionService<Void> writeService = new ExecutorCompletionService<Void>(
                executor);
        final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                values.keySet(),
                quorumParameters.getWriteRequestParameters());

        getPartitionMap().runForCellsWrite(tableName, values, new Function<Pair<KeyValueService, Map<Cell, byte[]>>, Void>() {
            @Override @Nullable
            public Void apply(@Nullable final Pair<KeyValueService, Map<Cell, byte[]>> e) {
                Future<Void> future = writeService.submit(new Callable<Void>() {
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
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(
                executor);
        final QuorumTracker<Void, Map.Entry<Cell, Value>> tracker = QuorumTracker.of(
                cellValues.entries(),
                quorumParameters.getWriteRequestParameters());

        getPartitionMap().runForCellsWrite(tableName, cellValues, new Function<Pair<KeyValueService, Multimap<Cell, Value>>, Void>() {
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
    }

    @Override
    public void putUnlessExists(final String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        // TODO
        // put(tableName, values, 0);
        final ExecutorCompletionService<Void> writeService = new ExecutorCompletionService<Void>(
                executor);
        final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                values.keySet(),
                quorumParameters.getWriteRequestParameters());

        getPartitionMap().runForCellsWrite(tableName, values, new Function<Pair<KeyValueService, Map<Cell, byte[]>>, Void>() {
            @Override @Nullable
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
    }

    @Override
    @Idempotent
    public void delete(final String tableName, Multimap<Cell, Long> keys) {
        final QuorumTracker<Void, Map.Entry<Cell, Long>> tracker = QuorumTracker.of(
                keys.entries(),
                quorumParameters.getNoFailureRequestParameters());
        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(
                executor);

        getPartitionMap().runForCellsWrite(tableName, keys, new Function<Pair<KeyValueService, Multimap<Cell, Long>>, Void>() {
            @Override @Nullable
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
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName, Set<Cell> cells) {
        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(executor);
        final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                cells,
                quorumParameters.getWriteRequestParameters());

        getPartitionMap().runForCellsWrite(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
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
    public void dropTable(String tableName) throws InsufficientConsistencyException {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.dropTable(tableName);
        }
    }

    @Override
    @Idempotent
    public void createTable(String tableName, int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.createTable(tableName, maxValueSizeInBytes);
        }
    }

    @Override
    @Idempotent
    public void truncateTable(String tableName) throws InsufficientConsistencyException {
        for (KeyValueService kvs : getDelegates()) {
            kvs.truncateTable(tableName);
        }
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        for (KeyValueService kvs : getDelegates()) {
            kvs.truncateTables(tableNames);
        }
    }

    @Override
    @Idempotent
    public void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes)
            throws InsufficientConsistencyException {
        for (KeyValueService kvs : getDelegates()) {
            kvs.createTables(tableNamesToMaxValueSizeInBytes);
        }
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        return retryUntilSuccess(
                getPartitionMap().getDelegates().iterator(),
                new Function<KeyValueService, Set<String>>() {
                    @Override
                    @Nullable
                    public Set<String> apply(@Nullable KeyValueService kvs) {
                        return kvs.getAllTableNames();
                    }
                });
    }

    // *** Metadata
    // ****************************************************************************************
    @Override
    @Idempotent
    public byte[] getMetadataForTable(final String tableName) {
        return retryUntilSuccess(
                getPartitionMap().getDelegates().iterator(),
                new Function<KeyValueService, byte[]>() {
                    @Override
                    @Nullable
                    public byte[] apply(@Nullable KeyValueService kvs) {
                        return kvs.getMetadataForTable(tableName);
                    }
                });
    }

    @Override
    @Idempotent
    public void putMetadataForTable(String tableName, byte[] metadata) {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.putMetadataForTable(tableName, metadata);
        }
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        return retryUntilSuccess(
                getPartitionMap().getDelegates().iterator(),
                new Function<KeyValueService, Map<String, byte[]>>() {
                    @Override
                    @Nullable
                    public Map<String, byte[]> apply(@Nullable KeyValueService kvs) {
                        return kvs.getMetadataForTables();
                    }
                });
    }

    @Override
    @Idempotent
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.putMetadataForTables(tableNameToMetadata);
        }
    }

    // *** Simple forward methods ***************************************************************
    @Override
    public void compactInternally(String tableName) {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.compactInternally(tableName);
        }
    }

    @Override
    public void close() {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.close();
        }
    }

    @Override
    public void teardown() {
        for (KeyValueService kvs : getPartitionMap().getDelegates()) {
            kvs.teardown();
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return getPartitionMap().getDelegates();
    }

    @Override
    public void initializeFromFreshInstance() {
        for (KeyValueService kvs : getDelegates()) {
            kvs.initializeFromFreshInstance();
        }
    }

    // *** Creation *******************************************************************************
//    public static PartitionedKeyValueService create(Set<? extends KeyValueService> svcPool,
//                                                    QuorumParameters quorumParameters) {
//        Preconditions.checkArgument(svcPool.size() == 5);
//        NavigableMap<byte[], KeyValueService> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
//
//        final byte[][] points = new byte[][] { new byte[] { (byte) 0x11 },
//                new byte[] { (byte) 0x22 }, new byte[] { (byte) 0x33 }, new byte[] { (byte) 0x44 },
//                new byte[] { (byte) 0x55 } };
//
//        int i = 0;
//        for (KeyValueService kvs : svcPool) {
//            ring.put(points[i++], kvs);
//        }
//
//        return create(ring, quorumParameters);
//    }
    protected PartitionedKeyValueService(ExecutorService executor, QuorumParameters quorumParameters) {
        this.executor = executor;
        this.quorumParameters = quorumParameters;
    }

    // *** Helper methods *************************************************************************
    private static <T, U, V extends Iterator<? extends U>> T retryUntilSuccess(V iterator,
                                                                               Function<U, T> fun) {

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

    private void mergeCellValueMapIntoMap(Map<Cell, Value> globalResult, Map<Cell, Value> partResult) {
        for (Map.Entry<Cell, Value> e : partResult.entrySet()) {
            if (!globalResult.containsKey(e.getKey())
                    || globalResult.get(e.getKey()).getTimestamp() < e.getValue().getTimestamp()) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    private void mergeLatestTimestampMapIntoMap(Map<Cell, Long> globalResult,
                                                Map<Cell, Long> partResult) {
        for (Map.Entry<Cell, Long> e : partResult.entrySet()) {
            if (!globalResult.containsKey(e.getKey())
                    || globalResult.get(e.getKey()) < e.getValue()) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    private void mergeAllTimestampsMapIntoMap(Multimap<Cell, Long> globalResult,
                                              Multimap<Cell, Long> partResult) {
        for (Map.Entry<Cell, Long> e : partResult.entries()) {
            if (!globalResult.containsEntry(e.getKey(), e.getValue())) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    protected abstract PartitionMap getPartitionMap();

}
