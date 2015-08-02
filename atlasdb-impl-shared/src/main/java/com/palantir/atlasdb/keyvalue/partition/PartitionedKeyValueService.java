package com.palantir.atlasdb.keyvalue.partition;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.partition.api.TableAwarePartitionMapApi;
import com.palantir.atlasdb.keyvalue.partition.util.ClosablePeekingIterator;
import com.palantir.atlasdb.keyvalue.partition.util.PartitionedRangedIterator;
import com.palantir.atlasdb.keyvalue.partition.util.RowResultUtil;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class PartitionedKeyValueService implements KeyValueService {

    private static final Logger log = LoggerFactory.getLogger(PartitionedKeyValueService.class);
    private final TableAwarePartitionMapApi tpm;
    private final QuorumParameters quorumParameters = new QuorumParameters(3, 2, 2);

    private PartitionedKeyValueService(NavigableMap<byte[], KeyValueService> ring, ExecutorService executor) {
        this.tpm = BasicPartitionMap.create(quorumParameters, ring);
        this.executor = executor;
    }

    @Override
    public void initializeFromFreshInstance() {
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        final Map<Cell, Value> overallResult = Maps.newHashMap();
        final ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                executor);
        final Map<KeyValueService, ? extends Iterable<byte[]>> tasks = tpm.getServicesForRowsRead(tableName, rows);
        final QuorumTracker<Map<Cell, Value>, byte[]> tracker = QuorumTracker.of(rows, quorumParameters.getReadRequestParameters());

        // Schedule tasks for execution
        for (final Map.Entry<KeyValueService, ? extends Iterable<byte[]>> e : tasks.entrySet()) {
            Future<Map<Cell, Value>> future = execSvc.submit(new Callable<Map<Cell, Value>>() {
                @Override
                public Map<Cell, Value> call() throws Exception {
                    return e.getKey().getRows(tableName, e.getValue(), columnSelection, timestamp);
                }
            });
            tracker.registerRef(future, e.getValue());
        }

        try {
            // Wait until we can conclude success or failure
            while (!tracker.finished()) {
                Future<Map<Cell, Value>> future = execSvc.take();
                try {
                    Map<Cell, Value> result = future.get();
                    mergeCellValueMapIntoMap(overallResult, result);
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    log.warn("Could not complete read request in table " + tableName);
                    tracker.handleFailure(future);
                    // Check if the failure is fatal
                    if (tracker.failure()) {
                        throw Throwables.rewrapAndThrowUncheckedException("Could not get enough reads.", e.getCause());
                    }
                }
            }
            return overallResult;
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            tracker.cancel(true);
        }
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, Map<Cell, Long> timestampByCell) {
        Map<KeyValueService, Map<Cell, Long>> tasks = tpm.getServicesForCellsRead(tableName, timestampByCell);
        ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                executor);
        QuorumTracker<Map<Cell, Value>, Cell> tracker = QuorumTracker.of(
                timestampByCell.keySet(),
                quorumParameters.getReadRequestParameters());
        Map<Cell, Value> globalResult = Maps.newHashMap();

        // Schedule the tasks
        for (final Map.Entry<KeyValueService, Map<Cell, Long>> e : tasks.entrySet()) {
            Future<Map<Cell, Value>> future = execSvc.submit(new Callable<Map<Cell, Value>>() {
                @Override
                public Map<Cell, Value> call() throws Exception {
                    return e.getKey().get(tableName, e.getValue());
                }
            });
            tracker.registerRef(future, e.getValue().keySet());
        }

        // Wait until success or failure can be concluded
        try {
            while (!tracker.finished()) {
                Future<Map<Cell, Value>> future = execSvc.take();
                try {
                    Map<Cell, Value> result = future.get();
                    mergeCellValueMapIntoMap(globalResult, result);
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    log.warn("Could not complete read in table " + tableName);
                    tracker.handleFailure(future);
                    // Check if the failure was fatal
                    if (tracker.failure()) {
                        throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        } finally {
            tracker.cancel(true);
        }

        return globalResult;
    }

    private void mergeCellValueMapIntoMap(Map<Cell, Value> globalResult, Map<Cell, Value> partResult) {
        for (Map.Entry<Cell, Value> e : partResult.entrySet()) {
            if (!globalResult.containsKey(e.getKey())
            || globalResult.get(e.getKey()).getTimestamp() < e.getValue().getTimestamp()) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    private void mergeLatestTimestampMapIntoMap(Map<Cell, Long> globalResult, Map<Cell, Long> partResult) {
        for (Map.Entry<Cell, Long> e : partResult.entrySet()) {
            if (!globalResult.containsKey(e.getKey()) || globalResult.get(e.getKey()) < e.getValue()) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    private void mergeAllTimestampsMapIntoMap(Multimap<Cell, Long> globalResult, Multimap<Cell, Long> partResult) {
        for (Map.Entry<Cell, Long> e : partResult.entries()) {
            if (!globalResult.containsEntry(e.getKey(), e.getValue())) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    private final ExecutorService executor;

    @Override
    public void put(final String tableName, Map<Cell, byte[]> values, final long timestamp) {
        final Map<KeyValueService, Map<Cell, byte[]>> tasks = tpm.getServicesForCellsWrite(tableName, values);
        final ExecutorCompletionService<Void> writeService = new ExecutorCompletionService<Void>(
                executor);
        final QuorumTracker<Void, Cell> tracker = QuorumTracker.of(
                values.keySet(),
                quorumParameters.getWriteRequestParameters());

        // Schedule the requests
        for (final Map.Entry<KeyValueService, Map<Cell, byte[]>> e : tasks.entrySet()) {
            Future<Void> future = writeService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    e.getKey().put(tableName, e.getValue(), timestamp);
                    return null;
                }
            });
            tracker.registerRef(future, e.getValue().keySet());
        }

        // Wait until we can conclude success or failure.
        try {
            while (!tracker.finished()) {
                Future<Void> future = writeService.take();
                try {
                    future.get();
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    log.warn("Could not complete write request in table " + tableName);
                    tracker.handleFailure(future);
                    if (tracker.failure()) {
                        tracker.cancel(false);
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        }
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        final Map<KeyValueService, Multimap<Cell, Value>> tasks = tpm.getServicesForTimestampsWrite(tableName, cellValues);
        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(executor);
        final QuorumTracker<Void, Map.Entry<Cell, Value>> tracker =
                QuorumTracker.of(cellValues.entries(), quorumParameters.getWriteRequestParameters());

        for (final Map.Entry<KeyValueService, Multimap<Cell, Value>> e : tasks.entrySet()) {
            Future<Void> future = execSvc.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    e.getKey().putWithTimestamps(tableName, e.getValue());
                    return null;
                }
            });
            tracker.registerRef(future, e.getValue().entries());
        }

        try {
            while (!tracker.finished()) {
                Future<Void> future = execSvc.take();
                try {
                    future.get();
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    log.warn("Could not complete write request in table " + tableName);
                    tracker.handleFailure(future);
                    if (tracker.failure()) {
                        tracker.cancel(true);
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e.getCause());
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Idempotent
    public void delete(final String tableName, Multimap<Cell, Long> keys) {
        final Map<KeyValueService, Multimap<Cell, Long>> tasks = tpm.getServicesForDelete(tableName, keys);
        final QuorumTracker<Void, Map.Entry<Cell, Long>> tracker = QuorumTracker.of(
                keys.entries(), quorumParameters.getNoFailureRequestParameters());
        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(executor);

        for (final Map.Entry<KeyValueService, Multimap<Cell, Long>> e : tasks.entrySet()) {
            final Future<Void> future = execSvc.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    e.getKey().delete(tableName, e.getValue());
                    return null;
                }
            });
            tracker.registerRef(future, e.getValue().entries());
        }

        try {
            while (!tracker.finished()) {
                Future<Void> future = execSvc.take();
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Throwables.throwUncheckedException(e);
                } catch (ExecutionException e) {
                    log.warn("Could not complete write request in table " + tableName);
                    // This should cause tracker to immediately finish with failure.
                    assert(tracker.failure());
                    if (tracker.failure()) {
                        tracker.cancel(true);
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        }
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       RangeRequest rangeRequest,
                                                       final long timestamp) {

        final Multimap<ConsistentRingRangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(
                tableName,
                rangeRequest);

        return new PartitionedRangedIterator<Value>(services.keySet()) {
            @Override
            public RowResult<Value> computeNext() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.mergeResults(getRowIterator());
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Value>>> computeNextRange(ConsistentRingRangeRequest range) {
                Set<ClosablePeekingIterator<RowResult<Value>>> result = Sets.newHashSet();
                for (KeyValueService kvs : services.get(range)) {
                    ClosableIterator<RowResult<Value>> it = kvs.getRange(tableName, range.get(), timestamp);
                    result.add(ClosablePeekingIterator.of(it));
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
        final Multimap<ConsistentRingRangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(
                tableName,
                rangeRequest);

        return new PartitionedRangedIterator<Set<Value>>(services.keySet()) {
            @Override
            public RowResult<Set<Value>> computeNext() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.allResults(getRowIterator());
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Value>>>> computeNextRange(ConsistentRingRangeRequest range) {
                Set<ClosablePeekingIterator<RowResult<Set<Value>>>> result = Sets.newHashSet();
                for (KeyValueService kvs : services.get(range)) {
                    ClosableIterator<RowResult<Set<Value>>> it = kvs.getRangeWithHistory(tableName, range.get(), timestamp);
                    result.add(ClosablePeekingIterator.of(it));
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
        final Multimap<ConsistentRingRangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(
                tableName,
                rangeRequest);

        return new PartitionedRangedIterator<Set<Long>>(services.keySet()) {
            @Override
            public RowResult<Set<Long>> computeNext() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.allTimestamps(getRowIterator());
            }

            @Override
            protected Set<ClosablePeekingIterator<RowResult<Set<Long>>>> computeNextRange(ConsistentRingRangeRequest range) {
                Set<ClosablePeekingIterator<RowResult<Set<Long>>>> result = Sets.newHashSet();
                for (KeyValueService kvs : services.get(range)) {
                    ClosablePeekingIterator<RowResult<Set<Long>>> it =
                            ClosablePeekingIterator.of(kvs.getRangeOfTimestamps(tableName, range.get(), timestamp));
                    result.add(it);
                }
                return result;
            }
        };
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {
        Map<KeyValueService, Set<Cell>> services = tpm.getServicesForCellsRead(tableName, cells, timestamp);
        ExecutorCompletionService<Multimap<Cell, Long>> execSvc = new ExecutorCompletionService<Multimap<Cell,Long>>(executor);
        QuorumTracker<Multimap<Cell, Long>, Cell> tracker = QuorumTracker.of(cells, quorumParameters.getNoFailureRequestParameters());
        Multimap<Cell, Long> globalResult = HashMultimap.create();

        for (final Map.Entry<KeyValueService, Set<Cell>> e : services.entrySet()) {
            Future<Multimap<Cell, Long>> future = execSvc.submit(new Callable<Multimap<Cell,Long>>() {
                @Override
                public Multimap<Cell, Long> call() throws Exception {
                    return e.getKey().getAllTimestamps(tableName, cells, timestamp);
                }
            });
            tracker.registerRef(future, e.getValue());
        }

        try {
            while (!tracker.finished()) {
                Future<Multimap<Cell, Long>> future = execSvc.take();
                try {
                    Multimap<Cell, Long> kvsReslt = future.get();
                    mergeAllTimestampsMapIntoMap(globalResult, kvsReslt);
                    tracker.handleSuccess(future);
                }
                catch (ExecutionException e) {
                    tracker.handleFailure(future);
                    assert(tracker.failure());
                    if (tracker.failure()) {
                        tracker.cancel(true);
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        } finally {
            tracker.cancel(true);
        }

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

    @Override
    @Idempotent
    public void dropTable(String tableName) throws InsufficientConsistencyException {
        tpm.dropTable(tableName);
    }

    @Override
    @Idempotent
    public void createTable(String tableName, int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
        tpm.addTable(tableName, maxValueSizeInBytes);
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        return tpm.getAllTableNames();
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(String tableName) {
        return tpm.getTableMetadata(tableName);
    }

    @Override
    @Idempotent
    public void putMetadataForTable(String tableName, byte[] metadata) {
        tpm.storeTableMetadata(tableName, metadata);
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName, Set<Cell> cells) {
        Map<KeyValueService, Set<Cell>> services = tpm.getServicesForCellsWrite(tableName, cells);
        ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(executor);
        QuorumTracker<Void, Cell> tracker = QuorumTracker.of(cells, quorumParameters.getWriteRequestParameters());

        for (final Map.Entry<KeyValueService, Set<Cell>> e : services.entrySet()) {
            Future<Void> future = execSvc.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    e.getKey().addGarbageCollectionSentinelValues(tableName, e.getValue());
                    return null;
                }
            });
            tracker.registerRef(future, e.getValue());
        }

        try {
            while (!tracker.finished()) {
                Future<Void> future = execSvc.take();
                try {
                    future.get();
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    tracker.handleFailure(future);
                    if (tracker.failure()) {
                        tracker.cancel(true);
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void compactInternally(String tableName) {
        tpm.compactInternally(tableName);
    }

    @Override
    public void close() {
        tpm.close();
    }

    @Override
    public void teardown() {
        tpm.tearDown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return tpm.getDelegates();
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final String tableName, Map<Cell, Long> timestampByCell) {
        Map<Cell, Long> result = Maps.newHashMap();
        Map<KeyValueService, Map<Cell, Long>> tasks = tpm.getServicesForCellsRead(tableName, timestampByCell);
        QuorumTracker<Map<Cell, Long>, Cell> tracker = QuorumTracker.of(
                timestampByCell.keySet(), quorumParameters.getReadRequestParameters());
        ExecutorCompletionService<Map<Cell, Long>> execSvc = new ExecutorCompletionService<Map<Cell,Long>>(executor);

        for (final Map.Entry<KeyValueService, Map<Cell, Long>> e : tasks.entrySet()) {
            Future<Map<Cell, Long>> future = execSvc.submit(new Callable<Map<Cell, Long>>() {
                @Override
                public Map<Cell, Long> call() throws Exception {
                    return e.getKey().getLatestTimestamps(tableName, e.getValue());
                }
            });
            tracker.registerRef(future, e.getValue().keySet());
        }

        try {
            while (!tracker.finished()) {
                Future<Map<Cell, Long>> future = execSvc.take();
                try {
                    Map<Cell, Long> kvsResult = future.get();
                    mergeLatestTimestampMapIntoMap(result, kvsResult);
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    log.warn("Could not complete read request in table " + tableName);
                    tracker.handleFailure(future);
                    if (tracker.failure()) {
                        Throwables.rewrapAndThrowUncheckedException(e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        } finally {
            tracker.cancel(true);
        }

        return result;
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

    @Override
    @Idempotent
    public void truncateTable(String tableName) throws InsufficientConsistencyException {
        tpm.truncateTable(tableName);
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        tpm.truncateTables(tableNames);
    }

    @Override
    @Idempotent
    public void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes)
            throws InsufficientConsistencyException {
        for (Map.Entry<String, Integer> e : tableNamesToMaxValueSizeInBytes.entrySet()) {
            createTable(e.getKey(), e.getValue());
        }
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        return tpm.getTablesMetadata();
    }

    @Override
    @Idempotent
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        for (Map.Entry<String, byte[]> e : tableNameToMetadata.entrySet()) {
            putMetadataForTable(e.getKey(), e.getValue());
        }
    }

    public static PartitionedKeyValueService create(Set<? extends KeyValueService> svcPool) {
        Preconditions.checkArgument(svcPool.size() == 4);
        NavigableMap<byte[], KeyValueService> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        final byte[][] points = new byte[][] {
                new byte[] {(byte) 0x00},
                new byte[] {(byte) 0x40},
                new byte[] {(byte) 0x80},
                new byte[] {(byte) 0xC0}
        };

        int i = 0;
        for (KeyValueService kvs : svcPool) {
            ring.put(points[i++], kvs);
        }

        return create(ring);
    }

    public static PartitionedKeyValueService create(NavigableMap<byte[], KeyValueService> ring) {
        return create(ring, PTExecutors.newCachedThreadPool());
    }

    public static PartitionedKeyValueService create(NavigableMap<byte[], KeyValueService> ring, ExecutorService executor) {
        return new PartitionedKeyValueService(ring, executor);
    }

}
