package com.palantir.atlasdb.keyvalue.partition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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
import com.palantir.atlasdb.keyvalue.partition.util.RangeComparator;
import com.palantir.atlasdb.keyvalue.partition.util.RowResultUtil;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class PartitionedKeyValueService implements KeyValueService {

    final TableAwarePartitionMapApi tpm;

    final int replicationFactor = 3;
    final int readFactor = 2;
    final int writeFactor = 2;

    private PartitionedKeyValueService(ExecutorService executor) {
        // TODO
        tpm = AllInOnePartitionMap.Create(replicationFactor, readFactor, writeFactor, 10);
        this.executor = executor;
    }

    private PartitionedKeyValueService() {
        this(PTExecutors.newCachedThreadPool());
        //this(PTExecutors.newFixedThreadPool(16, PTExecutors.newNamedThreadFactory(true)));
    }

    @Override
    public void initializeFromFreshInstance() {
        // TODO
    }

    private Value getCell(String tableName, Cell cell, long timestamp) {
        int succReads = 0;
        Value newestVal = null;
        final Map<Cell, Long> request = Maps.newHashMap();
        request.put(cell, timestamp);

        for (KeyValueService kvs : tpm.getServicesForRead(tableName, cell.getRowName())) {
            Value result = kvs.get(tableName, request).get(cell);
            if (newestVal == null || newestVal.getTimestamp() < result.getTimestamp()) {
                newestVal = result;
            }
            succReads += 1;
        }

        if (succReads < readFactor) {
            throw new RuntimeException("Could not get enough reads.");
        }

        return newestVal;
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
        final Map<KeyValueService, Iterable<byte[]>> tasks = null;
        final RowQuorumTracker<Future<Map<Cell, Value>>> tracker = RowQuorumTracker.of(
                rows,
                replicationFactor,
                readFactor);

        // Schedule tasks for execution
        for (final Map.Entry<KeyValueService, Iterable<byte[]>> e : tasks.entrySet()) {
            Future<Map<Cell, Value>> future = execSvc.submit(new Callable<Map<Cell, Value>>() {
                @Override
                public Map<Cell, Value> call() throws Exception {
                    return e.getKey().getRows(tableName, e.getValue(), columnSelection, timestamp);
                }
            });
            tracker.registerRef(future, e.getValue());
        }

        // Wait until we can conclude success or failure
        while (!tracker.finished()) {
            try {
                Future<Map<Cell, Value>> future = execSvc.take();
                Map<Cell, Value> result = future.get();
                mergeCellValueMapIntoMap(overallResult, result);
                tracker.handleSuccess(future);
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                System.err.println("Could not complete getRow request.");
            }
        }

        if (tracker.failure()) {
            throw new RuntimeException("Could not get enough reads.");
        }

        return overallResult;
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, Map<Cell, Long> timestampByCell) {
        Map<KeyValueService, Map<Cell, Long>> tasks = null;
        ExecutorCompletionService<Map<Cell, Value>> execSvc = new ExecutorCompletionService<Map<Cell, Value>>(
                executor);
        QuorumTracker<Future<Map<Cell, Value>>, Cell> tracker = QuorumTracker.of(
                timestampByCell.keySet(),
                replicationFactor,
                readFactor);
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
        while (!tracker.finished()) {
            Future<Map<Cell, Value>> future = null;
            try {
                future = execSvc.take();
                Map<Cell, Value> result = future.get();
                mergeCellValueMapIntoMap(globalResult, result);
                tracker.handleSuccess(future);
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                System.err.println("Could not complete a get request");
                assert (future != null);
                tracker.handleFailure(future);
            }
        }

        return globalResult;
    }

    private boolean shouldUpdateMapping(Map<Cell, Value> map, Map.Entry<Cell, Value> newEntry) {
        return !map.containsKey(newEntry.getKey())
                || map.get(newEntry.getKey()).getTimestamp() < newEntry.getValue().getTimestamp();
    }

    private void mergeCellValueMapIntoMap(Map<Cell, Value> globalResult, Map<Cell, Value> partResult) {
        for (Map.Entry<Cell, Value> e : partResult.entrySet()) {
            if (shouldUpdateMapping(globalResult, e)) {
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

    private final ExecutorService executor;

    @Override
    public void put(final String tableName, Map<Cell, byte[]> values, final long timestamp) {
        final Map<KeyValueService, Map<Cell, byte[]>> tasks = null;
        final ExecutorCompletionService<Void> writeService = new ExecutorCompletionService<Void>(
                executor);
        final QuorumTracker<Future<Void>, Cell> tracker = QuorumTracker.of(
                values.keySet(),
                replicationFactor,
                writeFactor);

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
        while (!tracker.finished()) {
            Future<Void> future = null;
            try {
                future = writeService.take();
                future.get();
                tracker.handleSuccess(future);
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                System.err.println("Write failed: " + e);
                assert (future != null);
                tracker.handleFailure(future);
            }
        }

        if (tracker.failure()) {
            throw new RuntimeException("Could not get enough writes.");
        }
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        final Map<KeyValueService, Multimap<Cell, Value>> tasks = null;
        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<Void>(executor);
        final QuorumTracker<Future<Void>, Map.Entry<Cell, Value>> tracker =
                QuorumTracker.of(cellValues.entries(), replicationFactor, writeFactor);

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

        while (!tracker.finished()) {
            Future<Void> future = null;
            try {
                future = execSvc.take();
                future.get();
                tracker.handleSuccess(future);
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            } catch (ExecutionException e1) {
                System.err.println("getRows failed.");
                tracker.handleFailure(future);
            }
        }

        if (tracker.failure()) {
            throw new RuntimeException("Could not get enough writes.");
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
        final Map<KeyValueService, Multimap<Cell, Long>> tasks = null;
        final QuorumTracker<Future<Void>, Map.Entry<Cell, Long>> tracker = QuorumTracker.of(keys.entries(), replicationFactor, replicationFactor);
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

        while (!tracker.finished()) {
            try {
                Future<Void> future = execSvc.take();
                future.get();
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                System.err.println("Error deleting item.");
                // This should cause tracker to immediately finish with failure.
            }
        }

        if (tracker.failure()) {
            throw new RuntimeException("Could not get enough writes for delete");
        }
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {

        final Multimap<RangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(
                tableName,
                rangeRequest);
        final ListMultimap<RangeRequest, ClosablePeekingIterator<RowResult<Value>>> rangeIterators = Multimaps.newListMultimap(
                new TreeMap<RangeRequest, Collection<ClosablePeekingIterator<RowResult<Value>>>>(RangeComparator.Instance()),
                new Supplier<List<ClosablePeekingIterator<RowResult<Value>>>>() {
                    @Override
                    public List<ClosablePeekingIterator<RowResult<Value>>> get() {
                        return Lists.newArrayList();
                    }
                });

        // Open a set of iterators for each sub-range of the ring.
        for (Map.Entry<RangeRequest, KeyValueService> e : services.entries()) {
            final RangeRequest range = e.getKey();
            final KeyValueService kvs = e.getValue();
            ClosableIterator<RowResult<Value>> it = kvs.getRange(tableName, range, timestamp);
            rangeIterators.put(range, ClosablePeekingIterator.of(it));
        }

        return new PartitionedRangedIterator<Value>(rangeIterators) {
            @Override
            public RowResult<Value> next() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.mergeResults(rowIterator);
            }
        };
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        final Multimap<RangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(
                tableName,
                rangeRequest);
        final ListMultimap<RangeRequest, ClosablePeekingIterator<RowResult<Set<Value>>>> rangeIterators = Multimaps.newListMultimap(
                new TreeMap<RangeRequest, Collection<ClosablePeekingIterator<RowResult<Set<Value>>>>>(),
                new Supplier<List<ClosablePeekingIterator<RowResult<Set<Value>>>>>() {
                    @Override
                    public List<ClosablePeekingIterator<RowResult<Set<Value>>>> get() {
                        return Lists.newArrayList();
                    }
                });

        // Open a set of iterators for each sub-range of the ring.
        for (Map.Entry<RangeRequest, KeyValueService> e : services.entries()) {
            final RangeRequest range = e.getKey();
            final KeyValueService kvs = e.getValue();
            ClosableIterator<RowResult<Set<Value>>> it = kvs.getRangeWithHistory(tableName, range, timestamp);
            rangeIterators.put(range, ClosablePeekingIterator.of(it));
        }

        return new PartitionedRangedIterator<Set<Value>>(rangeIterators) {
            @Override
            public RowResult<Set<Value>> next() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.allResults(rowIterator);
            }
        };
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp)
            throws InsufficientConsistencyException {
        final Multimap<RangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(
                tableName,
                rangeRequest);
        final ListMultimap<RangeRequest, ClosablePeekingIterator<RowResult<Set<Long>>>> rangeIterators = Multimaps.newListMultimap(
                new TreeMap<RangeRequest, Collection<ClosablePeekingIterator<RowResult<Set<Long>>>>>(),
                new Supplier<List<ClosablePeekingIterator<RowResult<Set<Long>>>>>() {
                    @Override
                    public List<ClosablePeekingIterator<RowResult<Set<Long>>>> get() {
                        return Lists.newArrayList();
                    }
                });

        // Open a set of iterators for each sub-range of the ring.
        for (Map.Entry<RangeRequest, KeyValueService> e : services.entries()) {
            final RangeRequest range = e.getKey();
            final KeyValueService kvs = e.getValue();
            ClosableIterator<RowResult<Set<Long>>> it = kvs.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
            rangeIterators.put(range, ClosablePeekingIterator.of(it));
        }

        return new PartitionedRangedIterator<Set<Long>>(rangeIterators) {
            @Override
            public RowResult<Set<Long>> next() {
                Preconditions.checkState(hasNext());
                return RowResultUtil.allTimestamps(rowIterator);
            }
        };
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
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp)
            throws InsufficientConsistencyException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compactInternally(String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void teardown() {
        tpm.tearDown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final String tableName, Map<Cell, Long> timestampByCell) {
        Map<Cell, Long> result = Maps.newHashMap();
        Map<KeyValueService, Map<Cell, Long>> tasks = null;
        QuorumTracker<Future<Map<Cell, Long>>, Cell> tracker = QuorumTracker.of(timestampByCell.keySet(), replicationFactor, readFactor);
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

        while (!tracker.finished()) {
            try {
                Future<Map<Cell, Long>> future = execSvc.take();
                Map<Cell, Long> kvsResult = future.get();
                mergeLatestTimestampMapIntoMap(result, kvsResult);
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                System.err.println("Could not get latest timestamp read");
            }
        }

        if (tracker.failure()) {
            throw new RuntimeException("Could not get enough reads");
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
        throw new UnsupportedOperationException();
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        throw new UnsupportedOperationException();
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

    public static PartitionedKeyValueService create() {
        return new PartitionedKeyValueService();
    }

    public static PartitionedKeyValueService create(ExecutorService executor) {
        return new PartitionedKeyValueService(executor);
    }

}
