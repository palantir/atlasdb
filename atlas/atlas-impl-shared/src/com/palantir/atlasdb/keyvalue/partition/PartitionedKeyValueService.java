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

import javax.annotation.Nonnull;
import javax.ws.rs.NotSupportedException;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
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
import com.palantir.atlasdb.keyvalue.partition.util.PeekingIteratorComparator;
import com.palantir.atlasdb.keyvalue.partition.util.RangeComparator;
import com.palantir.atlasdb.keyvalue.partition.util.RowResultComparator;
import com.palantir.atlasdb.keyvalue.partition.util.RowResultUtil;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PartitionedKeyValueService implements KeyValueService {

    final TableAwarePartitionMapApi tpm;

    final int replicationFactor = 3;
    final int readFactor = 2;
    final int writeFactor = 2;

    private PartitionedKeyValueService(ExecutorService executor) {
        // TODO
        tpm = AllInOnePartitionMap.Create(replicationFactor, readFactor, writeFactor, 10);
    }

    private PartitionedKeyValueService() {
        this(PTExecutors.newFixedThreadPool(16, PTExecutors.newNamedThreadFactory(true)));
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

    private void deleteCell(String tableName, Cell cell, long timestamp) {
        int succWrites = 0;
        Multimap<Cell, Long> request = ArrayListMultimap.create();
        request.put(cell, timestamp);

        Set<KeyValueService> services = tpm.getServicesForWrite(tableName, cell.getRowName());
        for (KeyValueService kvs : services) {
            kvs.delete(tableName, request);
            succWrites += 1;
        }

        if (succWrites < services.size()) {
            throw new RuntimeException("Could not delete value from all services");
        }
    }

    private <K, V> void addOrCreateAndAdd(Map<K, Set<V>> map, K key, V val) {
        if (!map.containsKey(key)) {
            map.put(key, Sets.<V> newHashSet());
        }
        map.get(key).add(val);
    }

    private <K, I, J> void addOrCreateAndAdd(Map<K, Map<I, J>> map, K key, I i, J j) {
        if (!map.containsKey(key)) {
            map.put(key, Maps.<I, J> newHashMap());
        }
        map.get(key).put(i, j);
    }

    private <S, I> void whoHasWhat(Map<S, Set<I>> result, Iterable<S> services, I item) {
        Preconditions.checkNotNull(result);
        for (S service : services) {
            addOrCreateAndAdd(result, service, item);
        }
    }

    private <S, I, J> void whoHasWhat(Map<S, Map<I, J>> result, Iterable<S> services, I key, J value) {
        Preconditions.checkNotNull(result);
        for (S service : services) {
            addOrCreateAndAdd(result, service, key, value);
        }
    }

    @Nonnull
    private Map<KeyValueService, Set<byte[]>> whoHasRowsForRead(String tableName,
                                                                Iterable<byte[]> rows) {
        Map<KeyValueService, Set<byte[]>> whoHasWhat = Maps.newHashMap();
        for (byte[] row : rows) {
            whoHasWhat(whoHasWhat, tpm.getServicesForRead(tableName, row), row);
        }
        return whoHasWhat;
    }

    @Nonnull
    private Map<KeyValueService, Set<Cell>> whoHasCellsForRead(String tableName,
                                                               Iterable<Cell> cells) {
        Map<KeyValueService, Set<Cell>> whoHasWhat = Maps.newHashMap();
        for (Cell cell : cells) {
            whoHasWhat(whoHasWhat, tpm.getServicesForRead(tableName, cell.getRowName()), cell);
        }
        return whoHasWhat;
    }

    @Nonnull
    private Map<KeyValueService, Map<Cell, Long>> whoHasCellsForReads(String tableName,
                                                                      Map<Cell, Long> cellsByTimestamp) {
        Map<KeyValueService, Map<Cell, Long>> whoHasWhat = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : cellsByTimestamp.entrySet()) {
            final Cell cell = e.getKey();
            final Long timestamp = e.getValue();
            whoHasWhat(
                    whoHasWhat,
                    tpm.getServicesForRead(tableName, cell.getRowName()),
                    cell,
                    timestamp);
        }
        return whoHasWhat;
    }

    @Nonnull
    private Map<KeyValueService, Map<Cell, byte[]>> whoHasCellsForWrite(String tableName,
                                                                        Map<Cell, byte[]> cells) {
        Map<KeyValueService, Map<Cell, byte[]>> result = Maps.newHashMap();
        for (Map.Entry<Cell, byte[]> e : cells.entrySet()) {
            final Cell cell = e.getKey();
            final byte[] val = e.getValue();
            whoHasWhat(result, tpm.getServicesForWrite(tableName, cell.getRowName()), cell, val);
        }
        return result;
    }

    @Nonnull
    private Map<KeyValueService, Multimap<Cell, Value>> whoHasCellsForWrite(String tableName,
                                                                            Multimap<Cell, Value> cells) {
        Map<KeyValueService, Multimap<Cell, Value>> result = Maps.newHashMap();
        for (Map.Entry<Cell, Value> e : cells.entries()) {
            final Cell cell = e.getKey();
            final Value val = e.getValue();
            Set<KeyValueService> services = tpm.getServicesForWrite(tableName, cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, HashMultimap.<Cell, Value> create());
                }
                result.get(kvs).put(cell, val);
            }
        }
        return result;
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
            tracker.registerRef(future);
        }

        // Wait until we can conclude success or failure
        while (!tracker.finished()) {
            try {
                Future<Map<Cell, Value>> future = execSvc.take();
                Map<Cell, Value> result = future.get();
                mergeMapIntoMap(overallResult, result);
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
                mergeMapIntoMap(globalResult, result);
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

    private void mergeMapIntoMap(Map<Cell, Value> globalResult, Map<Cell, Value> partResult) {
        for (Map.Entry<Cell, Value> e : partResult.entrySet()) {
            if (shouldUpdateMapping(globalResult, e)) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    private ExecutorService executor = PTExecutors.newCachedThreadPool();

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
        throw new NotSupportedException();
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
        final TreeMultimap<RangeRequest, PeekingIterator<RowResult<Value>>> peekingIterators = TreeMultimap.create(
                RangeComparator.Instance(),
                PeekingIteratorComparator.WithComparator(RowResultComparator.Instance()));

        // Get all the (range -> iterator) mappings into the map
        for (Map.Entry<RangeRequest, KeyValueService> e : services.entries()) {
            final RangeRequest range = e.getKey();
            final KeyValueService kvs = e.getValue();
            ClosableIterator<RowResult<Value>> it = kvs.getRange(tableName, range, timestamp);
            peekingIterators.put(range, Iterators.peekingIterator(it));
        }

        return ClosableIterators.<RowResult<Value>> wrap(new AbstractIterator<RowResult<Value>>() {

            Iterator<RangeRequest> rangesIterator = peekingIterators.keySet().iterator();
            PeekingIterator<RowResult<Value>> rowsIterator = Iterators.peekingIterator(Iterators.mergeSorted(
                    peekingIterators.get(rangesIterator.next()),
                    RowResultComparator.Instance()));

            @Override
            protected RowResult<Value> computeNext() {
                while (!rowsIterator.hasNext()) {
                    if (!rangesIterator.hasNext()) {
                        return endOfData();
                    } else {
                        rowsIterator = Iterators.peekingIterator(Iterators.mergeSorted(
                                peekingIterators.get(rangesIterator.next()),
                                RowResultComparator.Instance()));
                    }
                }
                return RowResultUtil.mergeResults(rowsIterator);
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw new NotImplementedException();
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp)
            throws InsufficientConsistencyException {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp)
            throws InsufficientConsistencyException {
        throw new NotImplementedException();
    }

    @Override
    public void compactInternally(String tableName) {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        throw new NotImplementedException();
    }

    @Override
    public void teardown() {
        tpm.tearDown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        throw new NotImplementedException();
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        Map<Cell, Long> result = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : timestampByCell.entrySet()) {
            final Cell cell = e.getKey();
            final long ts = e.getValue();
            Value val = getCell(tableName, cell, ts);

            if (val != null) {
                result.put(cell, val.getTimestamp());
            }
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
        throw new NotImplementedException();
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        throw new NotImplementedException();
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

    public static PartitionedKeyValueService Create() {
        return new PartitionedKeyValueService();
    }

    public static PartitionedKeyValueService Create(ExecutorService executor) {
        return new PartitionedKeyValueService(executor);
    }

}
