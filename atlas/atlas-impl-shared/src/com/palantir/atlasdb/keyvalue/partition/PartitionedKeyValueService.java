package com.palantir.atlasdb.keyvalue.partition;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

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
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PartitionedKeyValueService implements KeyValueService {

    final TableAwarePartitionMapApi tpm;

    final int replicationFactor = 3;
    final int readFactor = 2;
    final int writeFactor = 2;

    private PartitionedKeyValueService(ExecutorService executor) {
        tpm = AllInOnePartitionMap.Create();
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

    private void putCell(String tableName, Cell cell, byte[] value, long timestamp) {
        int succWrites = 0;

        final Map<Cell, byte[]> request = Maps.newHashMap();
        request.put(cell, value);

        for (KeyValueService kvs : tpm.getServicesForWrite(tableName, cell.getRowName())) {
            kvs.put(tableName, request, timestamp);
            succWrites += 1;
        }

        if (succWrites < writeFactor) {
            throw new RuntimeException("Could not get enough writes");
        }
    }

    private void putCellUnlessExists(String tableName, Cell cell, byte[] value) {
        int succWrites = 0;

        final Map<Cell, byte[]> request = Maps.newHashMap();
        request.put(cell, value);

        for (KeyValueService kvs : tpm.getServicesForWrite(tableName, cell.getRowName())) {
            kvs.putUnlessExists(tableName, request);
            succWrites += 1;
        }

        if (succWrites < writeFactor) {
            throw new RuntimeException("Could not get enough writes");
        }
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

    @Nonnull
    private Map<KeyValueService, Set<byte[]>> whoHasRowsForRead(String tableName, Iterable<byte[]> rows) {
        Map<KeyValueService, Set<byte[]>> whoHasWhat = Maps.newHashMap();
        for (byte[] row : rows) {
            Set<KeyValueService> services = tpm.getServicesForRead(tableName, row);
            for (KeyValueService kvs : services) {
                if (!whoHasWhat.containsKey(kvs)) {
                    whoHasWhat.put(kvs, Sets.<byte[]>newHashSet());
                }
                whoHasWhat.get(kvs).add(row);
            }
        }
        return whoHasWhat;
    }

    @Nonnull
    private Map<KeyValueService, Set<Cell>> whoHasCellsForRead(String tableName, Iterable<Cell> cells) {
        Map<KeyValueService, Set<Cell>> whoHasWhat = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueService> services = tpm.getServicesForRead(tableName, cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!whoHasWhat.containsKey(kvs)) {
                    whoHasWhat.put(kvs, Sets.<Cell>newHashSet());
                }
                whoHasWhat.get(kvs).add(cell);
            }
        }
        return whoHasWhat;
    }

    @Nonnull
    private Map<KeyValueService, Map<Cell, Long>> whoHasCellsForReads(String tableName, Map<Cell, Long> cellsByTimestamp) {
        Map<KeyValueService, Map<Cell, Long>> whoHasWhat = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : cellsByTimestamp.entrySet()) {
            final Cell cell = e.getKey();
            final Long timestamp = e.getValue();
            Set<KeyValueService> services = tpm.getServicesForRead(tableName, cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!whoHasWhat.containsKey(kvs)) {
                    whoHasWhat.put(kvs, Maps.<Cell, Long>newHashMap());
                }
                whoHasWhat.get(kvs).put(cell, timestamp);
            }
        }
        return whoHasWhat;
    }

    @Nonnull
    private Map<KeyValueService, Multimap<Cell, Value>> whoHasCellsForWrite(String tableName, Multimap<Cell, Value> cells) {
        Map<KeyValueService, Multimap<Cell, Value>> result = Maps.newHashMap();
        for (Map.Entry<Cell, Value> e : cells.entries()) {
            final Cell cell = e.getKey();
            final Value val = e.getValue();
            Set<KeyValueService> services = tpm.getServicesForWrite(tableName, cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, HashMultimap.<Cell, Value>create());
                }
                result.get(kvs).put(cell, val);
            }
        }
        return result;
    }

    @Nonnull
    private Map<KeyValueService, Map<Cell, byte[]>> whoHasCellsForWrite(String tableName, Map<Cell, byte[]> cells) {
        Map<KeyValueService, Map<Cell, byte[]>> result = Maps.newHashMap();
        for (Map.Entry<Cell, byte[]> e : cells.entrySet()) {
            final Cell cell = e.getKey();
            final byte[] val = e.getValue();
            Set<KeyValueService> services = tpm.getServicesForWrite(tableName, cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Maps.<Cell, byte[]>newHashMap());
                }
                result.get(kvs).put(cell, val);
            }
        }
        return result;
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(String tableName,
                             Iterable<byte[]> rows,
                             ColumnSelection columnSelection,
                             long timestamp) {
        Preconditions.checkArgument(tableName != null);
        Preconditions.checkArgument(rows != null);
        Preconditions.checkArgument(columnSelection != null);

        final Map<Cell, Value> overallResult = Maps.newHashMap();
        final Map<Cell, Integer> numberOfReads = Maps.newHashMap();
        final Map<KeyValueService, Set<byte[]>> whoHasWhat = whoHasRowsForRead(tableName, rows);

        for (Map.Entry<KeyValueService, Set<byte[]>> e : whoHasWhat.entrySet()) {
            final KeyValueService kvs = e.getKey();
            final Iterable<byte[]> kvsRows = e.getValue();
            Map<Cell, Value> kvsResult = kvs.getRows(tableName, kvsRows, columnSelection, timestamp);
            for (Map.Entry<Cell, Value> r : kvsResult.entrySet()) {
                final Cell cell = r.getKey();
                final Value val = r.getValue();
                if (!overallResult.containsKey(cell) || overallResult.get(cell).getTimestamp() < val.getTimestamp()) {
                    overallResult.put(cell, val);
                }
                if (!numberOfReads.containsKey(cell)) {
                    numberOfReads.put(cell, 1);
                } else {
                    numberOfReads.put(cell, numberOfReads.get(cell) + 1);
                }
            }
        }

        // Remove rows that could not be retrieved from at least readFactor endpoints
        for (Iterator<Map.Entry<Cell, Integer>> it = numberOfReads.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Cell, Integer> e = it.next();
            if (e.getValue() < readFactor) {
                System.err.println("Skipping row due to not enough reads.");
                it.remove();
            }
        }

        return overallResult;
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        Map<Cell, Value> result = Maps.newHashMap();
        Map<Cell, Integer> numberOfReads = Maps.newHashMap();
        Map<KeyValueService, Map<Cell, Long>> whoHasWhat =whoHasCellsForReads(tableName, timestampByCell);
        for (Map.Entry<KeyValueService, Map<Cell, Long>> e : whoHasWhat.entrySet()) {
            final KeyValueService kvs = e.getKey();
            final Map<Cell, Long> kvsTimestampByCell = e.getValue();
            final Map<Cell, Value> kvsResult = kvs.get(tableName, kvsTimestampByCell);
            for (Map.Entry<Cell, Value> v : kvsResult.entrySet()) {
                final Cell cell = v.getKey();
                final Value val = v.getValue();
                if (!result.containsKey(cell) || result.get(cell).getTimestamp() < val.getTimestamp()) {
                    result.put(cell, val);
                }
                if (!numberOfReads.containsKey(cell)) {
                    numberOfReads.put(cell, 1);
                } else {
                    numberOfReads.put(cell, numberOfReads.get(cell) + 1);
                }
            }
        }

        for (Map.Entry<Cell, Integer> e : numberOfReads.entrySet()) {
            final Cell cell = e.getKey();
            if (e.getValue() < readFactor) {
                System.err.println("Skipping cell due to not enough reads.");
                result.remove(cell);
            }
        }
        return result;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        Map<KeyValueService, Map<Cell, byte[]>> whoHasWhat = whoHasCellsForWrite(tableName, values);
        Map<Cell, Integer> numberOfWrites = Maps.newHashMap();
        for (Map.Entry<KeyValueService, Map<Cell, byte[]>> e : whoHasWhat.entrySet()) {
            final KeyValueService kvs = e.getKey();
            final Map<Cell, byte[]> cells = e.getValue();
            kvs.put(tableName, cells, timestamp);
            for (Cell cell : cells.keySet()) {
                if (!numberOfWrites.containsKey(cell)) {
                    numberOfWrites.put(cell, 0);
                }
                numberOfWrites.put(cell, numberOfWrites.get(cell) + 1);
            }
        }
        for (Integer i : numberOfWrites.values()) {
            if (i < writeFactor) {
                // TODO: Rethrow one of the exceptions from above
                throw new RuntimeException("Could not get enough writes.");
            }
        }
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        Map<KeyValueService, Multimap<Cell, Value>> whoHasWhat = whoHasCellsForWrite(tableName, cellValues);
        Map<Value, Integer> numberOfWrites = Maps.newHashMap();
        for (Map.Entry<KeyValueService, Multimap<Cell, Value>> e : whoHasWhat.entrySet()) {
            final KeyValueService kvs = e.getKey();
            final Multimap<Cell, Value> kvsCells = e.getValue();
            kvs.putWithTimestamps(tableName, kvsCells);
            for (Value val : kvsCells.values()) {
                if (numberOfWrites.containsKey(val)) {
                    numberOfWrites.put(val, 0);
                }
                numberOfWrites.put(val, numberOfWrites.get(val) + 1);
            }
        }
        for (Integer i : numberOfWrites.values()) {
            if (i < writeFactor) {
                throw new RuntimeException("Could not get enough writes.");
            }
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            final Cell cell = e.getKey();
            final byte[] value = e.getValue();
            putCellUnlessExists(tableName, cell, value);
        }
    }

    @Override
    @Idempotent
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        for (Map.Entry<Cell, Long> e : keys.entries()) {
            final Cell cell = e.getKey();
            final long timestamp = e.getValue();
            deleteCell(tableName, cell, timestamp);
        }
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                final RangeRequest rangeRequest,
                                                final long timestamp) {

        final Multimap<RangeRequest, KeyValueService> services = tpm.getServicesForRangeRead(tableName, rangeRequest);
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

        return ClosableIterators.<RowResult<Value>>wrap(new AbstractIterator<RowResult<Value>>() {

            Iterator<RangeRequest> rangesIterator = peekingIterators.keySet().iterator();
            PeekingIterator<RowResult<Value>> rowsIterator = Iterators.peekingIterator(
                    Iterators.mergeSorted(peekingIterators.get(
                            rangesIterator.next()),
                            RowResultComparator.Instance()));

            @Override
            protected RowResult<Value> computeNext() {
                while (!rowsIterator.hasNext()) {
                    if (!rangesIterator.hasNext()) {
                        return endOfData();
                    } else {
                        rowsIterator = Iterators.peekingIterator(
                                Iterators.mergeSorted(peekingIterators.get(
                                    rangesIterator.next()),
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
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
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
