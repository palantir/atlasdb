package com.palantir.atlasdb.keyvalue.partition;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;

public class DynamicPartitionMapImpl implements DynamicPartitionMap {

    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], KeyValueServiceWithStatus> ring;
    private final Set<KeyValueService> services;
    private final ExecutorService executor = PTExecutors.newSingleThreadExecutor();
    private final BlockingQueue<Future<Void>> removals = Queues.newLinkedBlockingQueue();
    private final BlockingQueue<Future<Void>> joins = Queues.newLinkedBlockingQueue();

    private <K, V1, V2> NavigableMap<K, V2> transformValues(NavigableMap<K, V1> map, Function<V1, V2> transform) {
        NavigableMap<K, V2> result = Maps.newTreeMap(map.comparator());
        for (Entry<K, V1> entry : map.entrySet()) {
            result.put(entry.getKey(), transform.apply(entry.getValue()));
        }
        return result;
    }

    public DynamicPartitionMapImpl(QuorumParameters quorumParameters,
                               NavigableMap<byte[], KeyValueService> ring) {
        Preconditions.checkArgument(ring.keySet().size() >= quorumParameters.replicationFactor);
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(transformValues(ring, new Function<KeyValueService, KeyValueServiceWithStatus>() {
            @Override @Nullable
            public KeyValueServiceWithStatus apply(@Nullable KeyValueService input) {
                return new RegularKeyValueService(input);
            }
        }));
        this.services = Sets.newHashSet(ring.values());
    }

    // *** Helper methods **************************************************************************
    private Set<KeyValueService> getServicesHavingRow(byte[] key, boolean isWrite) {
        Set<KeyValueService> result = Sets.newHashSet();
        byte[] point = key;
        int extraServices = 0; // These are included in the result set but
                               // Are not counted against the replication factor
        while (result.size() < quorumParameters.getReplicationFactor() + extraServices) {
            point = ring.nextKey(point);
            KeyValueServiceWithStatus kvs = ring.get(point);
            if (!kvs.shouldUseFor(isWrite)) {
                assert !kvs.shouldCountFor(isWrite);
                continue;
            }
            result.add(kvs.get());
            if (!kvs.shouldCountFor(isWrite)) {
                extraServices += 1;
            }
        }
        return result;
    }

    private Map<KeyValueService, Set<Cell>> getServicesForCellsSet(String tableName, Set<Cell> cells, boolean isWrite) {
        Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueService> services = getServicesHavingRow(cell.getRowName(), isWrite);
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Sets.<Cell> newHashSet());
                }
                assert result.get(kvs).contains(cell) == false;
                result.get(kvs).add(cell);
            }
        }
        assert result.keySet().size() >= quorumParameters.getReplicationFactor();
        return result;
    }

    private <ValType> Map<KeyValueService, Map<Cell, ValType>> getServicesForCellsMap(String tableName,
                                                                         Map<Cell, ValType> cellMap, boolean isWrite) {
        Map<KeyValueService, Map<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMap.entrySet()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName(), isWrite);
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Maps.<Cell, ValType> newHashMap());
                }
                assert !result.get(kvs).containsKey(e.getKey());
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        if (!cellMap.isEmpty()) {
            assert result.keySet().size() >= quorumParameters.getReplicationFactor();
        }
        return result;
    }

    private <ValType> Map<KeyValueService, Multimap<Cell, ValType>> getServicesForCellsMultimap(String tableName,
                                                                                                Multimap<Cell, ValType> cellMultimap, boolean isWrite) {
        Map<KeyValueService, Multimap<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMultimap.entries()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName(), isWrite);
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, HashMultimap.<Cell, ValType> create());
                }
                assert !result.get(kvs).containsEntry(e.getKey(), e.getValue());
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        if (!cellMultimap.isEmpty()) {
            assert result.keySet().size() >= quorumParameters.getReplicationFactor();
        }
        return result;
    }
    // *********************************************************************************************

    /**
     *  Kasowanie w trakcie realizacji:
     *      - odczyty są kierowane do endpointa, który jest kasowany
     *      - zapisy są kierowane do obydwu
     *
     *  Dodawanie w trakcie realizacji:
     *      - odczyty są kierowane do następnika
     *      - zapisy są kierowane do obydwu
     *
     *  Struktura:
     *      - Map<byte[], KeyValueServiceWithInfo> ring
     *      - class KeyValueServiceWithInfo
     *
     */

    /**
     *  Statusy endpointów:
     *      - normalny: use for read, count for read, use for write, count for write
     *      - kasowany: use for read, count for read, use for write
     *      - dodawany:                               use for write
     *
     *  Generalnie: countForX => useForX
     */

    // *** Public methods **************************************************************************
    @Override
    public Multimap<ConsistentRingRangeRequest, KeyValueService> getServicesForRangeRead(String tableName,
                                                                                         RangeRequest range) {
        if (range.isReverse()) {
            throw new UnsupportedOperationException();
        }
        Multimap<ConsistentRingRangeRequest, KeyValueService> result = LinkedHashMultimap.create();

        byte[] rangeStart = range.getStartInclusive();
        if (range.getStartInclusive().length == 0) {
            rangeStart = RangeRequests.getFirstRowName();
        }

        // Note that there is no wrapping around when traversing the circle with the key.
        // Ie. the range does not go over through "zero" of the ring.
        while (range.inRange(rangeStart)) {

            // Setup the consistent subrange
            byte[] rangeEnd = ring.higherKey(rangeStart);
            if (rangeEnd == null || !range.inRange(rangeEnd)) {
                rangeEnd = range.getEndExclusive();
            }

            ConsistentRingRangeRequest crrr = ConsistentRingRangeRequest.of(
                    range.getBuilder()
                            .startRowInclusive(rangeStart)
                            .endRowExclusive(rangeEnd)
                            .build());

            Preconditions.checkState(!crrr.get().isEmptyRange());

            // We have now the "consistent" subrange which means that
            // every service having the (inclusive) start row will also
            // have all the other rows belonging to this range.
            // No other services will have any of these rows.
            result.putAll(crrr, getServicesHavingRow(rangeStart, false));

            // Proceed with next range
            rangeStart = ring.higherKey(rangeStart);
            // We are out of ranges to consider.
            if (rangeStart == null) {
                break;
            }
        }
        return result;
    }

    @Override
    public Map<KeyValueService, NavigableSet<byte[]>> getServicesForRowsRead(String tableName,
                                                                             Iterable<byte[]> rows) {
        Map<KeyValueService, NavigableSet<byte[]>> result = Maps.newHashMap();
        for (byte[] row : rows) {
            Set<KeyValueService> services = getServicesHavingRow(row, false);
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(
                            kvs,
                            Sets.<byte[]> newTreeSet(UnsignedBytes.lexicographicalComparator()));
                }
                assert !result.get(kvs).contains(row);
                result.get(kvs).add(row);
            }
        }
        if (!Iterables.isEmpty(rows)) {
            assert result.keySet().size() >= quorumParameters.getReplicationFactor();
        }
        return result;
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsRead(String tableName, Set<Cell> cells) {
        return getServicesForCellsSet(tableName, cells, false);
    }

    @Override
    public <T> Map<KeyValueService, Map<Cell, T>> getServicesForCellsRead(String tableName,
                                                                         Map<Cell, T> timestampByCell) {
        return getServicesForCellsMap(tableName, timestampByCell, false);
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                    Set<Cell> cells) {
        return getServicesForCellsSet(tableName, cells, true);
    }

    @Override
    public <T> Map<KeyValueService, Multimap<Cell, T>> getServicesForCellsWrite(String tableName,
                                                                           Multimap<Cell, T> keys) {
        return getServicesForCellsMultimap(tableName, keys, true);
    }

    @Override
    public Set<? extends KeyValueService> getDelegates() {
        return services;
    }

    @Override
    public <T> Map<KeyValueService, Map<Cell, T>> getServicesForCellsWrite(String tableName,
                                                                           Map<Cell, T> values) {
        return getServicesForCellsMap(tableName, values, true);
    }

    // TODO: This should probably take timestamp (?)
    private void copyData(KeyValueService destination, KeyValueService source, RangeRequest rangeToCopy) {
        for (String tableName : source.getAllTableNames()) {
            Multimap<Cell, Value> cells = HashMultimap.create();
            ClosableIterator<RowResult<Value>> allRows = source.getRange(tableName, rangeToCopy, Long.MAX_VALUE);
            while (allRows.hasNext()) {
                RowResult<Value> row = allRows.next();
                for (Entry<Cell, Value> entry : row.getCells()) {
                    cells.put(entry.getKey(), entry.getValue());
                }
            }
            destination.putWithTimestamps(tableName, cells);
            allRows.close();
        }
    }

    private void deleteData(KeyValueService kvs, RangeRequest rangeToDelete) {
        for (String tableName : kvs.getAllTableNames()) {
            Multimap<Cell, Long> cells = HashMultimap.create();
            ClosableIterator<RowResult<Set<Long>>> allTimestamps = kvs.getRangeOfTimestamps(tableName, rangeToDelete, Long.MAX_VALUE);
            while (allTimestamps.hasNext()) {
                RowResult<Set<Long>> row = allTimestamps.next();
                for (Entry<Cell, Set<Long>> entry : row.getCells()) {
                    for (Long timestamp : entry.getValue()) {
                        cells.put(entry.getKey(), timestamp);
                    }
                }
            }
            kvs.delete(tableName, cells);
        }
    }

    private List<RangeRequest> getRangesOperatedByKvs(byte[] kvsKey, boolean isWrite) {
        KeyValueServiceWithStatus kvsws = Preconditions.checkNotNull(ring.get(kvsKey));
        List<RangeRequest> result = Lists.newArrayList();

        byte[] startRange = kvsKey;
        byte[] endRange = kvsKey;
        for (int i = 0, extra = 0; i < quorumParameters.getReplicationFactor() + extra; ++i) {
            startRange = ring.previousKey(startRange);
            if (!ring.get(startRange).shouldUseFor(isWrite)) {
                continue;
            }
            if (UnsignedBytes.lexicographicalComparator().compare(startRange, endRange) < 0) {
                RangeRequest range = RangeRequest.builder()
                        .startRowInclusive(startRange)
                        .endRowExclusive(ring.nextKey(startRange))
                        .build();
                result.add(range);
            } else {
                RangeRequest range1 = RangeRequest.builder()
                        .endRowExclusive(endRange)
                        .build();
                RangeRequest range2 = RangeRequest.builder()
                        .startRowInclusive(startRange)
                        .build();
                result.add(range1);
                result.add(range2);
            }
            if (!ring.get(startRange).shouldCountFor(isWrite)) {
                extra++;
            }
            endRange = startRange;
        }

        return Lists.reverse(result);
    }

    @Override
    public synchronized void addEndpoint(final byte[] key, final KeyValueService kvs, String rack) {
        Preconditions.checkArgument(!ring.containsKey(key));
        KeyValueServiceWithStatus kvsWithStatus = new JoiningKeyValueService(kvs);
        ring.put(key, kvsWithStatus);
        joins.add(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                byte[] nextKey = ring.nextKey(key);
                List<RangeRequest> ranges = getRangesOperatedByKvs(nextKey, false);
                KeyValueService sourceKvs = ring.get(nextKey).get();
                // First, copy all the ranges besides the one in which
                // the new kvs is located. All these will be operated
                // by the new kvs.
                for (int i = 0; i < ranges.size() - 1; ++i) {
                    copyData(kvs, sourceKvs, ranges.get(i));
                }

                // Now we need to split the last range into two pieces.
                // The second piece will not be operated by the joiner,
                // thus I only need to copy the first part over.
                RangeRequest lastRange = ranges.get(ranges.size() - 1);
                RangeRequest lastRange1 = lastRange.getBuilder().endRowExclusive(key).build();
//                RangeRequest lastRange2 = lastRange.getBuilder().startRowInclusive(key).build();
                copyData(kvs, sourceKvs, lastRange1);
                // Some anti-bug asserts
                assert UnsignedBytes.lexicographicalComparator().compare(lastRange.getStartInclusive(), key) < 0;
                assert UnsignedBytes.lexicographicalComparator().compare(lastRange.getEndExclusive(), key) > 0;

                // The last thing to be done is removing the farthest
                // range from the following kvss.
                byte[] keyToRemove = nextKey;
                for (int i = 0; i < ranges.size() - 1; ++i) {
                    RangeRequest rangeToRemove = ranges.get(i);
                    deleteData(ring.get(keyToRemove).get(), rangeToRemove);
                    keyToRemove = ring.nextKey(keyToRemove);
                }
                deleteData(ring.get(keyToRemove).get(), lastRange1);
                ring.put(key, new RegularKeyValueService(kvs));
                return null;
            }
        }));

        try {
            joins.take().get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public synchronized void removeEndpoint(final byte[] key, final KeyValueService kvs, String rack) {
        Preconditions.checkArgument(ring.keySet().size() > quorumParameters.getReplicationFactor());
        KeyValueServiceWithStatus original = Preconditions.checkNotNull(ring.get(key));
        LeavingKeyValueService leavingKeyValueService = new LeavingKeyValueService(original.get());
        ring.put(key, leavingKeyValueService);

        removals.add(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                List<RangeRequest> ranges = getRangesOperatedByKvs(key, true);
                byte[] dstKvsKey = ring.nextKey(key);
                for (int i = 0; i < ranges.size() - 1; ++i) {
                    copyData(ring.get(dstKvsKey).get(), kvs, ranges.get(i));
                    dstKvsKey = ring.nextKey(dstKvsKey);
                }
                RangeRequest lastRange1 = ranges.get(ranges.size() - 1).getBuilder().endRowExclusive(key).build();
                copyData(ring.get(dstKvsKey).get(), kvs, lastRange1);
                ring.remove(key);
                return null;
            }
        }));

        try {
            removals.take().get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public synchronized void syncRemovedEndpoints() {
        while (!removals.isEmpty()) {
            Futures.getUnchecked(removals.poll());
        }
    }

    @Override
    public synchronized boolean removalInProgress() {
        return !removals.isEmpty();
    }
}
