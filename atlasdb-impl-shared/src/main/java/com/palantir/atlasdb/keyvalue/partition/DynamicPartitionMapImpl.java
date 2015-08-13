package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;

public class DynamicPartitionMapImpl implements DynamicPartitionMap {

    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], KeyValueServiceWithStatus> ring;
    private final Set<KeyValueService> services;

    public DynamicPartitionMapImpl(QuorumParameters quorumParameters,
                               NavigableMap<byte[], KeyValueService> ring) {
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(Maps.<byte[], KeyValueServiceWithStatus>newTreeMap(null));
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

    @Override
    public void addEndpoint(byte[] key, KeyValueService kvs, String rack) {
        Preconditions.checkArgument(!ring.containsKey(key));
        KeyValueServiceWithStatus kvsWithStatus = new JoiningKeyValueService(kvs);
        ring.put(key, kvsWithStatus);
        // TODO: Start backfill
    }

    @Override
    public void removeEndpoint(byte[] key, KeyValueService kvs, String rack) {
        Preconditions.checkArgument(ring.containsKey(key));
        LeavingKeyValueService leavingKeyValueService = new LeavingKeyValueService(ring.get(key).get());
        ring.put(key, leavingKeyValueService);
        // TODO: Start backfill
    }

    @Override
    public void syncRemovedEndpoints() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean removalInProgress() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
