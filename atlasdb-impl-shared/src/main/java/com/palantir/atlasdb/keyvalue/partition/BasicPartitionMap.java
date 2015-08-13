

package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.common.annotation.Immutable;

@Immutable @ThreadSafe public final class BasicPartitionMap implements PartitionMap {

    private final QuorumParameters quorumParameters;
    // This map is never modified
    private final CycleMap<byte[], KeyValueService> ring;
    private final ImmutableMap<KeyValueService, String> rackByKvs;
    private final ImmutableSet<KeyValueService> services;
    private final ImmutableSet<String> racks;

    // *** Construction ****************************************************************************
    private BasicPartitionMap(QuorumParameters quorumParameters,
                              NavigableMap<byte[], KeyValueService> ring,
                              Map<KeyValueService, String> rackByKvs) {
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
        this.rackByKvs = ImmutableMap.copyOf(rackByKvs);
        this.services = ImmutableSet.copyOf(ring.values());
        this.racks = ImmutableSet.copyOf(rackByKvs.values());
        Preconditions.checkArgument(quorumParameters.getReplicationFactor() <= racks.size());
    }

    public static BasicPartitionMap create(QuorumParameters quorumParameters,
                                           NavigableMap<byte[], KeyValueService> ring,
                                           Map<KeyValueService, String> rackByKvs) {
        return new BasicPartitionMap(quorumParameters, ring, rackByKvs);
    }

    public static BasicPartitionMap create(QuorumParameters quorumParameters,
                                           NavigableMap<byte[], KeyValueService> ring) {
        Map<KeyValueService, String> rackByKvs = Maps.newHashMap();
        // Assume each kvs to be in separate rack if no info is available.
        for (KeyValueService kvs : ring.values()) {
            rackByKvs.put(kvs, "" + kvs.hashCode());
        }
        return create(quorumParameters, ring, rackByKvs);
    }

    // *********************************************************************************************

    // *** Helper methods **************************************************************************
    private Set<KeyValueService> getServicesHavingRow(byte[] key) {
        Set<KeyValueService> result = Sets.newHashSet();
        Set<String> racks = Sets.newHashSet();
        byte[] point = key;
        while (result.size() < quorumParameters.getReplicationFactor()) {
            point = ring.nextKey(point);
            KeyValueService kvs = ring.get(point);
            String rack = rackByKvs.get(kvs);
            if (racks.add(rack)) {
                result.add(kvs);
            }
        }
        assert result.size() == quorumParameters.getReplicationFactor();
        return result;
    }

    private Map<KeyValueService, Set<Cell>> getServicesForCellsSet(String tableName, Set<Cell> cells) {
        Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueService> services = getServicesHavingRow(cell.getRowName());
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
                                                                         Map<Cell, ValType> cellMap) {
        Map<KeyValueService, Map<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMap.entrySet()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
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
                                                                                                Multimap<Cell, ValType> cellMultimap) {
        Map<KeyValueService, Multimap<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMultimap.entries()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
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
            result.putAll(crrr, getServicesHavingRow(rangeStart));

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
            Set<KeyValueService> services = getServicesHavingRow(row);
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
        return getServicesForCellsSet(tableName, cells);
    }

    @Override
    public Map<KeyValueService, Map<Cell, Long>> getServicesForCellsRead(String tableName,
                                                                         Map<Cell, Long> timestampByCell) {
        return getServicesForCellsMap(tableName, timestampByCell);
    }

    @Override
    public Map<KeyValueService, Map<Cell, byte[]>> getServicesForCellsWrite(String tableName,
                                                                            Map<Cell, byte[]> values) {
        return getServicesForCellsMap(tableName, values);
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                    Set<Cell> cells) {
        return getServicesForCellsSet(tableName, cells);
    }

    @Override
    public <T> Map<KeyValueService, Multimap<Cell, T>> getServicesForWrite(String tableName,
                                                                           Multimap<Cell, T> keys) {
        return getServicesForCellsMultimap(tableName, keys);
    }

    @Override
    public Set<? extends KeyValueService> getDelegates() {
        return services;
    }
}
