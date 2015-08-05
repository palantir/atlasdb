package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
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

public final class BasicPartitionMap implements PartitionMap {

    private static final Logger log = LoggerFactory.getLogger(BasicPartitionMap.class);
    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], KeyValueService> ring;
    private final Map<KeyValueService, String> rackByKvs;
    private final Set<KeyValueService> services;
    private final Set<String> racks;

    // *** Construction ****************************************************************************
    private BasicPartitionMap(QuorumParameters quorumParameters,
                              NavigableMap<byte[], KeyValueService> ring,
                              Map<KeyValueService, String> rackByKvs) {
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
        this.rackByKvs = rackByKvs;
        this.services = ImmutableSet.<KeyValueService> builder().addAll(ring.values()).build();
        this.racks = ImmutableSet.<String> builder().addAll(rackByKvs.values()).build();

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
        for (KeyValueService kvs : ImmutableSet.<KeyValueService> builder().addAll(ring.values()).build()) {
            rackByKvs.put(kvs, new Integer(kvs.hashCode()).toString());
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
            if (!racks.contains(rack)) {
                result.add(ring.get(point));
                racks.add(rack);
            }
        }
        assert result.size() == quorumParameters.getReplicationFactor();
        return result;
    }

    // *********************************************************************************************

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
                if (rangeEnd.length == 0) {
                    rangeEnd = RangeRequests.getLastRowName();
                }
            }

            ConsistentRingRangeRequest crrr = ConsistentRingRangeRequest.of(RangeRequest.builder(
                    range.isReverse()).startRowInclusive(rangeStart).endRowExclusive(rangeEnd).build());

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
        return result;
    }

    @Override
    public Map<KeyValueService, Map<Cell, Long>> getServicesForCellsRead(String tableName,
                                                                         Map<Cell, Long> timestampByCell) {
        Map<KeyValueService, Map<Cell, Long>> result = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : timestampByCell.entrySet()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Maps.<Cell, Long> newHashMap());
                }
                assert !result.get(kvs).containsKey(e.getKey());
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsRead(String tableName, Set<Cell> cells) {
        Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueService> services = getServicesHavingRow(cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Sets.<Cell> newHashSet());
                }
                assert (result.get(kvs).contains(cell) == false);
                result.get(kvs).add(cell);
            }
        }
        return result;
    }

    @Override
    public Map<KeyValueService, Map<Cell, byte[]>> getServicesForCellsWrite(String tableName,
                                                                            Map<Cell, byte[]> values) {
        Map<KeyValueService, Map<Cell, byte[]>> result = Maps.newHashMap();
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Maps.<Cell, byte[]> newHashMap());
                }
                assert (!result.get(kvs).containsKey(e.getKey()));
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        assert result.keySet().size() >= quorumParameters.getReplicationFactor();
        return result;
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                    Set<Cell> cells) {
        return getServicesForCellsRead(tableName, cells);
    }

    public <T> Map<KeyValueService, Multimap<Cell, T>> getServicesForWrite(String tableName,
                                                                           Multimap<Cell, T> keys) {
        Map<KeyValueService, Multimap<Cell, T>> result = Maps.newHashMap();
        for (Map.Entry<Cell, T> e : keys.entries()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, HashMultimap.<Cell, T> create());
                }
                assert (!result.get(kvs).containsEntry(e.getKey(), e.getValue()));
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public Set<? extends KeyValueService> getDelegates() {
        return services;
    }
}
