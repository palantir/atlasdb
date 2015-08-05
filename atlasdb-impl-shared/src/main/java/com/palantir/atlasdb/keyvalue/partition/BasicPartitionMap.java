package com.palantir.atlasdb.keyvalue.partition;

import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequest.Builder;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;


public final class BasicPartitionMap implements PartitionMap {

    private static final Logger log = LoggerFactory.getLogger(BasicPartitionMap.class);
    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], KeyValueService> ring;
    private final Set<KeyValueService> services;

    //*** Construction ****************************************************************************
    private BasicPartitionMap(QuorumParameters quorumParameters,
                              NavigableMap<byte[], KeyValueService> ring) {
        // Ensure there are actually enough kv stores.
        // TODO: Add rack info
        Preconditions.checkArgument(quorumParameters.getReplicationFactor() <= ring.keySet().size());
        // Careful with instruction order here
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
        this.services = Sets.newHashSet(ring.values());
        Preconditions.checkArgument(isRingValid());
    }

    /* This map CAN contain duplicate values (virtual partitions). However it is the callers responsibility
     * to not have same kvs repeated after less than rf points.
     */
    public static BasicPartitionMap create(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueService> ring) {
        return new BasicPartitionMap(quorumParameters, ring);
    }
    //*********************************************************************************************

    //*** Helper methods **************************************************************************
    // Make sure that the data is not replicated onto the original machine
    private boolean isRingValid() {
        int repf = quorumParameters.getReplicationFactor();
        ArrayList<KeyValueService> lastKvs = Lists.newArrayList();
        for (Map.Entry<byte[], KeyValueService> e : ring.entrySet()) {
            if (lastKvs.contains(e.getValue())) {
                return false;
            }
            if (lastKvs.size() == repf) {
                lastKvs.remove(0);
            }
            lastKvs.add(e.getValue());
        }
        return true;
    }

    private Set<KeyValueService> getServicesHavingRow(byte[] key) {
        Set<KeyValueService> result = Sets.newHashSet();
        byte[] point = key;
        for (int i = 0; i < quorumParameters.getReplicationFactor(); ++i) {
            point = ring.nextKey(point);
            result.add(ring.get(point));
        }
        return result;
    }
    //*********************************************************************************************

    @Override
    public Multimap<ConsistentRingRangeRequest, KeyValueService> getServicesForRangeRead(String tableName,
                                                                                         RangeRequest range) {
        if (range.isReverse()) {
            throw new UnsupportedOperationException();
        }
        Multimap<ConsistentRingRangeRequest, KeyValueService> result = LinkedHashMultimap.create();

        // This is either the original ring, or its reversed view (in case of reversed range req)
        CycleMap<byte[], KeyValueService> rangeRing = ring;
        if (range.isReverse()) {
            rangeRing = rangeRing.descendingMap();
        }

        byte[] key = range.getStartInclusive();
        if (range.getStartInclusive().length == 0) {
            key = RangeRequests.getFirstRowName();
        }

        // Note that there is no wrapping around when travering the circle with the key.
        // Wrap around can happen when gathering further kvss for a given key.
        while (range.inRange(key)) {
            Set<KeyValueService> services = Sets.newHashSet();

            // Setup the range
            Builder builder = range.isReverse() ? RangeRequest.reverseBuilder() : RangeRequest.builder();
            builder = builder.startRowInclusive(key);
            if (rangeRing.higherKey(key) != null) {
                byte[] topOfRange = rangeRing.higherKey(key);
                if (range.inRange(topOfRange)) {
                    builder = builder.endRowExclusive(topOfRange);
                } else {
                    builder = builder.endRowExclusive(range.getEndExclusive());
                }
            } else {
                builder = builder.endRowExclusive(range.getEndExclusive());
            }
            ConsistentRingRangeRequest crrr = ConsistentRingRangeRequest.of(builder.build());

            // Find kvss having this range
            byte[] kvsKey = rangeRing.nextKey(key);
            while (services.size() < quorumParameters.getReplicationFactor()) {
                // TODO: Add logic here to skip over same rack
                services.add(rangeRing.get(kvsKey));
                kvsKey = rangeRing.nextKey(kvsKey);
            }

            result.putAll(crrr, services);

            // Proceed with next range
            key = rangeRing.higherKey(key);
            // We are out of ranges to consider.
            if (key == null) {
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
                    result.put(kvs, Sets.<byte[]>newTreeSet(UnsignedBytes.lexicographicalComparator()));
                }
                assert(!result.get(kvs).contains(row));
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
                    result.put(kvs, Maps.<Cell, Long>newHashMap());
                }
                assert(result.get(kvs).containsKey(e.getKey()) == false);
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsRead(String tableName,
                                                                   Set<Cell> cells) {
        Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueService> services = getServicesHavingRow(cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Sets.<Cell>newHashSet());
                }
                assert(result.get(kvs).contains(cell) == false);
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
                    result.put(kvs, Maps.<Cell, byte[]>newHashMap());
                }
                assert(!result.get(kvs).containsKey(e.getKey()));
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                    Set<Cell> cells) {
        Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueService> services = getServicesHavingRow(cell.getRowName());
            for (KeyValueService kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Sets.<Cell>newHashSet());
                }
                assert(!result.get(kvs).contains(cell));
                result.get(kvs).add(cell);
            }
        }
        return result;
    }

    public <T> Map<KeyValueService, Multimap<Cell, T>> getServicesForWrite(String tableName,
                                                                           Multimap<Cell, T> keys) {
        Map<KeyValueService, Multimap<Cell, T>> result = Maps.newHashMap();
        for (Map.Entry<Cell, T> e : keys.entries()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueService kvs : services) {
               if (!result.containsKey(kvs)) {
                   result.put(kvs, HashMultimap.<Cell, T>create());
               }
               assert(!result.get(kvs).containsEntry(e.getKey(), e.getValue()));
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
