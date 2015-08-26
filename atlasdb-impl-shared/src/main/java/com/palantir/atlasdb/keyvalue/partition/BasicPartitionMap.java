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

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;
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
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.CycleMap;
import com.palantir.common.annotation.Immutable;
import com.palantir.util.Pair;

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
    public Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> getServicesForRangeRead(String tableName,
                                                                                         RangeRequest range) {
        if (range.isReverse()) {
            throw new UnsupportedOperationException();
        }
        Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> result = LinkedHashMultimap.create();

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
            for (KeyValueService kvs : getServicesHavingRow(rangeStart)) {
                result.put(crrr, new SimpleKeyValueEndpoint(kvs));
            }

            // Proceed with next range
            rangeStart = ring.higherKey(rangeStart);
            // We are out of ranges to consider.
            if (rangeStart == null) {
                break;
            }
        }
        return result;
    }

    private Map<KeyValueService, NavigableSet<byte[]>> getServicesForRowsRead(String tableName,
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
    public Set<? extends KeyValueService> getDelegates() {
        return services;
    }

    static <K, V> Pair<K, V> ofEntry(Entry<? extends K, ? extends V> entry) {
        return Pair.<K, V>create(entry.getKey(), entry.getValue());
    }

    @Override
    public void runForRowsRead(String tableName,
                               Iterable<byte[]> rows,
                               Function<Pair<KeyValueService, Iterable<byte[]>>, Void> task) {
        for (Entry<KeyValueService, NavigableSet<byte[]>> e : getServicesForRowsRead(tableName, rows).entrySet()) {
            task.apply(BasicPartitionMap.<KeyValueService, Iterable<byte[]>>ofEntry(e));
        }
    }

    @Override
    public void runForCellsRead(String tableName,
                                Set<Cell> cells,
                                Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        for (Entry<KeyValueService, Set<Cell>> e : getServicesForCellsSet(tableName, cells).entrySet()) {
            task.apply(ofEntry(e));
        }
    }

    @Override
    public <T> void runForCellsRead(String tableName,
                                    Map<Cell, T> cells,
                                    Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        for (Entry<KeyValueService, Map<Cell, T>> e : getServicesForCellsMap(tableName, cells).entrySet()) {
            task.apply(ofEntry(e));
        }
    }

    @Override
    public void runForCellsWrite(String tableName,
                                 Set<Cell> cells,
                                 Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        runForCellsRead(tableName, cells, task);
    }

    @Override
    public <T> void runForCellsWrite(String tableName,
                                     Map<Cell, T> cells,
                                     Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        runForCellsRead(tableName, cells, task);
    }

    @Override
    public <T> void runForCellsWrite(String tableName,
                                     Multimap<Cell, T> cells,
                                     Function<Pair<KeyValueService, Multimap<Cell, T>>, Void> task) {
        for (Entry<KeyValueService, Multimap<Cell, T>> e : getServicesForCellsMultimap(tableName, cells).entrySet()) {
            task.apply(ofEntry(e));
        }
    }
}
