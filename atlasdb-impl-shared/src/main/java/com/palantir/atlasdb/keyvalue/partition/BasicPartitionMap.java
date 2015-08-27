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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonProperty("quorumParameters")
    private final QuorumParameters quorumParameters;
    // This map is never modified
    @JsonProperty("ring")
    private final CycleMap<byte[], KeyValueEndpoint> ring;
    private final ImmutableMap<KeyValueEndpoint, String> rackByKvs;
    private final ImmutableSet<KeyValueEndpoint> services;
    private final ImmutableSet<String> racks;

    @JsonCreator
    public static BasicPartitionMap deserialize(@JsonProperty("quorumParameters") QuorumParameters quorumParameters,
                                                @JsonProperty("ring") CycleMap<byte[], KeyValueEndpoint> ring) {
        return create(quorumParameters, ring);
    }

    // *** Construction ****************************************************************************
    private BasicPartitionMap(QuorumParameters quorumParameters,
                              NavigableMap<byte[], KeyValueEndpoint> ring,
                              Map<KeyValueEndpoint, String> rackByKvs) {
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
        this.rackByKvs = ImmutableMap.copyOf(rackByKvs);
        this.services = ImmutableSet.copyOf(ring.values());
        this.racks = ImmutableSet.copyOf(rackByKvs.values());
        Preconditions.checkArgument(quorumParameters.getReplicationFactor() <= racks.size());
    }

    public static BasicPartitionMap create(QuorumParameters quorumParameters,
                                           NavigableMap<byte[], KeyValueEndpoint> ring,
                                           Map<KeyValueEndpoint, String> rackByKvs) {
        return new BasicPartitionMap(quorumParameters, ring, rackByKvs);
    }

    public static BasicPartitionMap create(QuorumParameters quorumParameters,
                                           NavigableMap<byte[], KeyValueEndpoint> ring) {
        Map<KeyValueEndpoint, String> rackByKvs = Maps.newHashMap();
        // Assume each kvs to be in separate rack if no info is available.
        for (KeyValueEndpoint kvs : ring.values()) {
            rackByKvs.put(kvs, "" + kvs.hashCode());
        }
        return create(quorumParameters, ring, rackByKvs);
    }

    // *********************************************************************************************

    // *** Helper methods **************************************************************************
    private Set<KeyValueEndpoint> getServicesHavingRow(byte[] key) {
        Set<KeyValueEndpoint> result = Sets.newHashSet();
        Set<String> racks = Sets.newHashSet();
        byte[] point = key;
        while (result.size() < quorumParameters.getReplicationFactor()) {
            point = ring.nextKey(point);
            KeyValueEndpoint kvs = ring.get(point);
            String rack = rackByKvs.get(kvs);
            if (racks.add(rack)) {
                result.add(kvs);
            }
        }
        assert result.size() == quorumParameters.getReplicationFactor();
        return result;
    }

    private Map<KeyValueEndpoint, Set<Cell>> getServicesForCellsSet(String tableName, Set<Cell> cells) {
        Map<KeyValueEndpoint, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(cell.getRowName());
            for (KeyValueEndpoint kvs : services) {
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

    private <ValType> Map<KeyValueEndpoint, Map<Cell, ValType>> getServicesForCellsMap(String tableName,
                                                                         Map<Cell, ValType> cellMap) {
        Map<KeyValueEndpoint, Map<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMap.entrySet()) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueEndpoint kvs : services) {
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

    private <ValType> Map<KeyValueEndpoint, Multimap<Cell, ValType>> getServicesForCellsMultimap(String tableName,
                                                                                                Multimap<Cell, ValType> cellMultimap) {
        Map<KeyValueEndpoint, Multimap<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMultimap.entries()) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueEndpoint kvs : services) {
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

    private Map<KeyValueEndpoint, NavigableSet<byte[]>> getServicesForRowsRead(String tableName,
                                                                             Iterable<byte[]> rows) {
        Map<KeyValueEndpoint, NavigableSet<byte[]>> result = Maps.newHashMap();
        for (byte[] row : rows) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(row);
            for (KeyValueEndpoint kvs : services) {
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

    @JsonIgnore
    @Override
    public Set<? extends KeyValueService> getDelegates() {
        throw new UnsupportedOperationException();
    }

    static <K, V> Pair<K, V> ofEntry(Entry<? extends K, ? extends V> entry) {
        return Pair.<K, V>create(entry.getKey(), entry.getValue());
    }

    private static <T> void apply(final Entry<KeyValueEndpoint, ? extends T> entry, final Function<Pair<KeyValueService, T>, Void> task) {
        entry.getKey().run(new Function<KeyValueService, Void>() {
            @Override @Nullable
            public Void apply(@Nullable KeyValueService input) {
                task.apply(Pair.<KeyValueService, T>create(input, entry.getValue()));
                return null;
            }

        });
    }

    @Override
    public void runForRowsRead(String tableName,
                               Iterable<byte[]> rows,
                               final Function<Pair<KeyValueService, Iterable<byte[]>>, Void> task) {
        for (final Entry<KeyValueEndpoint, ? extends Iterable<byte[]>> e : getServicesForRowsRead(tableName, rows).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public void runForCellsRead(String tableName,
                                Set<Cell> cells,
                                Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        for (Entry<KeyValueEndpoint, Set<Cell>> e : getServicesForCellsSet(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public <T> void runForCellsRead(String tableName,
                                    Map<Cell, T> cells,
                                    Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        for (Entry<KeyValueEndpoint, Map<Cell, T>> e : getServicesForCellsMap(tableName, cells).entrySet()) {
            apply(e, task);
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
        for (Entry<KeyValueEndpoint, Multimap<Cell, T>> e : getServicesForCellsMultimap(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }
}
