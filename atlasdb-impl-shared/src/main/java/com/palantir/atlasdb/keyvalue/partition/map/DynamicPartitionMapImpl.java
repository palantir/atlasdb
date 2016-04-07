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
package com.palantir.atlasdb.keyvalue.partition.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters.QuorumRequestParameters;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithJoiningStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithLeavingStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithNormalStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithStatus;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.CycleMap;
import com.palantir.atlasdb.keyvalue.partition.util.EndpointRequestExecutor;
import com.palantir.atlasdb.keyvalue.partition.util.EndpointRequestExecutor.EndpointRequestCompletionService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Pair;

/**
 * Removal in progress:
 * - direct the reads to the endpoint that is being deleted
 * - direct the writes to the endpoint that is being deleted
 *   and to the next endpoint
 *
 * Addition in progress:
 * - direct the reads to the next endpoint
 * - direct the writes to the endpoint that is being added
 *   and to the next endpoint
 *
 * Summary:
 *
 *  status    | use for read | count for read | use for write | count for write
 *  ----------|--------------|----------------|---------------|----------------
 *  normal    | X            | X              | X             | X
 *  leaving   | X            | X              | X             |
 *  joining   |              |                | X             |
 *
 * Explanation:
 * - do not count for read means: read and use the data but do not increment the
 *   counter of endpoints used ie. use one more endpoint from the ring than you would
 *   usually do to complete the operation
 * - do not count for write means: write the data but do not increment the counter
 *   of endpoints used ie. use one more endpoint from the ring than you would usually
 *   do to complete the operation
 *
 * Note: after reading from enough endpoints you shall not proceed to check if the
 *   next endpoint is to be counted for the operation. Example: qp=(3,2,2) and the 3
 *   higher endpoints at your key have normal status. You shall only use these 3 even
 *   if the 4-th one should not be counted for your operation!
 *
 * Note (sanity check): countForX implies useForX.
 *
 * @see EndpointWithStatus
 * @see EndpointWithNormalStatus
 * @see EndpointWithJoiningStatus
 * @see EndpointWithLeavingStatus
 *
 *
 * Sample partition map ring:
 *  A - 3
 *  B - 5
 *  C - 8
 *  D - 10
 *  E - 12
 *
 *
 * removeEndpoint scenario:
 *   - Change endpoint status from regular to leaving.
 *   - Get the ranges operated by this kvs for read (because I will be reading
 *     from the kvs and writing to other kvss). Note that the kind of operation
 *     (read/write) is not relevant in the current impl since it supports at most
 *     one operation running at a time.
 *   - Copy the farthest range to the first higher KVS.
 *   - Copy the second-farthest range to the second higher KVS.
 *   - etc...
 *   - Remove the KVS completely from the ring.
 *
 * addEndpoint scenario:
 *   - Insert as endpoint with joining status.
 *   - Get ranges operated by this KVS for write (because I will be reading
 *     from other kvss and writing to this one). Note that the kind of operation
 *     (read/write) is not relevant in the current impl since it supports at most
 *     one operation running at a time.
 *   - Copy all the ranges but the highest one from the first higher KVS.
 *     TODO: high availability.
 *   - Copy the part of the highest range that is below the new KVS.
 *   - Change endpoint status to regular.
 *   - Remove the farthest range from the first higher KVS.
 *   - Remove the second-farthest range from the second higher KVS.
 *   - etc...
 *
 *
 * Jackson notice: This class has custom serializer and deserializer.
 *
 */
public class DynamicPartitionMapImpl implements DynamicPartitionMap {

    private static final Logger log = LoggerFactory.getLogger(DynamicPartitionMapImpl.class);
    private static final int MAX_VALUE_SIZE = 1024 * 1024 * 1024;

    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], EndpointWithStatus> ring;
    private final MutableLong version = new MutableLong(0L);

    private transient final Set<KeyValueService> delegates;
    private transient final ExecutorService executor;

    private transient final Supplier<Long> versionSupplier = new Supplier<Long>() {
        @Override
        public Long get() {
            return version.longValue();
        }
    };

    @GuardedBy("this")
    private long operationsInProgress;

    /*** Creation ********************************************************************************/
    /**
     * This is used for deserialization.
     *
     * @param quorumParameters
     * @param ring
     */
    private DynamicPartitionMapImpl(QuorumParameters quorumParameters,
            CycleMap<byte[], EndpointWithStatus> ring, long version,
            long operationsInProgress, ExecutorService executor) {

        this.quorumParameters = quorumParameters;
        this.version.setValue(version);
        this.operationsInProgress = operationsInProgress;
        this.executor = executor;

        this.ring = buildRing(ring);
        Preconditions.checkArgument(numOfRacks() >= quorumParameters.getReplicationFactor(),
                "Cannot have less racks than replication factor.");

        this.delegates = Sets.newHashSet();

        for (EndpointWithStatus kve : this.ring.values()) {
            delegates.add(kve.get().keyValueService());
        }
    }

    private DynamicPartitionMapImpl(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueEndpoint> ring, ExecutorService executor) {
        this(quorumParameters, toRing(ring), 0L, 0, executor);
    }

    public static DynamicPartitionMapImpl create(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueEndpoint> ring, ExecutorService executor) {
        return new DynamicPartitionMapImpl(quorumParameters, ring, executor);
    }

    @Override
    public synchronized void pushMapToEndpoints() {
        EndpointRequestCompletionService<Void> execSvc = EndpointRequestExecutor.newService(executor);
        Set<Future<Void>> futures = Sets.newHashSet();

        for (final EndpointWithStatus kve : ImmutableSet.copyOf(ring.values())) {
            futures.add(execSvc.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    kve.get().partitionMapService().updateMap(DynamicPartitionMapImpl.this);
                    return null;
                }
            }, kve.get().keyValueService()));
        }

        while (!futures.isEmpty()) {
            try {
                Future<?> future = execSvc.take();
                future.get();
                futures.remove(future);
            } catch (InterruptedException | ExecutionException e) {
                Throwables.throwUncheckedException(e);
            }
        }
    }

    /*** Creation helpers ***/
    /**
     * Supply the version of this partition map to all endpoints in the ring.
     *
     * @param ring
     * @return The same object as supplied ie. <code>ring</code>.
     */
    private <T extends Map<byte[], EndpointWithStatus>> T buildRing(T ring) {
        for (EndpointWithStatus e : ring.values()) {
            e.get().registerPartitionMapVersion(versionSupplier);
        }
        return ring;
    }

    /**
     * Convert bare endpoints to EndpointsWithNormalStatus.
     *
     * @param map
     * @return
     */
    private static CycleMap<byte[], EndpointWithStatus> toRing(NavigableMap<byte[], KeyValueEndpoint> map) {
        NavigableMap<byte[], EndpointWithStatus> transformedMap = Maps.transformValues(map, new Function<KeyValueEndpoint, EndpointWithStatus>() {
            @Override
            public EndpointWithStatus apply(@Nullable KeyValueEndpoint input) {
                return new EndpointWithNormalStatus(input);
            }
        });
        // Make a mutable copy of the immutable result.
        return CycleMap.wrap(Maps.newTreeMap(transformedMap));
    }

    /** Helper methods ***************************************************************************/
    // This is the METHOD
    private Set<KeyValueEndpoint> getServicesHavingRow(byte[] key, boolean isWrite) {
        Set<KeyValueEndpoint> result = Sets.newHashSet();
        Set<String> racksToBeExcluded = Sets.newHashSet();

        byte[] point = key;
        int extraServices = 0; // These are included in the result set but
                               // Are not counted against the replication factor
        while (result.size() < quorumParameters.getReplicationFactor() + extraServices) {
            point = ring.nextKey(point);
            EndpointWithStatus kvs = ring.get(point);
            KeyValueEndpoint kve = kvs.get();

            if (!kvs.shouldUseFor(isWrite, racksToBeExcluded)) {
                assert !kvs.shouldCountFor(isWrite, racksToBeExcluded);
                continue;
            }

            boolean added = result.add(kve);
            assert added;

            if (!kvs.shouldCountFor(isWrite, racksToBeExcluded)) {
                extraServices += 1;
            } else {
                // Do not use more than one endpoint from given rack
                // Exception: "not counted" endpoint that is to be
                // treated as if it did not exist (because it will
                // be removed soon, but still needs to receive writes
                // as it is being used for reads).
                racksToBeExcluded.add(kve.rack());
            }
        }
        return result;
    }

    private int numOfRacks() {
        return numOfRacksWithoutEndpoint(null);
    }

    /**
     * This is the number of racks that will be in the ring
     * after the endpoint at <tt>key</tt> will be removed.
     *
     * Useful for checking that after removing an endpoint
     * the ring still will be valid (before doing the actual
     * removal).
     *
     * @param key
     * @return
     */
    private int numOfRacksWithoutEndpoint(@Nullable byte[] key) {
        final Set<String> racks = Sets.newHashSet();
        for (Entry<byte[], EndpointWithStatus> entry : ring.entrySet()) {
            if (!Arrays.equals(key, entry.getKey())) {
                racks.add(entry.getValue().get().rack());
            }
        }
        return racks.size();
    }

    private Map<KeyValueEndpoint, Set<Cell>> getServicesForCellsSet(String tableName, Set<Cell> cells, boolean isWrite) {
        Map<KeyValueEndpoint, Set<Cell>> result = Maps.newHashMap();
        for (Cell cell : cells) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(cell.getRowName(), isWrite);
            for (KeyValueEndpoint kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Sets.<Cell> newHashSet());
                }
                assert result.get(kvs).contains(cell) == false;
                result.get(kvs).add(cell);
            }
        }
        if (!cells.isEmpty()) {
            assert result.keySet().size() >= quorumParameters.getReplicationFactor();
        }
        return result;
    }

    private <ValType> Map<KeyValueEndpoint, Map<Cell, ValType>> getServicesForCellsMap(String tableName,
                                                                         Map<Cell, ValType> cellMap, boolean isWrite) {
        Map<KeyValueEndpoint, Map<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMap.entrySet()) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(e.getKey().getRowName(), isWrite);
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
                                                                                                Multimap<Cell, ValType> cellMultimap, boolean isWrite) {
        Map<KeyValueEndpoint, Multimap<Cell, ValType>> result = Maps.newHashMap();
        for (Map.Entry<Cell, ValType> e : cellMultimap.entries()) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(e.getKey().getRowName(), isWrite);
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

    private Map<KeyValueEndpoint, NavigableSet<byte[]>> getServicesForRowsRead(String tableName,
                                                                             Iterable<byte[]> rows) {
        Map<KeyValueEndpoint, NavigableSet<byte[]>> result = Maps.newHashMap();
        for (byte[] row : rows) {
            Set<KeyValueEndpoint> services = getServicesHavingRow(row, false);
            for (KeyValueEndpoint kvs : services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, Sets.<byte[]>newTreeSet(UnsignedBytes.lexicographicalComparator()));
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

    private static <T> void apply(final Entry<KeyValueEndpoint, ? extends T> entry, final Function<Pair<KeyValueService, T>, Void> task) {
        task.apply(Pair.<KeyValueService, T>create(entry.getKey().keyValueService(), entry.getValue()));
    }

    @Override
    public void runForRowsRead(String tableName, Iterable<byte[]> rows,
                               final Function<Pair<KeyValueService, Iterable<byte[]>>, Void> task) {
        for (final Entry<KeyValueEndpoint, NavigableSet<byte[]>> e : getServicesForRowsRead(tableName, rows).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public void runForCellsRead(String tableName, Set<Cell> cells,
                                final Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Set<Cell>> e : getServicesForCellsSet(tableName, cells, false).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public <T> void runForCellsRead(String tableName, Map<Cell, T> cells,
                                    final Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Map<Cell, T>> e : getServicesForCellsMap(tableName, cells, false).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public void runForCellsWrite(String tableName, Set<Cell> cells,
                                 final Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Set<Cell>> e : getServicesForCellsSet(tableName, cells, true).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public <T> void runForCellsWrite(String tableName, Multimap<Cell, T> cells,
                                     final Function<Pair<KeyValueService, Multimap<Cell, T>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Multimap<Cell, T>> e : getServicesForCellsMultimap(tableName, cells, true).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public <T> void runForCellsWrite(String tableName, Map<Cell, T> cells,
                                     Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        for (Entry<KeyValueEndpoint, Map<Cell, T>> e : getServicesForCellsMap(tableName, cells, true).entrySet()) {
            apply(e, task);
        }
    }

    @Override
    public Set<? extends KeyValueService> getDelegates() {
        return delegates;
    }

    /**
     * Copies rows within the specified range from all the tables.
     * @param destKve
     * @param srcKve
     * @param rangeToCopy
     */
    private void copyData(KeyValueService destKvs, RangeRequest rangeToCopy) {
        ImmutableList<PartitionMapService> mapServices = ImmutableList.<PartitionMapService> of(InMemoryPartitionMapService.create(this));
        PartitionedKeyValueService pkvs = PartitionedKeyValueService.create(quorumParameters, mapServices);
        for (String tableName : pkvs.getAllTableNames()) {
            // TODO: getRangeOfTimestamps?
            try (ClosableIterator<RowResult<Set<Value>>> allRows = pkvs
                    .getRangeWithHistory(tableName, rangeToCopy,
                            Long.MAX_VALUE)) {
                while (allRows.hasNext()) {
                    RowResult<Set<Value>> row = allRows.next();
                    for (Entry<Cell, Set<Value>> cell : row.getCells()) {

                        Multimap<Cell, Value> rowMap = HashMultimap.create();
                        rowMap.putAll(cell.getKey(), cell.getValue());

                        Multimap<Cell, Long> rowTsMap = HashMultimap.create();
                        for (Entry<Cell, Value> entry : rowMap.entries()) {
                            rowTsMap.put(entry.getKey(), entry.getValue().getTimestamp());
                        }

                        destKvs.putWithTimestamps(tableName, rowMap);
                    }
                }
            }
        }
    }

    /**
     * Deletes rows within the specified range from all the tables.
     * @param kve
     * @param rangeToDelete
     */
    private void deleteData(KeyValueService kvs, RangeRequest rangeToDelete) {
        for (String tableName : kvs.getAllTableNames()) {

            Multimap<Cell, Long> cells = HashMultimap.create();

            try (ClosableIterator<RowResult<Set<Long>>> allTimestamps =
                    kvs.getRangeOfTimestamps(tableName, rangeToDelete, Long.MAX_VALUE)) {

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
    }

    /**
     * Returns ranges that should be stored and/or read from the given kvs.
     * It is intended for use when adding and/or removing endpoints.
     *
     * @param kveKey Consider endpoint at this key.
     * @param isWrite Are we looking for write or read access?
     * @return Ranges in order. The first element is the farthest range.
     */
    private List<RangeRequest> getRangesOperatedByKvs(byte[] kveKey, boolean isWrite) {
        Preconditions.checkNotNull(ring.get(kveKey));
        List<RangeRequest> result = Lists.newArrayList();

        byte[] startRange = kveKey;
        byte[] endRange = kveKey;
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

    /**
     *
     * This will set the status of given endpoint to joining and push the updated map to
     * "crucial" endpoints. This means that if this operation succeeds, you can safely call
     * {@link #promoteAddedEndpoint(byte[])}.
     * <p>
     * It is recommended that you push the map to all endpoints using {@link #pushMapToEndpoints()}
     * after calling this function.
     * <p>
     * If the function fails, the local map will be reverted to previous state. It is possible that
     * some remote endpoints will have received the new version of map and they will not be reverted.
     * You should retry this function until it succeeds.
     * <p>
     * Note that this implementation supports at most one addEndpoint or removeEndpoint operation at
     * a time. This method will return <code>false</code> if such operation is already in progress.
     *
     * @param key
     * @param kve
     * @param rack
     * @return <code>true</code> if the operation has been accepted for execution, <code>false</code> otherwise.
     * @throws RuntimeException if an update of a crucial endpoint fails.
     */
    @Override
    public synchronized boolean addEndpoint(final byte[] key, final KeyValueEndpoint kve) {
        // Sanity checks
        Preconditions.checkArgument(!ring.containsKey(key));

        if (operationsInProgress != 0) {
            return false;
        }

        // First push current map version so that we can actually re-create tables etc.
        kve.partitionMapService().updateMap(this);
        kve.registerPartitionMapVersion(versionSupplier);

        ImmutableList<PartitionMapService> mapServices = ImmutableList.<PartitionMapService> of(InMemoryPartitionMapService.create(this));
        PartitionedKeyValueService pkvs = PartitionedKeyValueService.create(quorumParameters, mapServices);
        for (String tableName : pkvs.getAllTableNames()) {
            byte[] metadata = kve.keyValueService().getMetadataForTable(tableName);
            kve.keyValueService().createTable(tableName, metadata);
        }

        ring.put(key, new EndpointWithJoiningStatus(kve));
        version.increment();
        operationsInProgress = 1;

        // Push the map to crucial endpoints
        try {
            ring.get(key).get().partitionMapService().updateMap(this);
            byte[] otherKey = key;
            for (int i = 0; i < quorumParameters.getReplicationFactor(); ++i) {
                otherKey = ring.nextKey(otherKey);
                ring.get(otherKey).get().partitionMapService().updateMap(this);
            }
        } catch (RuntimeException e) {
            ring.remove(key);
            version.decrement();
            operationsInProgress = 0;
            throw e;
        }

        // Delegates are transient
        delegates.add(kve.keyValueService());

        // The operation has succeeded
        return true;
    }

    /**
     * You must retry this function until it succeeds before
     * promoting the endpoint with {@link #promoteAddedEndpoint(byte[])}.
     * <p>
     * You should not and you must not repeat the backfill if promotion fails.
     * You can safely retry just the promotion in such case.
     */
    @Override
    public synchronized void backfillAddedEndpoint(byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithJoiningStatus);
        Preconditions.checkState(operationsInProgress == 1);
        EndpointWithJoiningStatus ews = (EndpointWithJoiningStatus) ring.get(key);
        Preconditions.checkArgument(!ews.backfilled());

        KeyValueService kvs = ews.get().keyValueService();
        List<RangeRequest> ranges = getRangesOperatedByKvs(key, false);

        // Copy all the ranges that should be operated by this kvs.
        for (int i = 0; i < ranges.size(); ++i) {
            copyData(kvs, ranges.get(i));
        }

        // Remember that the backfill succeeded.
        ews.setBackfilled();
    }

    /**
     * Before:
     *
     * ranges operated by F
     * A     B     C     D     E     F     G     H     I
     * |     |     |-----|-----|-----|     |     |     |
     *
     * ranges operated by G
     * A     B     C     D     E     F     G     H     I
     * |     |     |     |-----|-----|-----|     |     |
     *
     * ranges operated by H
     * A     B     C     D     E     F     G     H     I
     * |     |     |     |     |-----|-----|-----|     |
     *
     *
     * Inserting E':
     *
     * ranges operated by E'
     * A     B     C     D     E  E' F     G     H     I
     * |     |     |-----|-----|--|  |     |     |     |
     *
     * ranges operated by F
     * A     B     C     D     E  E' F     G     H     I
     * |     |     |     |-----|--|--|     |     |     |
     *
     * ranges operated by G
     * A     B     C     D     E  E' F     G     H     I
     * |     |     |     |     |--|--|-----|     |     |
     *
     * ranges operated by H
     * A     B     C     D     E  E' F     G     H     I
     * |     |     |     |     |  |--|-----|-----|     |
     *
     *
     * Idea: remove the lowest range from REPF higher endpoints.
     * Copy REPF lower ranges to the newly added endpoint.
     *
     * You can and should retry this function until it succeeds.
     *
     */
    @Override
    public synchronized void promoteAddedEndpoint(byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithJoiningStatus);
        Preconditions.checkState(operationsInProgress == 1);
        EndpointWithJoiningStatus ews = (EndpointWithJoiningStatus) ring.get(key);
        Preconditions.checkArgument(ews.backfilled());

        byte[] nextKey = ring.nextKey(key);
        List<RangeRequest> ranges = getRangesOperatedByKvs(key, false);

        // First push the map to crucial endpoints
        // This is to ensure that no garbage is left behind
        ring.put(key,  ring.get(key).asNormal());
        version.increment();
        operationsInProgress = 0;

        try {
            byte[] otherKey = key;
            for (int i=0; i<quorumParameters.getReplicationFactor(); ++i) {
                otherKey = ring.nextKey(otherKey);
                ring.get(otherKey).get().partitionMapService().updateMap(this);
            }
        } catch (RuntimeException e) {
            ring.put(key, ews);
            version.decrement();
            operationsInProgress = 1;
            throw e;
        }

        // Now we can remove the farthest
        // ranges from the endpoints following this one.
        byte[] keyToRemove = nextKey;
        // TODO: Is the last range not a special case?
        for (int i = 0; i < ranges.size(); ++i) {
            deleteData(ring.get(keyToRemove).get().keyValueService(), ranges.get(i));
            if (ranges.get(i).getEndExclusive().length > 0) {
                keyToRemove = ring.nextKey(keyToRemove);
            } else {
                assert i + 1 < ranges.size();
                assert ranges.get(i+1).getStartInclusive().length == 0;
            }
        }
    }

    /**
     * This will set the status of given endpoint to leaving and push the updated map to
     * "crucial" endpoints. This means that if this operation succeeds, you can safely call
     * {@link #promoteRemovedEndpoint(byte[])}.
     * <p>
     * It is recommended that you push the map to all endpoints using {@link #pushMapToEndpoints()}
     * after calling this function.
     * <p>
     * If the function fails, the local map will be reverted to previous state. It is possible that
     * some remote endpoints will have received the new version of map and they will not be reverted.
     * You should retry this function until it succeeds.
     * <p>
     * Note that this implementation supports at most one addEndpoint or removeEndpoint operation at
     * a time. This method will return <code>false</code> if such operation is already in progress.
     *
     * @param key
     * @return <code>true</code> if the operation has been accepted for execution, <code>false</code> otherwise.
     * @throws RuntimeException if an update of a crucial endpoint fails.
     */
    @Override
    public synchronized boolean removeEndpoint(final byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithNormalStatus);
        Preconditions.checkArgument(numOfRacksWithoutEndpoint(key) >= quorumParameters.getReplicationFactor(),
                "Cannot have less racks than replication factor.");
        if (operationsInProgress != 0) {
            return false;
        }

        ring.put(key, ring.get(key).asLeaving());
        operationsInProgress = 1;
        version.increment();

        // Push the map to crucial endpoints
        try {
            byte[] otherKey = key;
            for (int i = 0; i < quorumParameters.getReplicationFactor(); ++i) {
                otherKey = ring.nextKey(otherKey);
                ring.get(otherKey).get().partitionMapService().updateMap(this);
            }
        } catch (RuntimeException e) {
            ring.put(key, ring.get(key).asNormal());
            version.decrement();
            operationsInProgress = 0;
            throw e;
        }

        // The operation has succeeded
        return true;
    }

    @Override
    public synchronized void backfillRemovedEndpoint(byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithLeavingStatus);
        Preconditions.checkState(operationsInProgress == 1);
        EndpointWithLeavingStatus ews = (EndpointWithLeavingStatus) ring.get(key);
        Preconditions.checkArgument(!ews.backfilled());

        List<RangeRequest> ranges = getRangesOperatedByKvs(key, true);

        byte[] dstKvsKey = ring.nextKey(key);
        for (int i = 0; i < ranges.size() - 1; ++i) {
            copyData(ring.get(dstKvsKey).get().keyValueService(), ranges.get(i));

            // If it is unbounded, we need to move both ranges to the
            // same destination kvs (it really is the same range).
            if (ranges.get(i).getEndExclusive().length != 0) {
                dstKvsKey = ring.nextKey(dstKvsKey);
            } else {
                assert ranges.size() > i + 1;
                assert ranges.get(i + 1).getStartInclusive().length == 0;
            }
        }

        // The special case for last range
        copyData(ring.get(dstKvsKey).get().keyValueService(),
                 ranges.get(ranges.size() - 1).getBuilder().endRowExclusive(key).build());


        ews.setBackfilled();
    }

    /**
     * Before:
     *
     * ranges operated by E:
     * A     B     C     D     E     F     G     H     I
     * |     |-----|-----|-----|     |     |     |     |
     *
     * ranges operated by F
     * A     B     C     D     E     F     G     H     I
     * |     |     |-----|-----|-----|     |     |     |
     *
     * ranges operated by G
     * A     B     C     D     E     F     G     H     I
     * |     |     |     |-----|-----|-----|     |     |
     *
     * ranges operated by H
     * A     B     C     D     E     F     G     H     I
     * |     |     |     |     |-----|-----|-----|     |
     *
     *
     * Removing E:
     *
     * ranges operated by F
     * A     B     C     D     *     F     G     H     I
     * |     |-----|-----|-----------|     |     |     |
     *
     * ranges operated by G
     * A     B     C     D     *     F     G     H     I
     * |     |     |-----|-----------|-----|     |     |
     *
     * ranges operated by H
     * A     B     C     D     *     F     G     H     I
     * |     |     |     |-----------|-----|-----|     |
     *
     *
     * Idea: add one lower range to REPF higher endpoints.
     * In case of the last one (H) I only need to add part
     * of the new range that this endpoint did not have
     * previously ([DE]).
     *
     *
     */
    @Override
    public synchronized void promoteRemovedEndpoint(byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithLeavingStatus);
        Preconditions.checkState(operationsInProgress == 1);
        EndpointWithLeavingStatus ews = (EndpointWithLeavingStatus) ring.get(key);
        Preconditions.checkArgument(ews.backfilled());

        KeyValueEndpoint kve = ring.get(key).get();
        KeyValueService kvs = kve.keyValueService();

        // Finalize
        ring.remove(key);
        version.increment();
        operationsInProgress = 0;

        byte[] otherKey = key;
        try {
            // TODO: Shouldn't this be +1, or filter based on normal status?
            for (int i=0; i<quorumParameters.getReplicationFactor(); ++i) {
                otherKey = ring.nextKey(otherKey);
                ring.get(otherKey).get().partitionMapService().updateMap(this);
            }
        } catch (RuntimeException e) {
            ring.put(key, ews);
            version.decrement();
            operationsInProgress = 1;
            throw e;
        }

        // Delegates are transient
        delegates.remove(kvs);

        // Now we can safely remove data from the endpoint.
        // If this fails... I don't care.
        try {
            kve.partitionMapService().updateMap(this);
            for (String table : kvs.getAllTableNames()) {
                kvs.dropTable(table);
            }
        } catch (RuntimeException e) {
            log.warn("Error while removing data from removed endpoint. Ignoring.");
            e.printStackTrace(System.out);
        }
    }

    @Override
    public long getVersion() {
        return version.toLong();
    }

    /**
     * For test purposes only!
     *
     * Directly set the version of this map to <code>version</code> without
     * any other side effects.
     *
     * @param version
     */
    @Deprecated @VisibleForTesting
    public void setVersion(long version) {
        this.version.setValue(version);
    }

    /*** toString, hashCode and equals ***********************************************************/
    @Override
    public String toString() {
        return "DynamicPartitionMapImpl (" + version +
               "): QP=(" + quorumParameters.getReplicationFactor() +
               "," + quorumParameters.getReadFactor() +
               "," + quorumParameters.getWriteFactor() + ")\n" +
               ringDescription();
    }

    private String ringDescription() {
        StringBuilder builder = new StringBuilder();
        for (Entry<byte[], EndpointWithStatus> e : ring.entrySet()) {
            builder.append(e.getValue().get() + " (" + statusDescription(e.getValue()) + ") @ " + Arrays.toString(e.getKey()) + "\n");
        }
        return builder.toString();
    }

    private static String statusDescription(EndpointWithStatus ews) {
        if (ews instanceof EndpointWithNormalStatus) {
            return "N";
        }
        if (ews instanceof EndpointWithJoiningStatus) {
            return "J";
        }
        if (ews instanceof EndpointWithLeavingStatus) {
            return "L";
        }
        throw new IllegalArgumentException("Unsupported EndpointWithStatus instance");
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + (int) (operationsInProgress ^ (operationsInProgress >>> 32));
        result = prime * result + ((quorumParameters == null) ? 0
                : quorumParameters.hashCode());
        result = prime * result + ((ring == null) ? 0 : ring.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DynamicPartitionMapImpl other = (DynamicPartitionMapImpl) obj;
        if (operationsInProgress != other.operationsInProgress)
            return false;
        if (quorumParameters == null) {
            if (other.quorumParameters != null)
                return false;
        } else if (!quorumParameters.equals(other.quorumParameters))
            return false;
        if (ring == null) {
            if (other.ring != null)
                return false;
        } else if (!ring.equals(other.ring))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }



    /*** serialization and deserialization *******************************************************/
    public static class Serializer extends JsonSerializer<DynamicPartitionMapImpl> {
        private static final Serializer instance = new Serializer();
        public static final Serializer instance() { return instance; }

        @Override
        public void serialize(DynamicPartitionMapImpl instance,
                              JsonGenerator gen, SerializerProvider serializers) throws IOException,
                JsonProcessingException {

            gen.writeStartObject();
            gen.writeObjectField("quorumParameters", instance.quorumParameters);
            gen.writeObjectField("version", instance.version.longValue());
            gen.writeObjectField("operationsInProgress", instance.operationsInProgress);
            gen.writeFieldName("ring");
            gen.writeStartArray();
            for (Entry<byte[], EndpointWithStatus> entry : instance.ring.entrySet()) {
                if (!(entry.getValue().get() instanceof SimpleKeyValueEndpoint)) {
                    throw new IllegalArgumentException("DynamicPartitionMapImpl serialization is only supported with SimplKeyValueEndpoint endpoints!");
                }
                gen.writeStartObject();
                gen.writeBinaryField("key", entry.getKey());
                gen.writeObjectField("endpointWithStatus", entry.getValue());
                gen.writeEndObject();
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }
    }

    public static class Deserializer extends JsonDeserializer<DynamicPartitionMapImpl>{
        static final Deserializer instance = new Deserializer();
        public static final Deserializer instance() { return instance; }

        @Override
        public DynamicPartitionMapImpl deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {

            JsonNode root = p.getCodec().readTree(p);

            long version = root.get("version").asLong();
            long operationsInProgress = root.get("operationsInProgress").asLong();
            QuorumParameters parameters = RemotingKeyValueService.kvsMapper().readValue(
                    root.get("quorumParameters").toString(), QuorumParameters.class);
            Iterator<JsonNode> ringIterator = root.get("ring").elements();
            NavigableMap<byte[], EndpointWithStatus> ring =
                    Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

            while (ringIterator.hasNext()) {
                JsonNode endpointNode = ringIterator.next();

                byte[] key = endpointNode.get("key").binaryValue();

                EndpointWithStatus endpoint = RemotingKeyValueService.kvsMapper().readValue(
                        endpointNode.get("endpointWithStatus").toString(),
                        EndpointWithStatus.class);

                ring.put(key, endpoint);
            }

            return new DynamicPartitionMapImpl(parameters, CycleMap.wrap(ring),
                    version, operationsInProgress, PTExecutors.newCachedThreadPool());
        }
    }

    private static final Cell REPF_CELL = Cell.create("quorumParameters".getBytes(), "repf".getBytes());
    private static final Cell READF_CELL = Cell.create("quorumParameters".getBytes(), "readf".getBytes());
    private static final Cell WRITEF_CELL = Cell.create("quorumParameters".getBytes(), "writef".getBytes());
    private static final Cell VERSION_CELL = Cell.create("version".getBytes(), "version".getBytes());
    private static final Cell OPS_IN_PROGRESS_CELL = Cell.create("operations".getBytes(), "inProgress".getBytes());

    public Map<Cell, byte[]> toTable() {
        try {
            Map<Cell, byte[]> result = Maps.newHashMap();

            // Store the quorum parameters
            result.put(REPF_CELL, Integer.toString(quorumParameters.getReplicationFactor()).getBytes());
            result.put(READF_CELL, Integer.toString(quorumParameters.getReadFactor()).getBytes());
            result.put(WRITEF_CELL, Integer.toString(quorumParameters.getWriteFactor()).getBytes());

            // Store the map version
            result.put(VERSION_CELL, Long.toString(version.longValue()).getBytes());

            // Store no of operations in progress
            result.put(OPS_IN_PROGRESS_CELL, Long.toString(operationsInProgress).getBytes());

            // Store the map
            for (Entry<byte[], EndpointWithStatus> entry : ring.entrySet()) {
                byte[] row = "map".getBytes();
                byte[] col = entry.getKey();
                if (!(entry.getValue().get() instanceof SimpleKeyValueEndpoint)) {
                    throw new IllegalArgumentException("DynamicPartitionMapImpl serialization is only supported with SimplKeyValueEndpoint endpoints!");
                }
                byte[] value = RemotingKeyValueService.kvsMapper().writeValueAsBytes(entry.getValue());
                result.put(Cell.create(row, col), value);
            }

            return result;

        } catch (JsonProcessingException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static DynamicPartitionMapImpl fromTable(Map<Cell, byte[]> table) {
        try {

            int repf = Integer.parseInt(new String(table.get(REPF_CELL)));
            int readf = Integer.parseInt(new String(table.get(READF_CELL)));
            int writef = Integer.parseInt(new String(table.get(WRITEF_CELL)));
            long version = Long.parseLong(new String(table.get(VERSION_CELL)));
            long operationsInProgress = Long.parseLong(new String(table.get(OPS_IN_PROGRESS_CELL)));
            QuorumParameters parameters = new QuorumParameters(repf, readf, writef);
            NavigableMap<byte[], EndpointWithStatus> ring =
                    Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

            for (Entry<Cell, byte[]> entry : table.entrySet()) {
                if (!Arrays.equals(entry.getKey().getRowName(), "map".getBytes())) {
                    continue;
                }
                byte[] key = entry.getKey().getColumnName();
                EndpointWithStatus ews = RemotingKeyValueService.kvsMapper().readValue(entry.getValue(), EndpointWithStatus.class);
                ring.put(key, ews);
            }

            return new DynamicPartitionMapImpl(parameters, CycleMap.wrap(ring),
                    version, operationsInProgress, PTExecutors.newCachedThreadPool());

        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public Map<byte[], QuorumRequestParameters> getReadRowsParameters(
            Iterable<byte[]> rows) {
        Map<byte[], QuorumRequestParameters> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        for (byte[] row : rows) {
            int repf = getServicesHavingRow(row, false).size();
            int readf = repf - (quorumParameters.getReplicationFactor() - quorumParameters.getReadFactor());
            int writef = repf - (quorumParameters.getReplicationFactor() - quorumParameters.getWriteFactor());
            QuorumParameters params = new QuorumParameters(repf, readf, writef);
            result.put(row, params.getReadRequestParameters());
        }

        return result;
    }

    @Override
    public Map<byte[], QuorumRequestParameters> getWriteRowsParameters(
            Set<byte[]> rows) {
        Map<byte[], QuorumRequestParameters> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        for (byte[] row : rows) {
            int repf = getServicesHavingRow(row, true).size();
            int readf = repf - (quorumParameters.getReplicationFactor() - quorumParameters.getReadFactor());
            int writef = repf - (quorumParameters.getReplicationFactor() - quorumParameters.getWriteFactor());
            QuorumParameters params = new QuorumParameters(repf, readf, writef);
            result.put(row, params.getWriteRequestParameters());
        }

        return result;
    }

    private static Set<byte[]> getRows(Set<Cell> cells) {
        Set<byte[]> result = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        for (Cell cell : cells) {
            result.add(cell.getRowName());
        }
        return result;
    }

    @Override
    public Map<Cell, QuorumRequestParameters> getReadCellsParameters(
            Set<Cell> cells) {
        Map<Cell, QuorumRequestParameters> result = Maps.newHashMap();
        Map<byte[], QuorumRequestParameters> rowsResult = getReadRowsParameters(getRows(cells));
        for (Cell cell : cells) {
            result.put(cell, rowsResult.get(cell.getRowName()));
        }
        return result;
    }

    @Override
    public Map<Cell, QuorumRequestParameters> getWriteCellsParameters(
            Set<Cell> cells) {
        Map<Cell, QuorumRequestParameters> result = Maps.newHashMap();
        Map<byte[], QuorumRequestParameters> rowsResult = getWriteRowsParameters(getRows(cells));
        for (Cell cell : cells) {
            result.put(cell, rowsResult.get(cell.getRowName()));
        }
        return result;
    }

    @Override
    public <T> Map<Entry<Cell, T>, QuorumRequestParameters> getReadEntriesParameters(
            Map<Cell, T> entries) {
        Map<Entry<Cell, T>, QuorumRequestParameters> result = Maps.newHashMap();
        Map<byte[], QuorumRequestParameters> rowsResult = getReadRowsParameters(getRows(entries.keySet()));
        for (Entry<Cell, T> e : entries.entrySet()) {
            result.put(e, rowsResult.get(e.getKey().getRowName()));
        }
        return result;
    }

    @Override
    public <T> Map<Entry<Cell, T>, QuorumRequestParameters> getReadEntriesParameters(
            Multimap<Cell, T> entries) {
        Map<Entry<Cell, T>, QuorumRequestParameters> result = Maps.newHashMap();
        Map<byte[], QuorumRequestParameters> rowsResult = getReadRowsParameters(getRows(entries.keySet()));
        for (Entry<Cell, T> e : entries.entries()) {
            result.put(e, rowsResult.get(e.getKey().getRowName()));
        }
        return result;
    }

    @Override
    public <T> Map<Entry<Cell, T>, QuorumRequestParameters> getWriteEntriesParameters(
            Map<Cell, T> entries) {
        Map<Entry<Cell, T>, QuorumRequestParameters> result = Maps.newHashMap();
        Map<byte[], QuorumRequestParameters> rowsResult = getWriteRowsParameters(getRows(entries.keySet()));
        for (Entry<Cell, T> e : entries.entrySet()) {
            result.put(e, rowsResult.get(e.getKey().getRowName()));
        }
        return result;
    }

    @Override
    public <T> Map<Entry<Cell, T>, QuorumRequestParameters> getWriteEntriesParameters(
            Multimap<Cell, T> entries) {
        Map<Entry<Cell, T>, QuorumRequestParameters> result = Maps.newHashMap();
        Map<byte[], QuorumRequestParameters> rowsResult = getWriteRowsParameters(getRows(entries.keySet()));
        for (Entry<Cell, T> e : entries.entries()) {
            result.put(e, rowsResult.get(e.getKey().getRowName()));
        }
        return result;
    }
}
