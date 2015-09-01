package com.palantir.atlasdb.keyvalue.partition.map;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
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
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithJoiningStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithLeavingStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithNormalStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithStatus;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.CycleMap;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;
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
 * Note (sanity check): countForX implicates useForX.
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
 */
public class DynamicPartitionMapImpl implements DynamicPartitionMap {

    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], EndpointWithStatus> ring;
    private final Mutable<Long> version = Mutables.newMutable(0L);

    private transient final BlockingQueue<Future<Void>> removals = Queues.newLinkedBlockingQueue();
    private transient final BlockingQueue<Future<Void>> joins = Queues.newLinkedBlockingQueue();
    private transient final Set<KeyValueService> delegates;
    private transient final ExecutorService executor = PTExecutors.newCachedThreadPool();

    private final Supplier<Long> versionSupplier = new Supplier<Long>() {
        @Override
        public Long get() {
            return Preconditions.checkNotNull(version.get());
        }
    };

    @GuardedBy("this")
    private long operationsInProgress;

    public static class Serializer extends JsonSerializer<DynamicPartitionMapImpl> {
        private static final Serializer instance = new Serializer();
        public static final Serializer instance() { return instance; }
        @Override
        public void serialize(DynamicPartitionMapImpl value,
                              JsonGenerator gen,
                              SerializerProvider serializers) throws IOException,
                JsonProcessingException {
            gen.writeStartObject();
            gen.writeObjectField("quorumParameters", value.quorumParameters);
            gen.writeObjectField("version", Preconditions.checkNotNull(value.version.get()));
            gen.writeObjectField("operationsInProgress", value.operationsInProgress);
            gen.writeFieldName("ring");
            gen.writeStartArray();
            for (Entry<byte[], EndpointWithStatus> entry : value.ring.entrySet()) {
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
            QuorumParameters parameters = new ObjectMapper().readValue(
                    "" + root.get("quorumParameters"),
                    QuorumParameters.class);
            Iterator<JsonNode> it = root.get("ring").elements();
            NavigableMap<byte[], EndpointWithStatus> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            while (it.hasNext()) {
                JsonNode entry = it.next();
                byte[] key = entry.get("key").binaryValue();
                String strEndpoint = "" + entry.get("endpointWithStatus");
                EndpointWithStatus endpoint = new ObjectMapper().readValue(
                        strEndpoint,
                        EndpointWithStatus.class);
                ring.put(key, endpoint);
            }
            DynamicPartitionMapImpl ret = new DynamicPartitionMapImpl(parameters, ring);
            ret.version.set(version);
            ret.operationsInProgress = operationsInProgress;
            return ret;
        }

        @Override
        public Object deserializeWithType(JsonParser p,
                                          DeserializationContext ctxt,
                                          TypeDeserializer typeDeserializer) throws IOException {
            return deserialize(p, ctxt);
        }
    }

    public DynamicPartitionMapImpl(QuorumParameters quorumParameters, NavigableMap<byte[], EndpointWithStatus> ring) {
        Preconditions.checkArgument(ring.keySet().size() >= quorumParameters.getReplicationFactor());
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
        buildRing(this.ring);
        delegates = Sets.newHashSet();
        for (EndpointWithStatus kve : this.ring.values()) {
            delegates.add(kve.get().keyValueService());
        }
    }

    private CycleMap<byte[], EndpointWithStatus> toRing(NavigableMap<byte[], KeyValueEndpoint> map) {
        NavigableMap<byte[], EndpointWithStatus> transformedMap = Maps.transformValues(map, new Function<KeyValueEndpoint, EndpointWithStatus>() {
            @Override
            public EndpointWithStatus apply(@Nullable KeyValueEndpoint input) {
                return new EndpointWithNormalStatus(input);
            }
        });
        return CycleMap.wrap(Maps.newTreeMap(transformedMap));
    }

    private void buildRing(NavigableMap<byte[], EndpointWithStatus> map) {
        for (EndpointWithStatus e : map.values()) {
            e.get().build(versionSupplier);
        }
    }

	private DynamicPartitionMapImpl(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueEndpoint> ring, int nothin) {
        this.ring = toRing(ring);
        buildRing(this.ring);
        this.quorumParameters = quorumParameters;
        delegates = Sets.newHashSet();
        for (EndpointWithStatus kve : this.ring.values()) {
                        kve.get().keyValueService();
            delegates.add(kve.get().keyValueService());
        }
    }

	public static DynamicPartitionMapImpl create(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueEndpoint> ring) {
        return new DynamicPartitionMapImpl(quorumParameters, ring, 0);
    }

	public static DynamicPartitionMapImpl create(NavigableMap<byte[], KeyValueEndpoint> ring) {
        return create(new QuorumParameters(3, 2, 2), ring);
    }

    // *** Helper methods **************************************************************************
    private Set<KeyValueEndpoint> getServicesHavingRow(byte[] key, boolean isWrite) {
        Set<KeyValueEndpoint> result = Sets.newHashSet();
        byte[] point = key;
        int extraServices = 0; // These are included in the result set but
                               // Are not counted against the replication factor
        while (result.size() < quorumParameters.getReplicationFactor() + extraServices) {
            point = ring.nextKey(point);
            EndpointWithStatus kvs = ring.get(point);
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


    @Override
    // *** Public methods **************************************************************************
    @JsonIgnore
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

    private static <T> void apply(final Entry<KeyValueEndpoint, ? extends T> entry, final Function<Pair<KeyValueService, T>, Void> task) {
        task.apply(Pair.<KeyValueService, T>create(entry.getKey().keyValueService(), entry.getValue()));
    }

    @Override
    public void runForRowsRead(String tableName,
                                Iterable<byte[]> rows,
                                final Function<Pair<KeyValueService, Iterable<byte[]>>, Void> task) {
        for (final Entry<KeyValueEndpoint, NavigableSet<byte[]>> e : getServicesForRowsRead(tableName, rows).entrySet()) {
            apply(e, task);
        }
    }

    private Map<KeyValueEndpoint, Set<Cell>> getServicesForCellsRead(String tableName, Set<Cell> cells) {
        return getServicesForCellsSet(tableName, cells, false);
    }

    @Override
    public void runForCellsRead(String tableName,
                                Set<Cell> cells,
                                final Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Set<Cell>> e : getServicesForCellsRead(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }

    private <T> Map<KeyValueEndpoint, Map<Cell, T>> getServicesForCellsRead(String tableName,
                                                                         Map<Cell, T> timestampByCell) {
        return getServicesForCellsMap(tableName, timestampByCell, false);
    }

    @Override
    public <T> void runForCellsRead(String tableName,
                                    Map<Cell, T> cells,
                                    final Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Map<Cell, T>> e : getServicesForCellsRead(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }

    private Map<KeyValueEndpoint, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                    Set<Cell> cells) {
        return getServicesForCellsSet(tableName, cells, true);
    }

    @Override
    public void runForCellsWrite(String tableName,
                                 Set<Cell> cells,
                                 final Function<Pair<KeyValueService, Set<Cell>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Set<Cell>> e : getServicesForCellsWrite(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }

    private <T> Map<KeyValueEndpoint, Multimap<Cell, T>> getServicesForCellsWrite(String tableName,
                                                                           Multimap<Cell, T> keys) {
        return getServicesForCellsMultimap(tableName, keys, true);
    }

    @Override
    public <T> void runForCellsWrite(String tableName,
                                     Multimap<Cell, T> cells,
                                     final Function<Pair<KeyValueService, Multimap<Cell, T>>, Void> task) {
        for (final Entry<KeyValueEndpoint, Multimap<Cell, T>> e : getServicesForCellsWrite(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }

    @JsonIgnore
    @Override
    public Set<? extends KeyValueService> getDelegates() {
        return delegates;
    }

    private <T> Map<KeyValueEndpoint, Map<Cell, T>> getServicesForCellsWrite(String tableName,
                                                                           Map<Cell, T> values) {
        return getServicesForCellsMap(tableName, values, true);
    }

    @Override
    public <T> void runForCellsWrite(String tableName,
                                     Map<Cell, T> cells,
                                     Function<Pair<KeyValueService, Map<Cell, T>>, Void> task) {
        for (Entry<KeyValueEndpoint, Map<Cell, T>> e : getServicesForCellsWrite(tableName, cells).entrySet()) {
            apply(e, task);
        }
    }

    /**
     * Copies rows within the specified range from all the tables.
     * @param destination
     * @param source
     * @param rangeToCopy
     */
    private void copyData(KeyValueEndpoint destination, KeyValueEndpoint source, RangeRequest rangeToCopy) {
        for (String tableName : source.keyValueService().getAllTableNames()) {
            Multimap<Cell, Value> cells = HashMultimap.create();
            ClosableIterator<RowResult<Value>> allRows = source.keyValueService().getRange(tableName, rangeToCopy, Long.MAX_VALUE);
            while (allRows.hasNext()) {
                RowResult<Value> row = allRows.next();
                for (Entry<Cell, Value> entry : row.getCells()) {
                    cells.put(entry.getKey(), entry.getValue());
                }
            }
            if (!cells.isEmpty()) {
                destination.keyValueService().putWithTimestamps(tableName, cells);
            }
            allRows.close();
        }
    }

    /**
     * Deletes rows within the specified range from all the tables.
     * @param kvs
     * @param rangeToDelete
     */
    private void deleteData(KeyValueEndpoint kvs, RangeRequest rangeToDelete) {
        for (String tableName : kvs.keyValueService().getAllTableNames()) {
            Multimap<Cell, Long> cells = HashMultimap.create();
            ClosableIterator<RowResult<Set<Long>>> allTimestamps = kvs.keyValueService().getRangeOfTimestamps(tableName, rangeToDelete, Long.MAX_VALUE);
            while (allTimestamps.hasNext()) {
                RowResult<Set<Long>> row = allTimestamps.next();
                for (Entry<Cell, Set<Long>> entry : row.getCells()) {
                    for (Long timestamp : entry.getValue()) {
                        cells.put(entry.getKey(), timestamp);
                    }
                }
            }
            kvs.keyValueService().delete(tableName, cells);
        }
    }

    /**
     * Returns ranges that should be stored and/or read from the given kvs.
     * It is intended for use when adding and/or removing endpoints.
     *
     * @param kvsKey Consider kvs at this key.
     * @param isWrite Are we looking for write or read access?
     * @return The first element is the farthest range.
     */
    private List<RangeRequest> getRangesOperatedByKvs(byte[] kvsKey, boolean isWrite) {
        Preconditions.checkNotNull(ring.get(kvsKey));
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
    public boolean addEndpoint(byte[] key, KeyValueEndpoint kvs, String rack) {
        return addEndpoint(key, kvs, rack, true);
    }

    public synchronized boolean addEndpoint(final byte[] key, final KeyValueEndpoint kvs, String rack, final boolean autoFinalize) {
        version.set(version.get() + 1);

        // Sanity checks
        Preconditions.checkArgument(!ring.containsKey(key));

        if (!removals.isEmpty() || !joins.isEmpty() || operationsInProgress > 0) {
            return false;
        }
        operationsInProgress++;

        kvs.build(versionSupplier);
        delegates.add(kvs.keyValueService());

        joins.add(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                byte[] nextKey = ring.nextKey(key);
                List<RangeRequest> ranges = getRangesOperatedByKvs(nextKey, false);

                // Insert the joiner into the ring
                KeyValueEndpoint sourceKvs = ring.get(nextKey).get();
                ring.put(key, new EndpointWithJoiningStatus(kvs));

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
                copyData(kvs, sourceKvs, lastRange1);

                // Some anti-bug asserts
                assert UnsignedBytes.lexicographicalComparator().compare(lastRange.getStartInclusive(), key) < 0;
                assert UnsignedBytes.lexicographicalComparator().compare(lastRange.getEndExclusive(), key) > 0;

                // Change the status to regular
                // TODO: Not doing this for testing purporses.
                // Use finalizer to finalize the join.
                // finalizeAddEndpoint(key, kvs);
                if (autoFinalize) {
                    finalizeAddEndpoint(key);
                }

                // The last thing to be done is removing the farthest
                // range from the following kvss.
                byte[] keyToRemove = nextKey;
                for (int i = 0; i < ranges.size() - 1; ++i) {
                    RangeRequest rangeToRemove = ranges.get(i);
                    deleteData(ring.get(keyToRemove).get(), rangeToRemove);
                    keyToRemove = ring.nextKey(keyToRemove);
                }
                deleteData(ring.get(keyToRemove).get(), lastRange1);
                return null;
            }
        }));

        return true;
    }

    public void syncAddEndpoint() {
        // Do it synchronously now for testing purposes
        try {
            while (!joins.isEmpty()) {
                Futures.getUnchecked(joins.take());
            }
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public synchronized void finalizeAddEndpoint(byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithJoiningStatus);
        KeyValueEndpoint kve = ring.get(key).get();
        version.set(version.get() + 1);
        while (!joins.isEmpty()) {
            try {
                Futures.getUnchecked(joins.take());
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            }
        }
        ring.put(key, new EndpointWithNormalStatus(kve));
        kve.build(versionSupplier);
        operationsInProgress--;
    }

    @Override
    public synchronized boolean removeEndpoint(byte[] key) {
        return removeEndpoint(key, true);
    }

    public synchronized boolean removeEndpoint(final byte[] key, final boolean autoFinalize) {
        final KeyValueEndpoint kve = Preconditions.checkNotNull(ring.get(key)).get();
        version.set(version.get() + 1);

        // Sanity checks
        Preconditions.checkArgument(ring.keySet().size() > quorumParameters.getReplicationFactor());

        if (!removals.isEmpty() || !joins.isEmpty() || operationsInProgress > 0) {
            return false;
        }
        operationsInProgress++;

        removals.add(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                List<RangeRequest> ranges = getRangesOperatedByKvs(key, true);

                // Set the status to leaving
                ring.put(key, Preconditions.checkNotNull(ring.get(key).asLeaving()));

                // Copy the farthest range to the first higher kvs.
                // Copy the second-farthest range to the second-higher kvs.
                // Etc.
                byte[] dstKvsKey = ring.nextKey(key);
                for (int i = 0; i < ranges.size() - 1; ++i) {
                    copyData(ring.get(dstKvsKey).get(), kve, ranges.get(i));
                    dstKvsKey = ring.nextKey(dstKvsKey);
                }

                // The special case for the last range.
                RangeRequest lastRange1 = ranges.get(ranges.size() - 1).getBuilder().endRowExclusive(key).build();
                copyData(ring.get(dstKvsKey).get(), kve, lastRange1);

                if (autoFinalize) {
                    // Remove the kvs from the ring.
                    finalizeRemoveEndpoint(key);
                }
                return null;
            }
        }));

        return true;
    }

    public void syncRemoveEndpoint() {
        // Do it synchronously for testing purposes.
        try {
            while (!removals.isEmpty()) {
                Futures.getUnchecked(removals.take());
            }
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }

    }

    public synchronized void finalizeRemoveEndpoint(byte[] key) {
        Preconditions.checkArgument(ring.get(key) instanceof EndpointWithLeavingStatus);
        KeyValueEndpoint kve = Preconditions.checkNotNull(ring.get(key)).get();
        version.set(version.get() + 1);
        while (!removals.isEmpty()) {
            try {
                Futures.getUnchecked(removals.take());
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            }
        }
        ring.remove(key);
        operationsInProgress--;
        delegates.remove(kve.keyValueService());
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    /**
     * For test purposes only!
     *
     * @param version
     */
    @Deprecated
	public void setVersion(long version) {
		this.version.set(version);
	}

    @Override
    public String toString() {
        return "DynamicPartitionMapImpl [quorumParameters=" + quorumParameters + ", ring=" + ring
                + ", version=" + version + "]";
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((quorumParameters == null) ? 0 : quorumParameters.hashCode());
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

}
