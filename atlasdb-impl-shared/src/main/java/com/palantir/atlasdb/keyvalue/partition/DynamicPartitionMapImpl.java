package com.palantir.atlasdb.keyvalue.partition;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
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
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithNormalStatus;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithStatus;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.util.CycleMap;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;

public class DynamicPartitionMapImpl implements DynamicPartitionMap {

    @JsonProperty("quorumParameters") private final QuorumParameters quorumParameters;
    @JsonProperty("ring") private final CycleMap<byte[], EndpointWithStatus> ring;
    private long version = 0L;

    private transient final BlockingQueue<Future<Void>> removals = Queues.newLinkedBlockingQueue();
    private transient final BlockingQueue<Future<Void>> joins = Queues.newLinkedBlockingQueue();

    static private <K, V1, V2> NavigableMap<K, V2> transformValues(NavigableMap<K, V1> map, Function<V1, V2> transform) {
        NavigableMap<K, V2> result = Maps.newTreeMap(map.comparator());
        for (Entry<K, V1> entry : map.entrySet()) {
            result.put(entry.getKey(), transform.apply(entry.getValue()));
        }
        return result;
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
        result = prime * result + ((quorumParameters == null) ? 0 : quorumParameters.hashCode());
        result = prime * result + ((ring == null) ? 0 : ring.hashCode());
        result = prime * result + (int) (version ^ (version >>> 32));
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
        if (version != other.version)
            return false;
        if (ring == null) {
            if (other.ring != null)
                return false;
        } else {
            for (Entry<byte[], EndpointWithStatus> entry : ring.entrySet()) {
                if (!other.ring.containsKey(entry.getKey())) {
                    return false;
                }
                if (!other.ring.get(entry.getKey()).equals(entry.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static class Serializer extends JsonSerializer<DynamicPartitionMapImpl> {
        private static final Serializer instance = new Serializer();
        public static final Serializer instance() { return instance; }
        @Override
        public void serialize(DynamicPartitionMapImpl value,
                              JsonGenerator gen,
                              SerializerProvider serializers) throws IOException,
                JsonProcessingException {
            gen.writeObjectField("quorumParameters", value.quorumParameters);
            gen.writeObjectField("version", value.version);
            gen.writeFieldName("ring");
            gen.writeStartArray();
            for (Entry<byte[], EndpointWithStatus> entry : value.ring.entrySet()) {
                gen.writeStartObject();
                gen.writeBinaryField("key", entry.getKey());
                gen.writeObjectField("endpointWithStatus", entry.getValue());
                gen.writeEndObject();
            }
            gen.writeEndArray();
        }
        @Override
        public void serializeWithType(DynamicPartitionMapImpl value,
                                      JsonGenerator gen,
                                      SerializerProvider serializers,
                                      TypeSerializer typeSer) throws IOException {
            typeSer.writeTypePrefixForObject(value, gen);
            serialize(value, gen, serializers);
            typeSer.writeTypeSuffixForObject(value, gen);
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
            QuorumParameters parameters = new ObjectMapper().readValue("" + root.get("quorumParameters"), QuorumParameters.class);
            Iterator<JsonNode> it = root.get("ring").elements();
            NavigableMap<byte[], EndpointWithStatus> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            while (it.hasNext()) {
                JsonNode entry = it.next();
                byte[] key = entry.get("key").binaryValue();
                String strEndpoint = "" + entry.get("endpointWithStatus");
                EndpointWithStatus endpoint = new ObjectMapper().readValue(strEndpoint, EndpointWithStatus.class);
                ring.put(key, endpoint);
            }
            DynamicPartitionMapImpl ret = new DynamicPartitionMapImpl(parameters, ring);
            ret.version = version;
            return ret;
        }
        @Override
        public Object deserializeWithType(JsonParser p,
                                          DeserializationContext ctxt,
                                          TypeDeserializer typeDeserializer) throws IOException {
//            typeDeserializer.deserializeTypedFromObject(p, ctxt);
            return deserialize(p, ctxt);
        }
    }

    @JsonCreator
    public DynamicPartitionMapImpl(@JsonProperty("quorumParameters") QuorumParameters quorumParameters,
                                   @JsonProperty("ring") NavigableMap<byte[], EndpointWithStatus> ring) {
        Preconditions.checkArgument(ring.keySet().size() >= quorumParameters.replicationFactor);
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
    }

    public static DynamicPartitionMapImpl create(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueEndpoint> ring) {
        return new DynamicPartitionMapImpl(quorumParameters, transformValues(ring, new Function<KeyValueEndpoint, EndpointWithStatus>() {
            @Override @Nullable
            public EndpointWithStatus apply(@Nullable KeyValueEndpoint input) {
                return new EndpointWithNormalStatus(input);
            }
        }));
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
        assert result.keySet().size() >= quorumParameters.getReplicationFactor();
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
        throw new UnsupportedOperationException();
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

    /**
     * Deletes rows within the specified range from all the tables.
     * @param kvs
     * @param rangeToDelete
     */
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

    /**
     * Returns ranges that should be stored and/or read from the given kvs.
     * It is intended for use when adding and/or removing endpoints.
     *
     * A - 3
     * B - 5
     * C - 8
     * D - 10
     * E - 12
     *
     * removeEndpoint scenario:
     *   - change status from regular to 'leaving'
     *   - get the ranges operated by this kvs for read (because I will be reading
     *     from the kvs and writing to other kvss)
     *   - copy the farthest range to the first higher kvs
     *   - copy the second-farthest range to the second higher kvs
     *   - etc...
     *   - remove the kvs from the ring
     *
     * addEndpoint scenario:
     *   - insert with the 'joining' status
     *   - get ranges operated by this kvs for write (because I will be reading
     *     from other kvss and writing to this one)
     *   - copy all the range from the first higher kvs (TODO: high availability)
     *     BUT the very highest range
     *   - copy the part of the very highest range that is below the new kvs
     *   - change status to 'regular'
     *   - remove the farthest range from the first higher kvs
     *   - remove the second-farthest range from the second higher kvs
     *   - etc...
     *
     * @param kvsKey Consider kvs at this key.
     * @param isWrite Are we looking for write or read access?
     * @return The first element is the farthest range.
     */
    private List<RangeRequest> getRangesOperatedByKvs(byte[] kvsKey, boolean isWrite) {
        EndpointWithStatus kvsws = Preconditions.checkNotNull(ring.get(kvsKey));
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
        version++;
        throw new UnsupportedOperationException();
//        // Sanity checks
//        Preconditions.checkArgument(!ring.containsKey(key));
//
//        joins.add(executor.submit(new Callable<Void>() {
//            @Override
//            public Void call() throws Exception {
//                byte[] nextKey = ring.nextKey(key);
//                List<RangeRequest> ranges = getRangesOperatedByKvs(nextKey, false);
//
//                // Insert the joiner into the ring
//                KeyValueService sourceKvs = ring.get(nextKey).get();
//                KeyValueServiceWithStatus kvsWithStatus = new JoiningKeyValueService(kvs);
//                ring.put(key, kvsWithStatus);
//
//                // First, copy all the ranges besides the one in which
//                // the new kvs is located. All these will be operated
//                // by the new kvs.
//                for (int i = 0; i < ranges.size() - 1; ++i) {
//                    copyData(kvs, sourceKvs, ranges.get(i));
//                }
//
//                // Now we need to split the last range into two pieces.
//                // The second piece will not be operated by the joiner,
//                // thus I only need to copy the first part over.
//                RangeRequest lastRange = ranges.get(ranges.size() - 1);
//                RangeRequest lastRange1 = lastRange.getBuilder().endRowExclusive(key).build();
//                copyData(kvs, sourceKvs, lastRange1);
//
//                // Some anti-bug asserts
//                assert UnsignedBytes.lexicographicalComparator().compare(lastRange.getStartInclusive(), key) < 0;
//                assert UnsignedBytes.lexicographicalComparator().compare(lastRange.getEndExclusive(), key) > 0;
//
//                // Change the status to regular
//                // TODO: Not doing this for testing purporses.
//                // Use finalizer to finalize the join.
//                // ring.put(key, new RegularKeyValueService(kvs));
//
//                // The last thing to be done is removing the farthest
//                // range from the following kvss.
//                byte[] keyToRemove = nextKey;
//                for (int i = 0; i < ranges.size() - 1; ++i) {
//                    RangeRequest rangeToRemove = ranges.get(i);
//                    deleteData(ring.get(keyToRemove).get(), rangeToRemove);
//                    keyToRemove = ring.nextKey(keyToRemove);
//                }
//                deleteData(ring.get(keyToRemove).get(), lastRange1);
//                return null;
//            }
//        }));
//
//        // Do it synchronously now for testing purposes (TODO)
//        try {
//            Futures.getUnchecked(joins.take());
//        } catch (InterruptedException e) {
//            throw Throwables.throwUncheckedException(e);
//        }
    }

    public void finalizeAddEndpoint(byte[] key, KeyValueEndpoint kvs) {
        version++;
        while (!joins.isEmpty()) {
            try {
                Futures.getUnchecked(joins.take());
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            }
        }
        ring.put(key, new EndpointWithNormalStatus(kvs));
    }

    @Override
    public synchronized void removeEndpoint(final byte[] key, final KeyValueService kvs, String rack) {
        version++;
        throw new UnsupportedOperationException();
//        // Sanity checks
//        Preconditions.checkArgument(ring.keySet().size() > quorumParameters.getReplicationFactor());
//
//        removals.add(executor.submit(new Callable<Void>() {
//            @Override
//            public Void call() throws Exception {
//                List<RangeRequest> ranges = getRangesOperatedByKvs(key, true);
//
//                // Set the status to leaving
//                KeyValueServiceWithStatus original = Preconditions.checkNotNull(ring.get(key));
//                LeavingKeyValueService leavingKeyValueService = new LeavingKeyValueService(original.get());
//                ring.put(key, leavingKeyValueService);
//
//                // Copy the farthest range to the first higher kvs.
//                // Copy the second-farthest range to the second-higher kvs.
//                // Etc.
//                byte[] dstKvsKey = ring.nextKey(key);
//                for (int i = 0; i < ranges.size() - 1; ++i) {
//                    copyData(ring.get(dstKvsKey).get(), kvs, ranges.get(i));
//                    dstKvsKey = ring.nextKey(dstKvsKey);
//                }
//
//                // The special case for the last range.
//                RangeRequest lastRange1 = ranges.get(ranges.size() - 1).getBuilder().endRowExclusive(key).build();
//                copyData(ring.get(dstKvsKey).get(), kvs, lastRange1);
//
//                // Remove the kvs from the ring.
//                // TODO: Not doing this for testing purposes.
//                // Use finalizer to finalize the removal.
////                ring.remove(key);
//                return null;
//            }
//        }));
//
//        // Do it synchronously for testing purposes.
//        try {
//            Futures.getUnchecked(removals.take());
//        } catch (InterruptedException e) {
//            throw Throwables.throwUncheckedException(e);
//        }
    }

    public void finalizeRemoveEndpoint(byte[] key, KeyValueService kvs) {
        Preconditions.checkArgument(Preconditions.checkNotNull(ring.get(key).get()) == kvs);
        version++;
        while (!removals.isEmpty()) {
            try {
                Futures.getUnchecked(removals.take());
            } catch (InterruptedException e) {
                Throwables.throwUncheckedException(e);
            }
        }
        ring.remove(key);
    }

    @Override
    public long getVersion() {
        return version;
    }

}
