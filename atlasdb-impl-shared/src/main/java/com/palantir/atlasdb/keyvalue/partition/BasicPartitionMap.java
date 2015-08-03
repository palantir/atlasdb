package com.palantir.atlasdb.keyvalue.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequest.Builder;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.partition.api.TableAwarePartitionMapApi;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeComparator;
import com.palantir.common.annotation.Idempotent;


public final class BasicPartitionMap implements TableAwarePartitionMapApi {

    private final Map<String, byte[]> tableMetadata;
    private final QuorumParameters quorumParameters;
    private final CycleMap<byte[], KeyValueService> ring;
    private static final byte[] EMPTY_METADATA = new byte[0];

    //*** Construction ****************************************************************************
    private BasicPartitionMap(QuorumParameters quorumParameters, NavigableMap<byte[], KeyValueService> ring) {
        Preconditions.checkArgument(quorumParameters.getReplicationFactor() <= ring.keySet().size());
        // Careful with instruction order here
        this.quorumParameters = quorumParameters;
        this.ring = CycleMap.wrap(ring);
        Preconditions.checkArgument(isRingValid());
        this.tableMetadata = Maps.newHashMap();
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

    private static boolean inRange(byte[] position, RangeRequest rangeRequest) {
        boolean reverse = rangeRequest.isReverse();
        int cmpStart = ConsistentRingRangeComparator.compareBytes(position, rangeRequest.getStartInclusive(), reverse);
        int cmpEnd = ConsistentRingRangeComparator.compareBytes(position, rangeRequest.getEndExclusive(), reverse);
        if (reverse) {
            return cmpStart <= 0 && cmpEnd > 0;
        } else {
            return cmpStart >= 0 && cmpEnd < 0;
        }
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
    public Set<String> getAllTableNames() {
        return tableMetadata.keySet();
    }

    @Override @Idempotent
    public void createTable(String tableName, int maxValueSize) throws InsufficientConsistencyException {
        if (tableMetadata.containsKey(tableName)) {
            return;
        }
        for (KeyValueService kvs : getAllServices()) {
            kvs.createTable(tableName, maxValueSize);
        }
        storeTableMetadata(tableName, EMPTY_METADATA);
    }

    @Override
    public void dropTable(String tableName) throws InsufficientConsistencyException {
        for (KeyValueService kvs : getAllServices()) {
            kvs.dropTable(tableName);
        }
        tableMetadata.remove(tableName);
    }

    @Override
    public void truncateTable(String tableName) throws InsufficientConsistencyException {
        for (KeyValueService kvs : getAllServices()) {
            kvs.truncateTable(tableName);
        }
    }

    @Override
    public void truncateTables(Set<String> tableNamess) throws InsufficientConsistencyException {
        for (KeyValueService kvs : getAllServices()) {
            kvs.truncateTables(tableNamess);
        }
    }

    @Override
    public void storeTableMetadata(String tableName, byte[] metadata) {
        tableMetadata.put(tableName, metadata);
    }

    @Override
    public byte[] getTableMetadata(String tableName) {
        return tableMetadata.get(tableName);
    }

    @Override
    public Map<String, byte[]> getTablesMetadata() {
        return Maps.newHashMap(tableMetadata);
    }

    @Override
    public void tearDown() {
        // TODO: Do I need a deep copy?
        for (KeyValueService kvs : getAllServices()) {
            kvs.teardown();
        }
    }

    private Set<KeyValueService> getAllServices() {
        return Sets.newHashSet(ring.values());
    }

    @Override
    public void close() {
        for (KeyValueService keyValueService : getAllServices()) {
            keyValueService.close();
        }
    }

    @Override
    public Multimap<ConsistentRingRangeRequest, KeyValueService> getServicesForRangeRead(String tableName,
                                                                                         RangeRequest range) {
        ListMultimap<ConsistentRingRangeRequest, KeyValueService> result = Multimaps.newListMultimap(
                Maps.<ConsistentRingRangeRequest, ConsistentRingRangeRequest, Collection<KeyValueService>>newTreeMap(ConsistentRingRangeComparator.instance()),
                new Supplier<List<KeyValueService>>() {
                    @Override
                    public List<KeyValueService> get() {
                        return new ArrayList<KeyValueService>();
                    }
                });

        // This is either the original ring, or its reversed view (in case of reversed range req)
        CycleMap<byte[], KeyValueService> rangeRing = ring;
        if (range.isReverse()) {
            rangeRing = rangeRing.descendingMap();
        }

        byte[] key = new byte[0];
        if (range.getStartInclusive().length > 0) {
            key = range.getStartInclusive();
        }

        // Note that there is no wrapping around when travering the circle with the key.
        // Wrap around can happen when gathering further kvss for a given key.
        while (key != null && inRange(key, range)) {
            Set<KeyValueService> services = Sets.newHashSet();

            // Setup the range
            Builder builder = range.isReverse() ? RangeRequest.reverseBuilder() : RangeRequest.builder();
            builder = builder.startRowInclusive(key);
            if (rangeRing.higherKey(key) != null) {
                builder = builder.endRowExclusive(rangeRing.nextKey(key));
            } else {
                // unbounded
            }
            ConsistentRingRangeRequest crrr = ConsistentRingRangeRequest.of(builder.build());

            // Find kvss having this range
            byte[] kvsKey = rangeRing.nextKey(key);
            for (int i = 0; i < quorumParameters.getReplicationFactor(); ++i) {
                services.add(rangeRing.get(kvsKey));
                kvsKey = rangeRing.nextKey(kvsKey);
            }

            result.putAll(crrr, services);

            // Proceed with next range
            key = rangeRing.higherKey(key);
        }

        return result;
    }

    @Override
    public Map<KeyValueService, ? extends Iterable<byte[]>> getServicesForRowsRead(String tableName,
                                                                         Iterable<byte[]> rows) {
        Map<KeyValueService, Set<byte[]>> result = Maps.newHashMap();
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
                                                                   Set<Cell> cells,
                                                                   long timestamp) {
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

    @Override
    public Map<KeyValueService, Multimap<Cell, Value>> getServicesForTimestampsWrite(String tableName,
                                                                                     Multimap<Cell, Value> cellValues) {
        Map<KeyValueService, Multimap<Cell, Value>> result = Maps.newHashMap();
        for (Map.Entry<Cell, Value> e : cellValues.entries()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueService kvs: services) {
                if (!result.containsKey(kvs)) {
                    result.put(kvs, HashMultimap.<Cell, Value>create());
                }
                assert(!result.get(kvs).containsEntry(e.getKey(), e.getValue()));
                result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public Map<KeyValueService, Multimap<Cell, Long>> getServicesForDelete(String tableName,
                                                                           Multimap<Cell, Long> keys) {
        Map<KeyValueService, Multimap<Cell, Long>> result = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : keys.entries()) {
            Set<KeyValueService> services = getServicesHavingRow(e.getKey().getRowName());
            for (KeyValueService kvs : services) {
               if (!result.containsKey(kvs)) {
                   result.put(kvs, HashMultimap.<Cell, Long>create());
               }
               assert(!result.get(kvs).containsEntry(e.getKey(), e.getValue()));
               result.get(kvs).put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public Set<? extends KeyValueService> getDelegates() {
        return getAllServices();
    }

    @Override
    public void compactInternally(String tableName) {
        for (KeyValueService kvs : getAllServices()) {
            kvs.compactInternally(tableName);
        }
    }
}
