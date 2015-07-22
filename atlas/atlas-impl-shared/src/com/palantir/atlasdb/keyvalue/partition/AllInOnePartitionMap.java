package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.TableAwarePartitionMapApi;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class AllInOnePartitionMap implements TableAwarePartitionMapApi {

    final Map<String, byte[]> tableMetadata;
    final NavigableMap<byte[], KeyValueService> ring;
    final int replicationFactor;
    final int readFactor;
    final int writeFactor;
    final static byte[][] points = new byte[][] {
        new byte[] {0},
        new byte[] {(byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
    };
    // This is (table name + row name + column name + timestamp)
    public final static int MAX_KEY_LEN = 1500;

    private AllInOnePartitionMap(int repf, int readf, int writef) {
        replicationFactor = repf;
        readFactor = readf;
        writeFactor = writef;
        tableMetadata = Maps.newHashMap();
        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (int i = 0; i < repf; ++i) {
            ring.put(points[i], new InMemoryKeyValueService(false));
        }
    }

    public static AllInOnePartitionMap Create() {
        return new AllInOnePartitionMap(3, 2, 2);
    }

    private Set<KeyValueService> getServiceWithKey(byte[] prefix) {
        Set<KeyValueService> result = Sets.newHashSet();
        byte[] point = ring.higherKey(prefix);
        for (int i=0; i<replicationFactor; ++i) {
            if (point == null) {
                point = ring.firstKey();
            }
            result.add(ring.get(point));
            point = ring.higherKey(point);
        }
        return result;
    }

    @Override
    public Set<KeyValueService> getServicesForRead(String tableName, byte[] row) {
        return getServiceWithKey(row);
    }

    @Override
    public Set<KeyValueService> getServicesForWrite(String tableName, byte[] row) {
        return getServicesForRead(tableName, row);
    }

    static boolean lessThan(byte[] a1, byte[] a2) {
        return UnsignedBytes.lexicographicalComparator().compare(a1, a2) < 0;
    }

    @Override
    public Multimap<RangeRequest, KeyValueService> getServicesForRangeRead(String tableName,
                                                                           RangeRequest range) {
        // Just support the simple case for now
        Preconditions.checkArgument(range.isReverse() == false);
        Preconditions.checkArgument(range.getEndExclusive().length > 0);

        final Multimap<RangeRequest, KeyValueService> result = HashMultimap.create();
        byte[] key = range.getStartInclusive();
        byte[] endRange = ring.higherKey(key);

        while (key != null && lessThan(key, range.getEndExclusive())) {
            RangeRequest currentRange;
            if (endRange != null) {
                currentRange = RangeRequest.builder()
                    .startRowInclusive(key)
                    .endRowExclusive(endRange)
                    .build();
            } else {
                currentRange = RangeRequest.builder()
                        .startRowInclusive(key)
                        .build();
            }
            result.putAll(
                    currentRange,
                    getServicesForRead(tableName, key));
            key = endRange;
            endRange = ring.higherKey(endRange);
        }

        return result;
    }

    @Override
    public Multimap<RangeRequest, KeyValueService> getServicesForRangeWrite(String tableName,
                                                                            RangeRequest range) {
        return getServicesForRangeRead(tableName, range);
    }

    @Override
    public void insertServices(String tableName, Set<KeyValueService> kvs) {
        throw new NotImplementedException();
    }

    @Override
    public void removeServices(String tableName, Set<KeyValueService> kvs) {
        throw new NotImplementedException();

    }

    @Override
    public Set<String> getAllTableNames() {
        return tableMetadata.keySet();
    }

    @Override
    public void addTable(String tableName, int maxValueSize) throws InsufficientConsistencyException {
        if (tableMetadata.containsKey(tableName)) {
            return;
        }
        // Should work for HashMap
        storeTableMetadata(tableName, null);
        for (KeyValueService kvs : getAllServices()) {
            kvs.createTable(tableName, maxValueSize);
        }
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
        for (KeyValueService kvs : getAllServices()) {
            kvs.teardown();
        }
    }

    private Set<KeyValueService> getAllServices() {
        final Set<KeyValueService> result = Sets.newHashSet();
        for (Map.Entry<byte[], KeyValueService> e : ring.entrySet()) {
            result.add(e.getValue());
        }
        return result;
    }

    @Override
    public void close() {
        throw new NotImplementedException();
    }
}
