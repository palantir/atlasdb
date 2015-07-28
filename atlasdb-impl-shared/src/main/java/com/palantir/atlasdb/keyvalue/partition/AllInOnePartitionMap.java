package com.palantir.atlasdb.keyvalue.partition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.TableAwarePartitionMapApi;
import com.palantir.atlasdb.keyvalue.partition.util.RangeComparator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class AllInOnePartitionMap implements TableAwarePartitionMapApi {

    final Map<String, byte[]> tableMetadata;
    final NavigableMap<byte[], KeyValueService> ring;
    final int replicationFactor;
    final int readFactor;
    final int writeFactor;

    private AllInOnePartitionMap(int repf, int readf, int writef, Collection<KeyValueService> services, byte[][] points) {
        Preconditions.checkArgument(readf + writef > repf);
        Preconditions.checkArgument(services.size() >= repf);
        replicationFactor = repf;
        readFactor = readf;
        writeFactor = writef;
        tableMetadata = Maps.newHashMap();
        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        int i = 0;
        for (KeyValueService kvs : services) {
            ring.put(points[i++], kvs);
        }
    }

    public static AllInOnePartitionMap Create(int repf, int readf, int writef, Collection<KeyValueService> services, byte[][] points) {
        return new AllInOnePartitionMap(repf, readf, writef, services, points);
    }

    public static AllInOnePartitionMap Create(int repf, int read, int writef, int numOfServices) {
        Preconditions.checkArgument(numOfServices < 255);
        KeyValueService[] services = new KeyValueService[numOfServices];
        byte[][] points = new byte[numOfServices][];
        for (int i=0; i<numOfServices; ++i) {
            services[i] = new InMemoryKeyValueService(false);
        }
        for (int i=0; i<numOfServices; ++i) {
            points[i] = new byte[] {(byte) (i + 1)};
        }
        return new AllInOnePartitionMap(repf, read, writef, Arrays.asList(services), points);
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

    static boolean inRange(byte[] position, RangeRequest rangeRequest) {
        Preconditions.checkNotNull(rangeRequest);
        if (position == null) {
            return false;
        }
        // Unbounded case - always in range
        if (rangeRequest.getEndExclusive().length == 0) {
            return true;
        }
        int cmp = UnsignedBytes.lexicographicalComparator().compare(position, rangeRequest.getEndExclusive());
        return (rangeRequest.isReverse() && cmp > 0) || (!rangeRequest.isReverse() && cmp < 0);
    }

    @Override
    public Multimap<RangeRequest, KeyValueService> getServicesForRangeRead(String tableName,
                                                                           RangeRequest range) {
        // Just support the simple case for now
        // Preconditions.checkArgument(range.isReverse() == false);

        /* The idea here is to traverse the ring. Each interval in the
         * ring becomes a key in the resulting map.
         * The values for a given interval is repf higher keys on
         * the ring (which can be retrieved using getServicesForRead).
         */

        final TreeMultimap<RangeRequest, KeyValueService> result = TreeMultimap.create(
                RangeComparator.Instance(),
                Ordering.arbitrary());

        // This is the pointer to current position on the ring.
        byte[] key = range.getStartInclusive();

        while (inRange(key, range)) {
            byte[] endRange;
            if (range.isReverse()) {
                endRange = ring.lowerKey(key);
                if (endRange == null ||
                        UnsignedBytes.lexicographicalComparator().compare(endRange, range.getEndExclusive()) < 0) {
                    endRange = range.getEndExclusive();
                }
            } else {
                endRange = ring.higherKey(key);
                if (endRange == null ||
                        UnsignedBytes.lexicographicalComparator().compare(endRange, range.getEndExclusive()) > 0) {
                    endRange = range.getEndExclusive();
                }
            }
            RangeRequest.Builder rangeBuilder = range.isReverse() ? RangeRequest.reverseBuilder() : RangeRequest.builder();
            rangeBuilder = rangeBuilder.startRowInclusive(key);
            rangeBuilder = rangeBuilder.endRowExclusive(endRange);
            result.putAll(
                    rangeBuilder.build(),
                    getServicesForRead(tableName, key));
            // Jump to the next interval
            key = endRange;
        }

        return result;
    }

    private Iterable<? extends KeyValueService> getServicesForRead(String tableName, byte[] key) {
        // TODO Auto-generated method stub
        return null;
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

    @Override
    public Map<KeyValueService, Iterable<byte[]>> getServicesForRowsRead(String tableName,
                                                                         Iterable<byte[]> rows) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<KeyValueService, Map<Cell, Long>> getServicesForCellsRead(String tableName,
                                                                         Map<Cell, Long> timestampByCell) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<KeyValueService, Map<Cell, byte[]>> getServicesForCellsWrite(String tableName,
                                                                            Map<Cell, byte[]> values) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<KeyValueService, Multimap<Cell, Value>> getServicesForTimestampsWrite(String tableName,
                                                                                     Multimap<Cell, Value> cellValues) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<KeyValueService, Multimap<Cell, Long>> getServicesForDelete(String tableName,
                                                                           Multimap<Cell, Long> keys) {
        // TODO Auto-generated method stub
        return null;
    }
}
