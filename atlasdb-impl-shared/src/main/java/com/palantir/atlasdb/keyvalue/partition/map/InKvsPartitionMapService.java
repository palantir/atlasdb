package com.palantir.atlasdb.keyvalue.partition.map;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.common.base.ClosableIterator;

/**
 * Only instances of {@code DynamicPartitionMapImpl} are supported by this implementation!
 *
 * @author htarasiuk
 *
 */
public final class InKvsPartitionMapService implements PartitionMapService {

    private final KeyValueService storage;

    public static final String PARTITION_MAP_TABLE = AtlasDbConstants.PARTITION_MAP_TABLE;

    private InKvsPartitionMapService(DynamicPartitionMapImpl partitionMap, KeyValueService storage) {
    	this.storage = storage;
    	storage.createTable(PARTITION_MAP_TABLE, 1234);
    	if (partitionMap != null) {
    	    updateMap(partitionMap);
    	}
    }

    private InKvsPartitionMapService(KeyValueService storage) {
        this(null, storage);
    }

    /**
     * This can be used if the partition map has been previously stored to {@code storage}
     * or if you want to first create an empty service.
     *
     * @param storage
     * @return
     */
    public static InKvsPartitionMapService create(KeyValueService storage) {
        return new InKvsPartitionMapService(storage);
    }

    /**
     * This will put the {@code partitionMap} to {@code storage} overwriting any previous
     * values stored in the {@link #PARTITION_MAP_TABLE}.
     *
     * @param storage
     * @param partitionMap
     * @return
     */
    public static InKvsPartitionMapService create(KeyValueService storage, DynamicPartitionMapImpl partitionMap) {
        Preconditions.checkNotNull(partitionMap);
        return new InKvsPartitionMapService(partitionMap, storage);
    }

    /**
     * This will use a newly created InMemoryKeyValueService to store the map.
     *
     * @param partitionMap
     * @return
     */
    public static InKvsPartitionMapService createInMemory(DynamicPartitionMapImpl partitionMap) {
        Preconditions.checkNotNull(partitionMap);
        return new InKvsPartitionMapService(partitionMap, new InMemoryKeyValueService(false));
    }

    /**
     * This will use a newly created InMemoryKeyValueService to store the map.
     *
     * @return
     */
    public static InKvsPartitionMapService createEmptyInMemory() {
        return new InKvsPartitionMapService(new InMemoryKeyValueService(false));
    }

    @Override
    public synchronized DynamicPartitionMapImpl getMap() {
        ClosableIterator<RowResult<Value>> iterator =
                storage.getRange(PARTITION_MAP_TABLE, RangeRequest.all(), 1L);
        Map<Cell, byte[]> cells = Maps.newHashMap();
        while (iterator.hasNext()) {
            RowResult<Value> row = iterator.next();
            for (Entry<Cell, Value> entry : row.getCells()) {
                assert !cells.containsKey(entry.getKey());
                cells.put(entry.getKey(), entry.getValue().getContents());
            }
        }
        iterator.close();
        return DynamicPartitionMapImpl.fromTable(cells);
    }

    @Override
    public synchronized long getMapVersion() {
        return getMap().getVersion();
    }

    /**
     * Only instances of {@code DynamicPartitionMapImpl} are supported by this implementation.
     *
     * @param partitionMap Must be instance of {@code DynamicPartitionMapImpl}.
     */
    @Override
    public synchronized void updateMap(DynamicPartitionMap partitionMap) {
        if (!(partitionMap instanceof DynamicPartitionMapImpl)) {
            throw new IllegalArgumentException("Only instances of DynamicPartitionMapImpl are supported!");
        }

        DynamicPartitionMapImpl dpmi = (DynamicPartitionMapImpl) partitionMap;
        storage.createTable(PARTITION_MAP_TABLE, 1234);
        storage.truncateTable(PARTITION_MAP_TABLE);
        storage.put(PARTITION_MAP_TABLE, dpmi.toTable(), 0L);
    }

}
