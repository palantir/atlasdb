package com.palantir.atlasdb.keyvalue.partition.map;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;

public final class PartitionMapServiceImpl implements PartitionMapService {

    // TODO: Store this in KVS
    private DynamicPartitionMap partitionMap;

    public PartitionMapServiceImpl(DynamicPartitionMap partitionMap) {
    	this.partitionMap = Preconditions.checkNotNull(partitionMap);
    }

    /**
     * You need to push a new map before issuing getVersion or get
     * when using this constructor!
     */
    public PartitionMapServiceImpl() {
        this.partitionMap = null;
    }

    @Override
    public synchronized DynamicPartitionMap getMap() {
        return Preconditions.checkNotNull(partitionMap);
    }

    @Override
    public synchronized long getMapVersion() {
        return partitionMap.getVersion();
    }

    @Override
    public synchronized void updateMap(DynamicPartitionMap partitionMap) {
    	this.partitionMap = Preconditions.checkNotNull(partitionMap);
    }

}
