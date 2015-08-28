package com.palantir.atlasdb.keyvalue.partition.map;

import org.assertj.core.util.Preconditions;

import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;

public final class PartitionMapServiceImpl implements PartitionMapService {

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
    public synchronized DynamicPartitionMap get() {
        return Preconditions.checkNotNull(partitionMap);
    }

    @Override
    public synchronized long getVersion() {
        return partitionMap.getVersion();
    }

    @Override
    public synchronized void update(DynamicPartitionMap partitionMap) {
    	this.partitionMap = Preconditions.checkNotNull(partitionMap);
    }

}
