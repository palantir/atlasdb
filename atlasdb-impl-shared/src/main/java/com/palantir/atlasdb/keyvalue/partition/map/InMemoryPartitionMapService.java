package com.palantir.atlasdb.keyvalue.partition.map;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;

public class InMemoryPartitionMapService implements PartitionMapService {

    private DynamicPartitionMap partitionMap;

    private InMemoryPartitionMapService(DynamicPartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }

    public static InMemoryPartitionMapService create(DynamicPartitionMap partitionMap) {
        return new InMemoryPartitionMapService(Preconditions.checkNotNull(partitionMap));
    }

    /**
     * You must store a map using {@link #updateMap(DynamicPartitionMap)} before
     * calling {@link #getMap()} or {@link #getMapVersion()}.
     *
     */
    public static InMemoryPartitionMapService createEmpty() {
        return new InMemoryPartitionMapService(null);
    }

    @Override
    public DynamicPartitionMap getMap() {
        return Preconditions.checkNotNull(partitionMap);
    }

    @Override
    public long getMapVersion() {
        return partitionMap.getVersion();
    }

    @Override
    public void updateMap(DynamicPartitionMap partitionMap) {
        this.partitionMap = Preconditions.checkNotNull(partitionMap);
    }

}
