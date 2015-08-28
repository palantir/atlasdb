package com.palantir.atlasdb.keyvalue.partition;

import org.assertj.core.util.Preconditions;

import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;

public final class PartitionMapServiceImpl implements PartitionMapService {

    public PartitionMapServiceImpl(PartitionMap partitionMap, long version) {
        Preconditions.checkNotNull(partitionMap);
        this.partitionMap = VersionedObject.of(partitionMap, version);
    }

    public PartitionMapServiceImpl() {
        this.partitionMap = VersionedObject.of(null, 0L);
    }

    VersionedObject<PartitionMap> partitionMap;

    @Override
    public synchronized VersionedObject<PartitionMap> get() {
        Preconditions.checkNotNull(partitionMap.getObject());
        return partitionMap;
    }

    @Override
    public synchronized long getVersion() {
        return partitionMap.getVersion();
    }

    @Override
    public synchronized void update(long version, PartitionMap partitionMap) {
        this.partitionMap = VersionedObject.of(
                Preconditions.checkNotNull(partitionMap), version);
    }

}
