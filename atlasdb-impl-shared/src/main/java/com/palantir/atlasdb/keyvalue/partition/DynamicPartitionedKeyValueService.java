package com.palantir.atlasdb.keyvalue.partition;

import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.common.concurrent.PTExecutors;

public class DynamicPartitionedKeyValueService extends PartitionedKeyValueService {

    PartitionMap partitionMap;

    public DynamicPartitionedKeyValueService(PartitionMap partitionMap) {
        super(PTExecutors.newCachedThreadPool(), new QuorumParameters(3, 3, 3));
        this.partitionMap = partitionMap;
    }

    @Override
    protected PartitionMap getPartitionMap() {
        return partitionMap;
    }

}
