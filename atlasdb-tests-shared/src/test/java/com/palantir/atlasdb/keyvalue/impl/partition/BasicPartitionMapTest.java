package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.NavigableMap;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.BasicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;

public class BasicPartitionMapTest extends AbstractPartitionMapTest {

    PartitionMap tpm;

    @Override
    protected PartitionMap getPartitionMap(QuorumParameters qp, NavigableMap<byte[], KeyValueService> ring) {
        if (tpm == null) {
            tpm = BasicPartitionMap.create(qp, ring);
        }
        return tpm;
    }

}
