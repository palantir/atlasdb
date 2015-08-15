package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.NavigableMap;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;

public class DynamicPartitionMapTest extends AbstractPartitionMapTest {

    private DynamicPartitionMap dpm;

    @Override
    protected PartitionMap getPartitionMap(QuorumParameters qp,
                                           NavigableMap<byte[], KeyValueService> ring) {
        if (dpm == null) {
            dpm = new DynamicPartitionMapImpl(qp, ring);
            for (int i = 0 ; i < 4 ; ++i) {
                System.err.println(i);
                dpm.removeEndpoint(points[i], services.get(i), TABLE1);
            }
        }
        return dpm;
    }

}
