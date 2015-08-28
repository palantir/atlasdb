package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;

import java.util.NavigableMap;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;

public abstract class AbstractPartitionMapServiceTest {

    protected abstract PartitionMapService getPartitionMapService(VersionedObject<PartitionMap> partitionMap);

    protected static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);
    protected static final NavigableMap<byte[], KeyValueEndpoint> ring; static {
        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        ring.put(new byte[] {0},       InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), new PartitionMapServiceImpl()));
        ring.put(new byte[] {0, 0},    InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), new PartitionMapServiceImpl()));
        ring.put(new byte[] {0, 0, 0}, InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), new PartitionMapServiceImpl()));
    }
    protected static final PartitionMap samplePartitionMap = DynamicPartitionMapImpl.create(ring);
    protected static final long initialVersion = 1L;

    @Test
    public void testPms() {
        PartitionMapService pms = getPartitionMapService(VersionedObject.of(samplePartitionMap, initialVersion));
        assertEquals(initialVersion, pms.getVersion());
        Object o = pms.get();
        // TODO
        //        assertEquals(samplePartitionMap, pms.get().getObject());
        pms.update(2L, samplePartitionMap);
        assertEquals(2L, pms.getVersion());
    }

}
