package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;

import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;

public class PartitionMapServiceTest {

    protected static final NavigableMap<byte[], KeyValueEndpoint> RING; static {
        RING = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        RING.put(new byte[] {0},       InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), new PartitionMapServiceImpl()));
        RING.put(new byte[] {0, 0},    InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), new PartitionMapServiceImpl()));
        RING.put(new byte[] {0, 0, 0}, InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), new PartitionMapServiceImpl()));
    }

    protected static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);
    protected DynamicPartitionMapImpl samplePartitionMap;
    protected static final long INITIAL_VERSION = 0L;

    @Before
    public void setUp() {
    	samplePartitionMap = DynamicPartitionMapImpl.create(RING);
    }

    @SuppressWarnings("deprecation")
	@Test
    public void testPms() {
        PartitionMapService pms = createPartitionMapService(samplePartitionMap);
        assertEquals(INITIAL_VERSION, pms.getVersion());
        assertEquals(samplePartitionMap, pms.get());
        samplePartitionMap.setVersion(INITIAL_VERSION + 1);
        pms.update(samplePartitionMap);
        assertEquals(INITIAL_VERSION + 1, pms.getVersion());
        assertEquals(samplePartitionMap, pms.get());
    }

    @SuppressWarnings("deprecation")
	@Test
    public void testPmsEmptyFirst() {
        PartitionMapService pms = createEmptyPartitionMapService();
        pms.update(samplePartitionMap);
        assertEquals(INITIAL_VERSION, pms.getVersion());
        assertEquals(samplePartitionMap, pms.get());
        samplePartitionMap.setVersion(INITIAL_VERSION + 1);
        pms.update(samplePartitionMap);
        assertEquals(INITIAL_VERSION + 1, pms.getVersion());
        assertEquals(samplePartitionMap, pms.get());
    }

    protected PartitionMapService createPartitionMapService(DynamicPartitionMap partitionMap) {
    	return new PartitionMapServiceImpl(DynamicPartitionMapImpl.create(RING));
    }

    protected PartitionMapServiceImpl createEmptyPartitionMapService() {
    	return new PartitionMapServiceImpl();
    }
}
