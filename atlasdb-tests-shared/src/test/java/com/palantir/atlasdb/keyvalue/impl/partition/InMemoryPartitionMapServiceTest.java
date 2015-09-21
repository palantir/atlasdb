/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.palantir.atlasdb.keyvalue.partition.map.InKvsPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.common.concurrent.PTExecutors;

public class InMemoryPartitionMapServiceTest {

    protected static final NavigableMap<byte[], KeyValueEndpoint> RING; static {
        RING = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        RING.put(new byte[] {0},       InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InKvsPartitionMapService.createEmptyInMemory()));
        RING.put(new byte[] {0, 0},    InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InKvsPartitionMapService.createEmptyInMemory()));
        RING.put(new byte[] {0, 0, 0}, InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InKvsPartitionMapService.createEmptyInMemory()));
    }

    protected static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);
    protected DynamicPartitionMapImpl samplePartitionMap;
    protected static final long INITIAL_VERSION = 0L;

    @Before
    public void setUp() {
        samplePartitionMap = DynamicPartitionMapImpl.create(QUORUM_PARAMETERS, RING, PTExecutors.newCachedThreadPool());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPms() {
        PartitionMapService pms = createPartitionMapService(samplePartitionMap);
        assertEquals(INITIAL_VERSION, pms.getMapVersion());
        assertEquals(samplePartitionMap, pms.getMap());
        samplePartitionMap.setVersion(INITIAL_VERSION + 1);
        pms.updateMap(samplePartitionMap);
        assertEquals(INITIAL_VERSION + 1, pms.getMapVersion());
        assertEquals(samplePartitionMap, pms.getMap());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPmsEmptyFirst() {
        PartitionMapService pms = createEmptyPartitionMapService();
        pms.updateMap(samplePartitionMap);
        assertEquals(INITIAL_VERSION, pms.getMapVersion());
        assertEquals(samplePartitionMap, pms.getMap());
        samplePartitionMap.setVersion(INITIAL_VERSION + 1);
        pms.updateMap(samplePartitionMap);
        assertEquals(INITIAL_VERSION + 1, pms.getMapVersion());
        assertEquals(samplePartitionMap, pms.getMap());
    }

    protected PartitionMapService createPartitionMapService(DynamicPartitionMap partitionMap) {
        return InMemoryPartitionMapService.create(partitionMap);
    }

    protected PartitionMapService createEmptyPartitionMapService() {
        return InMemoryPartitionMapService.createEmpty();
    }
}
