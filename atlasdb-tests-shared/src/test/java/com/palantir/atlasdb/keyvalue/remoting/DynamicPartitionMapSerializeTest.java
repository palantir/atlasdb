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
package com.palantir.atlasdb.keyvalue.remoting;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.common.concurrent.PTExecutors;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class DynamicPartitionMapSerializeTest {

    static final KeyValueService endpointKvs = new InMemoryKeyValueService(false);
    static final PartitionMapService endpointPms = InMemoryPartitionMapService.createEmpty();
    static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);
    static final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    @Rule public final DropwizardClientRule endpointKvsService = new DropwizardClientRule(endpointKvs);
    @Rule public final DropwizardClientRule endpointPmsService = new DropwizardClientRule(endpointPms);

    static final int NUM_EPTS = 3;
    KeyValueEndpoint[] endpoint = new KeyValueEndpoint[NUM_EPTS];
    DynamicPartitionMapImpl partitionMap;

    @Before
    public void setUp() {
        for (int i=0; i<NUM_EPTS; ++i) {
            endpoint[i] = new SimpleKeyValueEndpoint(endpointKvsService.baseUri().toString(), endpointPmsService.baseUri().toString());
        }
        NavigableMap<byte[], KeyValueEndpoint> ring = ImmutableSortedMap
                .<byte[], KeyValueEndpoint>orderedBy(UnsignedBytes.lexicographicalComparator())
                .put(new byte[] {0}, endpoint[0])
                .put(new byte[] {0, 0}, endpoint[1])
                .put(new byte[] {0, 0, 0}, endpoint[2])
                .build();
        partitionMap = DynamicPartitionMapImpl.create(QUORUM_PARAMETERS, ring, PTExecutors.newCachedThreadPool());
    }

    @Test
    public void testSerialize() throws JsonProcessingException {
        System.err.println(mapper.writeValueAsString(partitionMap));
    }

    @Test
    public void testDeserialize() throws IOException {
        String asString = mapper.writeValueAsString(partitionMap);
        PartitionMap deserialized = mapper.readValue(asString, DynamicPartitionMapImpl.class);
        System.err.println(deserialized);
        System.err.println(partitionMap);
        assertEquals(partitionMap, deserialized);
    }
}
