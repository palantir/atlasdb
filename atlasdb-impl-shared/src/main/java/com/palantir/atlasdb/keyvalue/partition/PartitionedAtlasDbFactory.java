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
package com.palantir.atlasdb.keyvalue.partition;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class PartitionedAtlasDbFactory implements AtlasDbFactory<PartitionedKeyValueService> {

    @Override
    public String getType() {
        return "partitioned";
    }

    @Override
    public PartitionedKeyValueService createRawKeyValueService(JsonNode config)
            throws IOException {
        return PartitionedKeyValueService.create(createConfig(config.get("partitionedConfig")));
    }

    @Override
    public TimestampService createTimestampService(
            PartitionedKeyValueService rawKvs) {
        return PersistentTimestampService.create(PartitionedBoundStore.create(rawKvs));
    }

    private PartitionedKeyValueConfiguration createConfig(JsonNode node) {
        int repf = node.get("replicationFactor").asInt();
        int readf = node.get("readFactor").asInt();
        int writef = node.get("writeFactor").asInt();
        QuorumParameters parameters = new QuorumParameters(repf, readf, writef);

        List<PartitionMapService> partitionMapProviders = Lists.newArrayList();
        Iterator<JsonNode> endpointsIterator = node.get("partitionMapProviders").elements();
        while (endpointsIterator.hasNext()) {
            String pmsUri = endpointsIterator.next().asText();
            partitionMapProviders.add(RemotingPartitionMapService.createClientSide(pmsUri));
        }

        int partitionMapProvidersReadFactor = node.get("partitionMapProvidersReadFactor").asInt();

        return PartitionedKeyValueConfiguration.of(parameters, partitionMapProviders, partitionMapProvidersReadFactor);
    }

}
