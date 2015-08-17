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
package com.palantir.atlasdb.cassandra.spi;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class CassandraAtlasServerFactory implements AtlasDbFactory<CassandraKeyValueService> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getType() {
        return "cassandra";
    }

    @Override
    public CassandraKeyValueService createRawKeyValueService(JsonNode config) throws IOException {
        CassandraKeyValueConfiguration typedConfig =
                mapper.treeToValue(config, CassandraKeyValueConfiguration.class);
        return createKv(typedConfig);
    }

    @Override
    public TimestampService createTimestampService(CassandraKeyValueService rawKvs) {
        return PersistentTimestampService.create(CassandraTimestampBoundStore.create(rawKvs));
    }

    private static CassandraKeyValueService createKv(CassandraKeyValueConfiguration config) {
        return CassandraKeyValueService.create(ImmutableSet.copyOf(config.servers),
                config.port,
                config.poolSize,
                config.keyspace,
                config.isSsl,
                config.replicationFactor,
                config.mutationBatchCount,
                config.mutationBatchSizeBytes,
                config.fetchBatchCount,
                config.safetyDisabled,
                config.autoRefreshNodes);
    }
}
