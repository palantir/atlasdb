/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.backend;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public class CassandraKeyValueServiceInstrumentation extends KeyValueServiceInstrumentation {

    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueServiceInstrumentation.class);

    public CassandraKeyValueServiceInstrumentation() {
        super(9160, "cassandra-docker-compose.yml");
    }

    @Override
    public KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr) {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .addServers(addr)
                .poolSize(20)
                .keyspace("atlasdb")
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username("cassandra")
                        .password("cassandra")
                        .build())
                .ssl(false)
                .replicationFactor(1)
                .mutationBatchCount(10000)
                .mutationBatchSizeBytes(10000000)
                .fetchBatchCount(1000)
                .safetyDisabled(false)
                .autoRefreshNodes(false)
                .build();
    }

    @Override
    public boolean canConnect(InetSocketAddress addr) {
        try {
            CassandraKeyValueService.create(
                    CassandraKeyValueServiceConfigManager.createSimpleManager(
                            (CassandraKeyValueServiceConfig) getKeyValueServiceConfig(addr)),
                    Optional.of(ImmutableLeaderConfig.builder()
                            .quorumSize(1)
                            .localServer(addr.getHostString())
                            .leaders(ImmutableSet.of(addr.getHostString()))
                            .build()));
            return true;
        } catch (Exception e) {
            log.error("Unable to create Cassandra KVS", e);
            return false;
        }
    }

    @Override
    public String toString() {
        return "CASSANDRA";
    }
}
