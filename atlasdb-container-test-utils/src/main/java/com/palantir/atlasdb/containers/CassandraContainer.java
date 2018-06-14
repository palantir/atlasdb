/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.containers;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraJmxCompactionConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class CassandraContainer extends Container {

    public static final int CASSANDRA_PORT = 9160;
    public static final String USERNAME = "cassandra";
    public static final String PASSWORD = "cassandra";

    public static final InetSocketAddress server = new InetSocketAddress("cassandra", CASSANDRA_PORT);
    public static final CassandraKeyValueServiceConfig KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(server)
            .addressTranslation(ImmutableMap.of("172.24.0.2", server))
            .poolSize(20)
            .keyspace("atlasdb")
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username(USERNAME)
                    .password(PASSWORD)
                    .build())
            .replicationFactor(1)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .jmx(ImmutableCassandraJmxCompactionConfig.builder()
                    .username(USERNAME)
                    .password(PASSWORD)
                    .build())
            .build();

    public static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig
            .builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    @Override
    public Map<String, String> getEnvironment() {
        return CassandraEnvironment.get();
    }

    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-cassandra.yml";
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> CassandraKeyValueServiceImpl.createForTesting(
                KVS_CONFIG,
                LEADER_CONFIG)
                .isInitialized());
    }
}
