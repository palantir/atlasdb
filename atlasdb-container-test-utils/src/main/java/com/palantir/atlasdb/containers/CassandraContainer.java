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
package com.palantir.atlasdb.containers;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraJmxCompactionConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class CassandraContainer extends Container {
    public static final int THRIFT_PORT = CassandraConstants.DEFAULT_THRIFT_PORT;
    public static final int CQL_PORT = CassandraConstants.DEFAULT_CQL_PORT;
    public static final String USERNAME = "cassandra";
    public static final String PASSWORD = "cassandra";

    private static final ImmutableCassandraKeyValueServiceConfig.Builder SHARED_CONFIG =
            ImmutableCassandraKeyValueServiceConfig.builder()
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
                    .safetyDisabled(false)
                    .autoRefreshNodes(false)
                    .jmx(ImmutableCassandraJmxCompactionConfig.builder()
                            .username(USERNAME)
                            .password(PASSWORD)
                            .build());

    public static final CassandraKeyValueServiceConfig THRIFT_CONFIG =
            SHARED_CONFIG
                    .servers(ImmutableList.of(new InetSocketAddress("cassandra", THRIFT_PORT)))
                    .build();

    public static final CassandraKeyValueServiceConfig CQL_CONFIG =
            SHARED_CONFIG
                    .servers(ImmutableList.of(new InetSocketAddress("cassandra", CQL_PORT)))
                    .build();

    public static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig.builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    @Override
    public Map<String, String> getEnvironment() {
        return CassandraVersion.getEnvironment();
    }

    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-cassandra.yml";
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> {
            testWithBothThriftAndCql().forEach(Supplier::get);
            return true;
        });
    }

    public static Iterable<Supplier<KeyValueService>> testWithBothThriftAndCql() {
        return Arrays.asList(
                () -> CassandraKeyValueService.create(
                        CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.THRIFT_CONFIG),
                        CassandraContainer.LEADER_CONFIG),
                () -> CQLKeyValueService.create(
                        CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.CQL_CONFIG))
                );
    }
}
