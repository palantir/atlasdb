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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class ThreeNodeCassandraCluster extends Container {
    private static final Logger log = LoggerFactory.getLogger(ThreeNodeCassandraCluster.class);

    private static final CassandraVersion CASSANDRA_VERSION = CassandraVersion.fromEnvironment();

    public static final String CLI_CONTAINER_NAME = "cli";
    public static final String FIRST_CASSANDRA_CONTAINER_NAME = "cassandra1";
    public static final String SECOND_CASSANDRA_CONTAINER_NAME = "cassandra2";
    public static final String THIRD_CASSANDRA_CONTAINER_NAME = "cassandra3";

    public static final CassandraKeyValueServiceConfig KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress(FIRST_CASSANDRA_CONTAINER_NAME, CassandraContainer.CASSANDRA_PORT))
            .addServers(new InetSocketAddress(SECOND_CASSANDRA_CONTAINER_NAME, CassandraContainer.CASSANDRA_PORT))
            .addServers(new InetSocketAddress(THIRD_CASSANDRA_CONTAINER_NAME, CassandraContainer.CASSANDRA_PORT))
            .poolSize(20)
            .keyspace("atlasdb")
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username(CassandraContainer.USERNAME)
                    .password(CassandraContainer.PASSWORD)
                    .build())
            .replicationFactor(3)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .autoRefreshNodes(false)
            .build();

    public static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig
            .builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-cassandra-three-node.yml";
    }

    @Override
    public Map<String, String> getEnvironment() {
        return CassandraEnvironment.get();
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> {

            try {
                ThreeNodeCassandraClusterOperations cassandraOperations =
                        new ThreeNodeCassandraClusterOperations(rule, CASSANDRA_VERSION);

                if (!cassandraOperations.nodetoolShowsThreeCassandraNodesUp()) {
                    return false;
                }

                // slightly hijacking the isReady function here - using it
                // to actually modify the cluster
                cassandraOperations.replicateSystemAuthenticationDataOnAllNodes();

                return canCreateCassandraKeyValueService();
            } catch (Exception e) {
                log.info("Exception while checking if the Cassandra cluster was ready", e);
                return false;
            }
        });
    }

    private static boolean canCreateCassandraKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                KVS_CONFIG,
                LEADER_CONFIG,
                () -> 0L)
                .isInitialized();
    }
}
