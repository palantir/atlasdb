/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.containers;

import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import java.net.InetSocketAddress;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreeNodeCassandraCluster extends Container {
    private static final Logger log = LoggerFactory.getLogger(ThreeNodeCassandraCluster.class);

    private static final CassandraVersion CASSANDRA_VERSION = CassandraVersion.fromEnvironment();

    public static final String CLI_CONTAINER_NAME = "cli";
    public static final String FIRST_CASSANDRA_CONTAINER_NAME = "cassandra1";
    public static final String SECOND_CASSANDRA_CONTAINER_NAME = "cassandra2";
    public static final String THIRD_CASSANDRA_CONTAINER_NAME = "cassandra3";
    private static final CassandraCredentialsConfig CREDENTIALS =
            ImmutableCassandraCredentialsConfig.builder()
                    .username("username")
                    .password("password")
                    .build();

    public static final CassandraKeyValueServiceConfig KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .servers(ImmutableDefaultConfig.builder().addThriftHosts(
                    new InetSocketAddress(FIRST_CASSANDRA_CONTAINER_NAME, CassandraContainer.CASSANDRA_THRIFT_PORT),
                    new InetSocketAddress(SECOND_CASSANDRA_CONTAINER_NAME, CassandraContainer.CASSANDRA_THRIFT_PORT),
                    new InetSocketAddress(THIRD_CASSANDRA_CONTAINER_NAME, CassandraContainer.CASSANDRA_THRIFT_PORT))
                    .build())
            .poolSize(20)
            .keyspace("atlasdb")
            .replicationFactor(3)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .autoRefreshNodes(false)
            .credentials(CREDENTIALS)
            .build();

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
                return new ThreeNodeCassandraClusterOperations(rule,
                        CASSANDRA_VERSION).nodetoolShowsThreeCassandraNodesUp() && canCreateCassandraKeyValueService();

            } catch (Exception e) {
                log.info("Exception while checking if the Cassandra cluster was ready", e);
                return false;
            }
        });
    }

    private static boolean canCreateCassandraKeyValueService() {
        return CassandraKeyValueServiceImpl.createForTesting(KVS_CONFIG)
                .isInitialized();
    }
}
