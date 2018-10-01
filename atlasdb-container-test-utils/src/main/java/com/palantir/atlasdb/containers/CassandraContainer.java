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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class CassandraContainer extends Container {
    static final int CASSANDRA_PORT = 9160;
    static final String USERNAME = "cassandra";
    static final String PASSWORD = "cassandra";
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig
            .builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    private final CassandraKeyValueServiceConfig config;
    private final String dockerComposeFile;
    private final String name;

    public CassandraContainer(Class<?> callingClass) {
        this(callingClass.getSimpleName(), "/docker-compose-cassandra.yml", "cassandra");
    }

    private CassandraContainer(String keyspace, String dockerComposeFile, String name) {
        this.config = ImmutableCassandraKeyValueServiceConfig.builder()
                .addServers(forService(name))
                .keyspace(keyspace)
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username(USERNAME)
                        .password(PASSWORD)
                        .build())
                .replicationFactor(1)
                .build();
        this.dockerComposeFile = dockerComposeFile;
        this.name = name;
    }

    public static CassandraContainer secondContainer(Class<?> callingClass) {
        return new CassandraContainer(callingClass.getSimpleName(), "/docker-compose-cassandra2.yml", "cassandra2");
    }

    @Override
    public Map<String, String> getEnvironment() {
        return CassandraEnvironment.get();
    }

    @Override
    public String getDockerComposeFile() {
        return dockerComposeFile;
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> CassandraKeyValueServiceImpl.createForTesting(
                config,
                LEADER_CONFIG)
                .isInitialized());
    }

    public CassandraKeyValueServiceConfig getConfig() {
        return config;
    }

    public String getServiceName() {
        return name;
    }

    private static InetSocketAddress forService(String name) {
        return new InetSocketAddress(name, CASSANDRA_PORT);
    }
}
