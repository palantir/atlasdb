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

import com.datastax.driver.core.Cluster;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCqlCapableConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.async.DefaultCassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.DefaultCqlClientFactory;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.logsafe.Preconditions;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class CassandraContainer extends Container {
    static final int CASSANDRA_CQL_PORT = 9042;
    static final int CASSANDRA_THRIFT_PORT = 9160;
    static final String USERNAME = "cassandra";
    static final String PASSWORD = "cassandra";
    private static final String CONTAINER_NAME = "cassandra";
    private static final String THROWAWAY_CONTAINER_NAME = "cassandra2";

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig.builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    private final CassandraKeyValueServiceConfig config;
    private final String dockerComposeFile;
    private final String name;

    public CassandraContainer() {
        this("/docker-compose-cassandra.yml", CONTAINER_NAME);
    }

    private CassandraContainer(String dockerComposeFile, String name) {
        String keyspace = UUID.randomUUID().toString().replace("-", "_");
        this.config = ImmutableCassandraKeyValueServiceConfig.builder()
                .servers(ImmutableCqlCapableConfig.builder()
                        .addCqlHosts(new InetSocketAddress(name, CASSANDRA_CQL_PORT))
                        .addThriftHosts(new InetSocketAddress(name, CASSANDRA_THRIFT_PORT))
                        .build())
                .keyspace(keyspace)
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username(USERNAME)
                        .password(PASSWORD)
                        .build())
                .poolSize(20)
                .mutationBatchCount(10000)
                .mutationBatchSizeBytes(10000000)
                .fetchBatchCount(1000)
                .replicationFactor(1)
                .build();
        this.dockerComposeFile = dockerComposeFile;
        this.name = name;
    }

    public static CassandraContainer throwawayContainer() {
        return new CassandraContainer("/docker-compose-cassandra2.yml", THROWAWAY_CONTAINER_NAME);
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
        try (CassandraKeyValueService cassandraKeyValueService = CassandraKeyValueServiceImpl.createForTesting(
                getConfigWithProxy(Containers.getSocksProxy(name).address()))) {
            return SuccessOrFailure.onResultOf(cassandraKeyValueService::isInitialized);
        } catch (Exception e) {
            return SuccessOrFailure.failure(e.getMessage());
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CassandraContainer && name.equals(((CassandraContainer) other).getServiceName());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public CassandraKeyValueServiceConfig getConfig() {
        return config;
    }

    public CassandraKeyValueServiceConfig getConfigWithProxy(SocketAddress proxyAddress) {
        Preconditions.checkState(config.servers() instanceof CqlCapableConfig, "Has to be CqlCapableConfig");
        CqlCapableConfig cqlCapableConfig = (CqlCapableConfig) config.servers();

        return ImmutableCassandraKeyValueServiceConfig.builder()
                .from(config)
                .servers(ImmutableCqlCapableConfig.builder()
                        .from(cqlCapableConfig)
                        .build())
                .asyncKeyValueServiceFactory(
                        new DefaultCassandraAsyncKeyValueServiceFactory(new DefaultCqlClientFactory(
                                () -> Cluster.builder().withNettyOptions(new SocksProxyNettyOptions(proxyAddress)))))
                .build();
    }

    public String getServiceName() {
        return name;
    }
}
