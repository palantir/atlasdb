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
package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;
import com.palantir.timelock.config.ImmutableDatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosTsBoundPersisterConfiguration;

public class TimeLockServerConfigurationTest {
    private static final String ADDRESS = "localhost:8701";
    private static final ClusterConfiguration CLUSTER = ImmutableClusterConfiguration.builder()
            .localServer(ADDRESS)
            .addServers(ADDRESS)
            .build();
    private static final Set<String> CLIENTS = ImmutableSet.of("client1", "client2");

    private static final TimeLockServerConfiguration CONFIGURATION_WITH_REQUEST_LIMIT =
            new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, null, null, null, true);
    private static final TimeLockServerConfiguration CONFIGURATION_WITHOUT_REQUEST_LIMIT =
            new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, null, null, null, false);
    private static final KeyValueServiceConfig POSTGRES_KVS_CONFIG = ImmutableDbKeyValueServiceConfig.builder()
            .connection(ImmutablePostgresConnectionConfig.builder()
                    .dbName("atlas")
                    .dbLogin("palantir")
                    .dbPassword(ImmutableMaskedValue.of("palantir"))
                    .host("hostName")
                    .port(5432)
                    .build())
            .ddl(ImmutablePostgresDdlConfig.builder().build())
            .build();
    public static final CassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig
            .builder()
            .addServers(new InetSocketAddress("cassandra", 9160))
            .poolSize(20)
            .keyspace("atlasdb")
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username("username")
                    .password("password")
                    .build())
            .replicationFactor(3)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .autoRefreshNodes(false)
            .build();


    @Test
    public void shouldAddDefaultConfigurationIfNotIncluded() {
        TimeLockServerConfiguration configuration = createSimpleConfig(CLUSTER, CLIENTS);
        assertThat(configuration.algorithm()).isEqualTo(ImmutablePaxosConfiguration.DEFAULT);
    }

    @Test
    public void shouldStartWithNoClients() {
        TimeLockServerConfiguration simpleConfig = createSimpleConfig(CLUSTER, ImmutableSet.of());
        assertThat(simpleConfig.clients()).isEmpty();
    }

    @Test
    public void shouldRejectClientsWithInvalidCharacters() {
        assertThatThrownBy(() -> createSimpleConfig(CLUSTER, ImmutableSet.of("/")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldRejectClientsConflictingWithInternalClients() {
        assertThatThrownBy(() -> createSimpleConfig(
                CLUSTER,
                ImmutableSet.of(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldRejectClientsWithEmptyName() {
        assertThatThrownBy(() -> createSimpleConfig(CLUSTER, ImmutableSet.of("")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldHavePositiveNumberOfAvailableThreadsWhenUsingClientRequestLimit() {
        assertThat(CONFIGURATION_WITH_REQUEST_LIMIT.availableThreads()).isGreaterThan(0);
    }

    @Test
    public void shouldRequireUseClientRequestLimitEnabledWhenCallingAvailableThreads() {
        assertThatThrownBy(CONFIGURATION_WITHOUT_REQUEST_LIMIT::availableThreads)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldUseClientRequestLimitIfTrue() {
        assertThat(CONFIGURATION_WITH_REQUEST_LIMIT.useClientRequestLimit()).isTrue();
    }

    @Test
    public void shouldNotUseClientRequestLimitIfFalse() {
        assertThat(CONFIGURATION_WITHOUT_REQUEST_LIMIT.useClientRequestLimit()).isFalse();
    }

    @Test
    public void shouldNotUseClientRequestLimitIfNotIncluded() {
        TimeLockServerConfiguration configuration = createSimpleConfig(CLUSTER, CLIENTS);
        assertThat(configuration.useClientRequestLimit()).isFalse();
    }

    @Test
    public void shouldNotEnableTimeLimiterIfNotSpecified() {
        TimeLockServerConfiguration configuration = createSimpleConfig(CLUSTER, CLIENTS);
        assertThat(configuration.timeLimiterConfiguration().enableTimeLimiting()).isFalse();
    }

    @Test
    public void shouldNotEnableDatabaseTimestampPeristerIfNotSpecified() {
        TimeLockServerConfiguration configuration = createSimpleConfig(CLUSTER, CLIENTS);
        assertThat(configuration.getTsBoundPersisterConfiguration()).isEqualTo(
                ImmutablePaxosTsBoundPersisterConfiguration.builder().build());
    }

    @Test
    public void shouldAllowDbKvsTimestampPeristerToBeSpecified() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, null, null,
                ImmutableDatabaseTsBoundPersisterConfiguration.builder()
                        .keyValueServiceConfig(POSTGRES_KVS_CONFIG)
                        .build(), null);
        assertThat(configuration.getTsBoundPersisterConfiguration())
                .isInstanceOf(ImmutableDatabaseTsBoundPersisterConfiguration.class);
    }

    @Test
    public void shouldNotAllowCassandraTimestampPeristerToBeSpecified() {
        assertThatThrownBy(() ->
                ImmutableDatabaseTsBoundPersisterConfiguration.builder()
                        .keyValueServiceConfig(CASSANDRA_KVS_CONFIG).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Only InMemory/Dbkvs is a supported for TimeLock's database persister. Found cassandra.");
    }

    private static TimeLockServerConfiguration createSimpleConfig(ClusterConfiguration cluster, Set<String> clients) {
        return new TimeLockServerConfiguration(null, cluster, clients, null, null, null, null);
    }
}
