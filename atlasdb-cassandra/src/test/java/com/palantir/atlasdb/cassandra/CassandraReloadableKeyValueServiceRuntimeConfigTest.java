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
package com.palantir.atlasdb.cassandra;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import org.junit.Test;

public class CassandraReloadableKeyValueServiceRuntimeConfigTest {

    private static final CassandraServersConfig SERVERS_1 = ImmutableDefaultConfig.builder()
            .addThriftHosts(InetSocketAddress.createUnresolved("host1", 5000))
            .addThriftHosts(InetSocketAddress.createUnresolved("host2", 5000))
            .build();
    private static final CassandraServersConfig SERVERS_2 = ImmutableDefaultConfig.builder()
            .addThriftHosts(InetSocketAddress.createUnresolved("host3", 5000))
            .addThriftHosts(InetSocketAddress.createUnresolved("host4", 5000))
            .build();

    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();

    private static final CassandraKeyValueServiceRuntimeConfig RUNTIME_CONFIG_WITH_DEFAULT_SERVERS =
            runtimeConfigBuilderWithDefaultServers().build();

    @Test
    public void unresponsiveHostBackoffTimeSecondsAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .unresponsiveHostBackoffTimeSeconds(5)
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServers()
                        .unresponsiveHostBackoffTimeSeconds(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().unresponsiveHostBackoffTimeSeconds()).isEqualTo(10);
    }

    @Test
    public void mutationBatchCountAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .mutationBatchCount(5)
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig = Refreshable.only(
                runtimeConfigBuilderWithDefaultServers().mutationBatchCount(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchCount()).isEqualTo(10);
    }

    @Test
    public void mutationBatchSizeBytesAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .mutationBatchSizeBytes(5)
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServers()
                        .mutationBatchSizeBytes(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchSizeBytes()).isEqualTo(10);
    }

    @Test
    public void fetchBatchCountAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .fetchBatchCount(5)
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig = Refreshable.only(
                runtimeConfigBuilderWithDefaultServers().fetchBatchCount(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().fetchBatchCount()).isEqualTo(10);
    }

    @Test
    public void sweepReadThreadsAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .sweepReadThreads(5)
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig = Refreshable.only(
                runtimeConfigBuilderWithDefaultServers().sweepReadThreads(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().sweepReadThreads()).isEqualTo(10);
    }

    @Test
    public void mergedConfigPrioritisesInstallForServers() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .servers(SERVERS_1)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().servers()).isEqualTo(SERVERS_1);
    }

    @Test
    public void mergedConfigDelegatesToRuntimeServersIfInstallIsEmpty() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentialsAndReplicationFactor().build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().servers()).isEqualTo(SERVERS_2);
    }

    @Test
    public void mergedConfigServersEmptyFailsInitialisation() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentialsAndReplicationFactor().build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(ImmutableDefaultConfig.of()).build();

        assertThatLoggableExceptionThrownBy(() -> CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(runtimeConfig)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("'servers' must have at least one defined host");
    }

    @Test
    public void mergedConfigPrioritisesInstallForConcurrentGetRangesThreadPoolSize() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .concurrentGetRangesThreadPoolSize(1234)
                .build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS));

        assertThat(reloadableConfig.get().concurrentGetRangesThreadPoolSize()).isEqualTo(1234);
    }

    @Test
    public void mergedConfigDerivesThreadPoolSizeIfInstallIsEmpty() {
        int poolSize = 4321;
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .poolSize(poolSize)
                .build();
        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS));

        assertThat(reloadableConfig.get().concurrentGetRangesThreadPoolSize())
                .isEqualTo(poolSize * SERVERS_1.numberOfThriftHosts());
    }

    @Test
    public void mergedConfigPrioritisesInstallForDefaultGetRangesConcurrency() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .concurrentGetRangesThreadPoolSize(1243)
                .build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS));

        assertThat(reloadableConfig.get().defaultGetRangesConcurrency())
                .isEqualTo(config.defaultGetRangesConcurrency());
    }

    @Test
    public void mergedConfigDerivesDefaultGetRangesConcurrencyIfInstallIsEmpty() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentialsAndReplicationFactor()
                .poolSize(10)
                .build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS));

        assertThat(reloadableConfig.get().defaultGetRangesConcurrency())
                .isEqualTo(8);
    }

    private static ImmutableCassandraKeyValueServiceConfig.Builder
            configBuilderWithDefaultCredentialsAndReplicationFactor() {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .credentials(CREDENTIALS)
                .replicationFactor(1);
    }

    private static ImmutableCassandraKeyValueServiceRuntimeConfig.Builder runtimeConfigBuilder() {
        return ImmutableCassandraKeyValueServiceRuntimeConfig.builder();
    }

    private static ImmutableCassandraKeyValueServiceRuntimeConfig.Builder runtimeConfigBuilderWithDefaultServers() {
        return runtimeConfigBuilder().servers(SERVERS_1);
    }
}
