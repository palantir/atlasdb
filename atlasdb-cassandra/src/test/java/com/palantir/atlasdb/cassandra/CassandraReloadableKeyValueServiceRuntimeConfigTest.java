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
import static org.mockito.Mockito.mock;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.spi.ImmutableSharedResourcesConfig;
import com.palantir.atlasdb.spi.LocalConnectionConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

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
            runtimeConfigBuilderWithDefaultServersAndReplicationFactor().build();

    // The following 5 tests are present to ensure we have back-compat with the previous merging reloadable config
    // which, for the following properties, prioritised the runtime config value instead of the install config value.
    @Test
    public void unresponsiveHostBackoffTimeSecondsUsesInstallIfPresent() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentials()
                .unresponsiveHostBackoffTimeSeconds(5)
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .unresponsiveHostBackoffTimeSeconds(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().unresponsiveHostBackoffTimeSeconds()).isEqualTo(5);
    }

    @Test
    public void unresponsiveHostBackoffTimeSecondsUsesRuntimeIfInstallNotPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .unresponsiveHostBackoffTimeSeconds(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().unresponsiveHostBackoffTimeSeconds()).isEqualTo(10);
    }

    @Test
    public void mutationBatchCountUsesInstallIfPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().mutationBatchCount(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .mutationBatchCount(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchCount()).isEqualTo(5);
    }

    @Test
    public void mutationBatchCountUsesRuntimeIfInstallNotPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .mutationBatchCount(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchCount()).isEqualTo(10);
    }

    @Test
    public void mutationBatchSizeBytesUsesInstallIfPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().mutationBatchSizeBytes(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .mutationBatchSizeBytes(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchSizeBytes()).isEqualTo(5);
    }

    @Test
    public void mutationBatchSizeBytesUsesRuntimeIfInstallNotPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .mutationBatchSizeBytes(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchSizeBytes()).isEqualTo(10);
    }

    @Test
    public void fetchBatchCountUsesInstallIfPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().fetchBatchCount(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .fetchBatchCount(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().fetchBatchCount()).isEqualTo(5);
    }

    @Test
    public void fetchBatchCountUsesRuntimeIfInstallNotPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .fetchBatchCount(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().fetchBatchCount()).isEqualTo(10);
    }

    @Test
    public void sweepReadThreadsUsesInstallIfPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().sweepReadThreads(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .sweepReadThreads(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().sweepReadThreads()).isEqualTo(5);
    }

    @Test
    public void sweepReadThreadsUsesRuntimeIfInstallIsNotPresent() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilderWithDefaultServersAndReplicationFactor()
                        .sweepReadThreads(10)
                        .build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().sweepReadThreads()).isEqualTo(10);
    }

    @Test
    public void mergedConfigPrioritisesInstallForServers() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().servers(SERVERS_1).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = runtimeConfigBuilderWithDefaultReplicationFactor()
                .servers(SERVERS_2)
                .build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().servers()).isEqualTo(SERVERS_1);
    }

    @Test
    public void mergedConfigDelegatesToRuntimeServersIfInstallIsEmpty() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = runtimeConfigBuilderWithDefaultReplicationFactor()
                .servers(SERVERS_2)
                .build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().servers()).isEqualTo(SERVERS_2);
    }

    @Test
    public void mergedConfigServersEmptyFailsInitialisation() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = runtimeConfigBuilderWithDefaultReplicationFactor()
                .servers(ImmutableDefaultConfig.of())
                .build();

        assertThatLoggableExceptionThrownBy(() -> CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(runtimeConfig)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("'servers' must have at least one defined host");
    }

    @Test
    public void mergedConfigPrioritisesInstallForConcurrentGetRangesThreadPoolSize() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentials()
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
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().poolSize(poolSize).build();
        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS));

        assertThat(reloadableConfig.get().concurrentGetRangesThreadPoolSize())
                .isEqualTo(poolSize * SERVERS_1.numberOfThriftHosts());
    }

    @Test
    public void mergedConfigPrioritisesInstallForDefaultGetRangesConcurrency() {
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentials()
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
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().poolSize(10).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS));

        assertThat(reloadableConfig.get().defaultGetRangesConcurrency()).isEqualTo(8);
    }

    @Test
    public void mergedConfigSharedGetRangesPoolSizeLessThanConcurrentGetRangesThreadPoolSizeFailsInitialisation() {
        int sharedGetRangesPoolSize = 123;
        int concurrentGetRangesThreadPoolSize = 321;
        CassandraKeyValueServiceConfig config = configBuilderWithDefaultCredentials()
                .sharedResourcesConfig(ImmutableSharedResourcesConfig.builder()
                        .sharedGetRangesPoolSize(sharedGetRangesPoolSize)
                        .sharedKvsExecutorSize(0)
                        .connectionConfig(mock(LocalConnectionConfig.class))
                        .build())
                .concurrentGetRangesThreadPoolSize(concurrentGetRangesThreadPoolSize)
                .build();

        assertThatLoggableExceptionThrownBy(() -> CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(RUNTIME_CONFIG_WITH_DEFAULT_SERVERS)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("If set, shared get ranges pool size must not be less than individual pool size.")
                .containsArgs(
                        SafeArg.of("shared", sharedGetRangesPoolSize),
                        SafeArg.of("individual", concurrentGetRangesThreadPoolSize));
    }

    @Test
    public void mergedConfigReplicationFactorNegativeFailsInitialisation() {
        CassandraKeyValueServiceConfig config =
                configBuilderWithDefaultCredentials().replicationFactor(-1).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = runtimeConfigBuilderWithDefaultReplicationFactor()
                .servers(ImmutableDefaultConfig.of())
                .replicationFactor(-1)
                .build();

        assertThatLoggableExceptionThrownBy(() -> CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(runtimeConfig)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("'replicationFactor' must be non-negative");
    }

    private static ImmutableCassandraKeyValueServiceConfig.Builder configBuilderWithDefaultCredentials() {
        return ImmutableCassandraKeyValueServiceConfig.builder().credentials(CREDENTIALS);
    }

    private static ImmutableCassandraKeyValueServiceRuntimeConfig.Builder
            runtimeConfigBuilderWithDefaultReplicationFactor() {
        return ImmutableCassandraKeyValueServiceRuntimeConfig.builder().replicationFactor(1);
    }

    private static ImmutableCassandraKeyValueServiceRuntimeConfig.Builder
            runtimeConfigBuilderWithDefaultServersAndReplicationFactor() {
        return runtimeConfigBuilderWithDefaultReplicationFactor().servers(SERVERS_1);
    }
}
