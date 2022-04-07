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

    @Test
    public void unresponsiveHostBackoffTimeSecondsAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config =
                configBuilder().unresponsiveHostBackoffTimeSeconds(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig = Refreshable.only(
                runtimeConfigBuilder().unresponsiveHostBackoffTimeSeconds(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().unresponsiveHostBackoffTimeSeconds()).isEqualTo(10);
    }

    @Test
    public void mutationBatchCountAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config =
                configBuilder().mutationBatchCount(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilder().mutationBatchCount(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchCount()).isEqualTo(10);
    }

    @Test
    public void mutationBatchSizeBytesAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config =
                configBuilder().mutationBatchSizeBytes(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig = Refreshable.only(
                runtimeConfigBuilder().mutationBatchSizeBytes(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().mutationBatchSizeBytes()).isEqualTo(10);
    }

    @Test
    public void fetchBatchCountAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config =
                configBuilder().fetchBatchCount(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilder().fetchBatchCount(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().fetchBatchCount()).isEqualTo(10);
    }

    @Test
    public void sweepReadThreadsAlwaysUsesRuntimeConfigValue() {
        CassandraKeyValueServiceConfig config =
                configBuilder().sweepReadThreads(5).build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(runtimeConfigBuilder().sweepReadThreads(10).build());

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, runtimeConfig);
        assertThat(reloadableConfig.get().sweepReadThreads()).isEqualTo(10);
    }

    @Test
    public void mergedConfigPrioritisesInstallForServers() {
        CassandraKeyValueServiceConfig config =
                configBuilder().servers(SERVERS_1).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().servers()).isEqualTo(SERVERS_1);
    }

    @Test
    public void mergedConfigDelegatesToRuntimeServersIfInstallIsEmpty() {
        CassandraKeyValueServiceConfig config = configBuilder().build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().servers()).isEqualTo(SERVERS_2);
    }

    @Test
    public void mergedConfigServersEmptyFailsInitialisation() {
        CassandraKeyValueServiceConfig config = configBuilder().build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(ImmutableDefaultConfig.of()).build();

        assertThatLoggableExceptionThrownBy(() -> CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                        config, Refreshable.only(runtimeConfig)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("'servers' must have at least one defined host");
    }

    @Test
    public void mergedConfigPrioritisesInstallForConcurrentGetRangesThreadPoolSize() {
        CassandraKeyValueServiceConfig config =
                configBuilder().concurrentGetRangesThreadPoolSize(1).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_1).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().concurrentGetRangesThreadPoolSize()).isEqualTo(1);
    }

    @Test
    public void mergedConfigDerivesThreadPoolSizeIfInstallIsEmpty() {
        int poolSize = 10;
        CassandraKeyValueServiceConfig config =
                configBuilder().poolSize(poolSize).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_1).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().concurrentGetRangesThreadPoolSize())
                .isEqualTo(poolSize * SERVERS_1.numberOfThriftHosts());
    }

    @Test
    public void mergedConfigPrioritisesInstallForDefaultGetRangesConcurrency() {
        CassandraKeyValueServiceConfig config =
                configBuilder().concurrentGetRangesThreadPoolSize(10).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_1).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().defaultGetRangesConcurrency())
                .isEqualTo(config.defaultGetRangesConcurrency());
    }

    @Test
    public void mergedConfigDerivesDefaultGetRangesConcurrencyIfInstallIsEmpty() {
        CassandraKeyValueServiceConfig config = configBuilder().poolSize(10).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_1).build();

        Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> reloadableConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(config, Refreshable.only(runtimeConfig));

        assertThat(reloadableConfig.get().defaultGetRangesConcurrency())
                .isEqualTo(Math.min(8, reloadableConfig.get().concurrentGetRangesThreadPoolSize() / 2));
    }

    private ImmutableCassandraKeyValueServiceConfig.Builder configBuilder() {
        return ImmutableCassandraKeyValueServiceConfig.builder();
    }

    private ImmutableCassandraKeyValueServiceRuntimeConfig.Builder runtimeConfigBuilder() {
        return ImmutableCassandraKeyValueServiceRuntimeConfig.builder();
    }
}
