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
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.Test;

public class CassandraReloadableKvsConfigTest {

    private static final CassandraServersConfig SERVERS_1 = ImmutableDefaultConfig.builder()
            .addThriftHosts(new InetSocketAddress("host1", 5000))
            .addThriftHosts(new InetSocketAddress("host2", 5000))
            .build();
    private static final CassandraServersConfig SERVERS_2 = ImmutableDefaultConfig.builder()
            .addThriftHosts(new InetSocketAddress("host3", 5000))
            .addThriftHosts(new InetSocketAddress("host4", 5000))
            .build();

    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();

    @Test
    public void ifNoRuntimeConfig_resolvesToInstallConfig() {
        CassandraKeyValueServiceConfig config =
                configBuilder().sweepReadThreads(1).build();

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, Refreshable.only(Optional.empty()));

        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(1);
    }

    @Test
    public void ifInstallAndRuntimeConfig_resolvesToRuntimeConfig() {
        CassandraKeyValueServiceConfig config =
                configBuilder().sweepReadThreads(1).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().sweepReadThreads(2).build();

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, Refreshable.only(Optional.of(runtimeConfig)));

        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(2);
    }

    @Test
    public void ifRuntimeConfigIsModified_reloadableConfigIsAlsoModified() {
        CassandraKeyValueServiceConfig config =
                configBuilder().sweepReadThreads(1).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig1 =
                runtimeConfigBuilder().sweepReadThreads(2).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig2 =
                runtimeConfigBuilder().sweepReadThreads(3).build();

        SettableRefreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigRefreshable =
                Refreshable.create(Optional.of(runtimeConfig1));

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, runtimeConfigRefreshable);

        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(2);

        runtimeConfigRefreshable.update(Optional.of(runtimeConfig2));

        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(3);
    }

    @Test
    public void ifInstallServersNonEmpty_resolvesToInstallConfig() {
        CassandraKeyValueServiceConfig config = configBuilder()
                .servers(SERVERS_1)
                .concurrentGetRangesThreadPoolSize(42)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, Refreshable.only(Optional.of(runtimeConfig)));

        assertThat(reloadableConfig.servers()).isEqualTo(SERVERS_1);
        assertThat(reloadableConfig.concurrentGetRangesThreadPoolSize()).isEqualTo(42);
    }

    @Test
    public void ifInstallServersEmpty_resolvesToRuntimeConfig() {
        CassandraKeyValueServiceConfig config = configBuilder()
                .servers(ImmutableDefaultConfig.of())
                .poolSize(42)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, Refreshable.only(Optional.of(runtimeConfig)));

        assertThat(reloadableConfig.servers()).isEqualTo(SERVERS_2);
        assertThat(reloadableConfig.concurrentGetRangesThreadPoolSize()).isEqualTo(84);
    }

    @Test
    public void ifInstallServersEmpty_resolvesToRuntimeConfigForRangesConcurrency() {
        CassandraKeyValueServiceConfig config = configBuilder()
                .servers(ImmutableDefaultConfig.of())
                .rangesConcurrency(42)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(SERVERS_2).build();

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, Refreshable.only(Optional.of(runtimeConfig)));

        assertThat(reloadableConfig.servers()).isEqualTo(SERVERS_2);
        assertThat(reloadableConfig.defaultGetRangesConcurrency()).isEqualTo(60);
    }

    @Test
    public void ifInstallAndRuntimeServersEmpty_failsInitialization() {
        CassandraKeyValueServiceConfig config =
                configBuilder().servers(ImmutableDefaultConfig.of()).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                runtimeConfigBuilder().servers(ImmutableDefaultConfig.of()).build();

        assertThatLoggableExceptionThrownBy(
                        () -> new CassandraReloadableKvsConfig(config, Refreshable.only(Optional.of(runtimeConfig))))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("'servers' must have at least one defined host")
                .hasExactlyArgs();
    }

    @Test
    public void ifRuntimeServersIsModifiedEmpty_failsReload() {
        CassandraKeyValueServiceConfig config = configBuilder()
                .servers(ImmutableDefaultConfig.of())
                .poolSize(42)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig1 =
                runtimeConfigBuilder().servers(SERVERS_2).build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig2 =
                runtimeConfigBuilder().servers(ImmutableDefaultConfig.of()).build();

        SettableRefreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigRefreshable =
                Refreshable.create(Optional.of(runtimeConfig1));

        CassandraReloadableKvsConfig reloadableConfig =
                new CassandraReloadableKvsConfig(config, runtimeConfigRefreshable);

        runtimeConfigRefreshable.update(Optional.of(runtimeConfig2));

        assertThat(reloadableConfig.servers()).isEqualTo(SERVERS_2);
        assertThat(reloadableConfig.concurrentGetRangesThreadPoolSize()).isEqualTo(84);
    }

    private ImmutableCassandraKeyValueServiceConfig.Builder configBuilder() {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .servers(SERVERS_1)
                .credentials(CREDENTIALS)
                .replicationFactor(1);
    }

    private ImmutableCassandraKeyValueServiceRuntimeConfig.Builder runtimeConfigBuilder() {
        return ImmutableCassandraKeyValueServiceRuntimeConfig.builder();
    }
}
