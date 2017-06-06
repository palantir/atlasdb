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
package com.palantir.atlasdb.config;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.google.common.base.Optional;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.remoting2.config.ssl.SslConfiguration;

public class AtlasDbConfigTest {
    private static final KeyValueServiceConfig KVS_CONFIG = mock(KeyValueServiceConfig.class);
    private static final LeaderConfig LEADER_CONFIG = ImmutableLeaderConfig.builder()
            .quorumSize(1)
            .localServer("me")
            .addLeaders("me")
            .build();
    private static final ServerListConfig DEFAULT_SERVER_LIST = ImmutableServerListConfig.builder()
            .addServers("server")
            .build();
    private static final TimeLockClientConfig TIMELOCK_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client("testClient")
            .serversList(DEFAULT_SERVER_LIST)
            .build();
    private static final Optional<SslConfiguration> SSL_CONFIG = Optional.of(mock(SslConfiguration.class));
    private static final Optional<SslConfiguration> OTHER_SSL_CONFIG = Optional.of(mock(SslConfiguration.class));
    private static final Optional<SslConfiguration> NO_SSL_CONFIG = Optional.absent();

    @Test
    public void configWithNoLeaderOrLockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void kvsConfigIsRequired() {
        ImmutableAtlasDbConfig.builder().build();
    }

    @Test
    public void configWithLeaderBlockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test
    public void configWithTimelockBlockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .timelock(TIMELOCK_CONFIG)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test
    public void remoteLockAndTimestampConfigIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithLockAndTimestampBlocks() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timelockBlockNotPermittedWithLockAndTimestampBlocks() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .timelock(ImmutableTimeLockClientConfig.builder()
                        .client("testClient")
                        .serversList(DEFAULT_SERVER_LIST).build())
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timelockBlockNotPermittedWithLeaderBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .timelock(TIMELOCK_CONFIG)
                .leader(LEADER_CONFIG)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithLockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithTimestampBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void lockBlockRequiresTimestampBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timestampBlockRequiresLockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test
    public void addingFallbackSslAddsItToLeaderBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingFallbackSslAddsItToLockBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.lock().get().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingFallbackSslAddsItToTimelockServersBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .timelock(TIMELOCK_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.timelock().get().serversList().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingFallbackSslAddsItToTimestampBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.timestamp().get().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingFallbackSslWhenItExistsDoesntOverride() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(ImmutableLeaderConfig.builder()
                        .from(LEADER_CONFIG)
                        .sslConfiguration(SSL_CONFIG)
                        .build())
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, OTHER_SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingAbsentFallbackSslWhenItDoesntExistsLeavesItAsAbsent() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, NO_SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration(), is(NO_SSL_CONFIG));
    }
}
