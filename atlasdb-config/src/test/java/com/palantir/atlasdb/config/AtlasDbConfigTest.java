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
package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtlasDbConfigTest {
    private static final KeyValueServiceConfig KVS_CONFIG_WITHOUT_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final KeyValueServiceConfig KVS_CONFIG_WITH_OTHER_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final KeyValueServiceConfig KVS_CONFIG_WITH_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final LeaderConfig LEADER_CONFIG = ImmutableLeaderConfig.builder()
            .quorumSize(1)
            .localServer("me")
            .addLeaders("me")
            .build();
    private static final ServerListConfig SINGLETON_SERVER_LIST = ImmutableServerListConfig.builder()
            .addServers("server")
            .build();
    private static final TimeLockClientConfig TIMELOCK_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client("client")
            .serversList(SINGLETON_SERVER_LIST)
            .build();
    private static final Optional<SslConfiguration> SSL_CONFIG = Optional.of(mock(SslConfiguration.class));
    private static final Optional<SslConfiguration> OTHER_SSL_CONFIG = Optional.of(mock(SslConfiguration.class));
    private static final Optional<SslConfiguration> NO_SSL_CONFIG = Optional.empty();

    private static final String TEST_NAMESPACE = "client";
    private static final String OTHER_CLIENT = "other-client";

    private static final TimeLockClientConfig TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT = ImmutableTimeLockClientConfig
            .builder()
            .client(Optional.empty())
            .serversList(SINGLETON_SERVER_LIST)
            .build();
    private static final TimeLockClientConfig TIMELOCK_CONFIG_WITH_OTHER_CLIENT = ImmutableTimeLockClientConfig
            .builder()
            .client(OTHER_CLIENT)
            .serversList(SINGLETON_SERVER_LIST)
            .build();
    private static final String CLIENT_NAMESPACE = "client";

    @BeforeClass
    public static void setUp() {
        when(KVS_CONFIG_WITHOUT_NAMESPACE.namespace()).thenReturn(Optional.empty());
        when(KVS_CONFIG_WITH_OTHER_NAMESPACE.namespace()).thenReturn(Optional.of(OTHER_CLIENT));
        when(KVS_CONFIG_WITH_NAMESPACE.namespace()).thenReturn(Optional.of(TEST_NAMESPACE));
    }

    @Test
    public void configWithNoLeaderOrLockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
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
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .build();
        assertThat(config.getNamespaceString(), equalTo(TEST_NAMESPACE));
        assertThat(config, not(nullValue()));
    }

    @Test
    public void configWithTimelockBlockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG)
                .build();
        assertThat(config.getNamespaceString(), equalTo(TEST_NAMESPACE));
        assertThat(config, not(nullValue()));
    }

    @Test
    public void remoteLockAndTimestampConfigIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
        assertThat(config.getNamespaceString(), equalTo(TEST_NAMESPACE));
        assertThat(config, not(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithLockAndTimestampBlocks() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timelockBlockNotPermittedWithLockAndTimestampBlocks() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(ImmutableTimeLockClientConfig.builder()
                        .client("testClient")
                        .serversList(SINGLETON_SERVER_LIST).build())
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timelockBlockNotPermittedWithLeaderBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG)
                .leader(LEADER_CONFIG)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithLockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .lock(SINGLETON_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithTimestampBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void lockBlockRequiresTimestampBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timestampBlockRequiresLockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
    }

    @Test
    public void absentNamespaceRequiresKvsNamespace() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                .leader(LEADER_CONFIG)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies((exception) ->
                assertThat(exception.getMessage(), containsString("config needs to be set")));
    }

    @Test
    public void absentNamespaceRequiresTimelockClient() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies((exception) ->
                        assertThat(exception.getMessage(), containsString("config should be present")));
    }

    @Test(expected = IllegalStateException.class)
    public void absentNamespaceRequiresMatchingKvsNamespaceAndTimelockClient() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                .build();
    }

    @Test
    public void namespaceAcceptsEmptyKvsNamespaceAndTimelockClient() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace("a client")
                .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                .build();
        assertThat(config.getNamespaceString(), equalTo("a client"));
    }

    @Test
    public void inMemoryConfigCanHaveEmptyNamespace() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertFalse("This test assumes the InMemoryAtlasDbConfig has no namespace by default",
                kvsConfig.namespace().isPresent());
        ImmutableAtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace(Optional.empty())
                .keyValueService(kvsConfig)
                .build();
        assertThat(config.getNamespaceString(), equalTo(AtlasDbConfig.UNSPECIFIED_NAMESPACE));
    }

    @Test
    public void inMemoryConfigWorksWithNonTestNamespace() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertFalse("This test assumes the InMemoryAtlasDbConfig has no namespace by default",
                kvsConfig.namespace().isPresent());
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace("clive")
                .keyValueService(kvsConfig)
                .build();
        assertThat(config.getNamespaceString(), equalTo("clive"));
    }

    @Test
    public void inMemoryConfigCannotHaveEmptyNamespaceWithEmptyTimelockClient() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertFalse("This test assumes the InMemoryAtlasDbConfig has no namespace by default",
                kvsConfig.namespace().isPresent());
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .namespace(Optional.empty())
                .keyValueService(kvsConfig)
                .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies((exception) ->
                        assertThat(exception.getMessage(), containsString("TimeLock client should not be empty")));
    }

    // Documenting that for in-memory, we don't care what the timelock client is - it just has to be non-empty.
    @Test
    public void inMemoryKeyspaceAndTimelockClientCanBeDifferent() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertFalse("This test assumes the InMemoryAtlasDbConfig has no namespace by default",
                kvsConfig.namespace().isPresent());
        ImmutableAtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(kvsConfig)
                .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                .build();
        assertThat(config.getNamespaceString(), equalTo(OTHER_CLIENT));
    }

    @Test
    public void namespaceAndTimelockClientShouldMatch() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .namespace(TEST_NAMESPACE)
                .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies((exception) ->
                        assertThat(exception.getMessage(), containsString("config should be the same")));
    }

    @Test
    public void namespaceAndKvsNamespaceShouldMatch() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .namespace(TEST_NAMESPACE)
                .keyValueService(KVS_CONFIG_WITH_OTHER_NAMESPACE)
                .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies((exception) ->
                        assertThat(exception.getMessage(), containsString("config should be the same")));
    }

    @Test
    public void addingFallbackSslAddsItToLeaderBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration(), is(SSL_CONFIG));
        assertThat(withoutSsl.getNamespaceString(), equalTo(TEST_NAMESPACE));
    }

    @Test
    public void addingFallbackSslAddsItToLockBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.lock().get().sslConfiguration(), is(SSL_CONFIG));
        assertThat(withoutSsl.getNamespaceString(), equalTo(TEST_NAMESPACE));
    }

    @Test
    public void addingFallbackSslAddsItToTimelockServersBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.timelock().get().serversList().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingFallbackSslAddsItToTimestampBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.timestamp().get().sslConfiguration(), is(SSL_CONFIG));
    }

    @Test
    public void addingFallbackSslWhenItExistsDoesntOverride() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
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
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, NO_SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration(), is(NO_SSL_CONFIG));
    }

    @Test
    public void cannotSpecifyZeroServersIfUsingTimestampBlock() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                .namespace(CLIENT_NAMESPACE)
                .timestamp(ImmutableServerListConfig.builder().build())
                .lock(SINGLETON_SERVER_LIST)
                .build()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void cannotSpecifyZeroServersIfUsingLockBlock() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                .namespace(CLIENT_NAMESPACE)
                .timestamp(SINGLETON_SERVER_LIST)
                .lock(ImmutableServerListConfig.builder().build())
                .build()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canSpecifyZeroServersIfUsingTimelockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                .namespace(CLIENT_NAMESPACE)
                .timelock(ImmutableTimeLockClientConfig.builder()
                        .serversList(ImmutableServerListConfig.builder().build())
                        .build())
                .build();
    }
}
