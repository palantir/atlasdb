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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class AtlasDbConfigTest {
    private static final KeyValueServiceConfig KVS_CONFIG_WITHOUT_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final KeyValueServiceConfig KVS_CONFIG_WITH_OTHER_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final KeyValueServiceConfig KVS_CONFIG_WITH_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final KeyValueServiceConfig CASSANDRA_CONFIG_WITHOUT_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final KeyValueServiceConfig CASSANDRA_CONFIG_WITH_NAMESPACE = mock(KeyValueServiceConfig.class);
    private static final LeaderConfig LEADER_CONFIG = ImmutableLeaderConfig.builder()
            .quorumSize(1)
            .localServer("me")
            .addLeaders("me")
            .build();
    private static final ServerListConfig SINGLETON_SERVER_LIST =
            ImmutableServerListConfig.builder().addServers("server").build();
    private static final TimeLockClientConfig TIMELOCK_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client("client")
            .serversList(SINGLETON_SERVER_LIST)
            .build();
    private static final Optional<SslConfiguration> SSL_CONFIG = Optional.of(mock(SslConfiguration.class));
    private static final Optional<SslConfiguration> OTHER_SSL_CONFIG = Optional.of(mock(SslConfiguration.class));
    private static final Optional<SslConfiguration> NO_SSL_CONFIG = Optional.empty();

    private static final String TEST_NAMESPACE = "client";
    private static final String OTHER_CLIENT = "other-client";

    private static final TimeLockClientConfig TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT =
            ImmutableTimeLockClientConfig.builder()
                    .client(Optional.empty())
                    .serversList(SINGLETON_SERVER_LIST)
                    .build();
    private static final TimeLockClientConfig TIMELOCK_CONFIG_WITH_OTHER_CLIENT =
            ImmutableTimeLockClientConfig.builder()
                    .client(OTHER_CLIENT)
                    .serversList(SINGLETON_SERVER_LIST)
                    .build();
    private static final String CLIENT_NAMESPACE = "client";
    private static final String CASSANDRA = "cassandra";

    @BeforeClass
    public static void setUp() {
        when(KVS_CONFIG_WITHOUT_NAMESPACE.namespace()).thenReturn(Optional.empty());
        when(KVS_CONFIG_WITH_OTHER_NAMESPACE.namespace()).thenReturn(Optional.of(OTHER_CLIENT));
        when(KVS_CONFIG_WITH_NAMESPACE.namespace()).thenReturn(Optional.of(TEST_NAMESPACE));
        when(CASSANDRA_CONFIG_WITHOUT_NAMESPACE.namespace()).thenReturn(Optional.empty());
        when(CASSANDRA_CONFIG_WITH_NAMESPACE.namespace()).thenReturn(Optional.of(TEST_NAMESPACE));

        when(KVS_CONFIG_WITHOUT_NAMESPACE.type()).thenReturn("rocksdb");
        when(KVS_CONFIG_WITH_OTHER_NAMESPACE.type()).thenReturn("database");
        when(KVS_CONFIG_WITH_NAMESPACE.type()).thenReturn("sqlite");
        when(CASSANDRA_CONFIG_WITHOUT_NAMESPACE.type()).thenReturn(CASSANDRA);
        when(CASSANDRA_CONFIG_WITH_NAMESPACE.type()).thenReturn(CASSANDRA);
    }

    @Test
    public void configWithNoLeaderOrLockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .build();
        assertThat(config).isNotNull();
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
        assertThat(config).isNotNull();
    }

    @Test
    public void configWithTimelockBlockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG)
                .build();
        assertThat(config).isNotNull();
    }

    @Test
    public void remoteLockAndTimestampConfigIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
        assertThat(config).isNotNull();
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
                        .serversList(SINGLETON_SERVER_LIST)
                        .build())
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
                .satisfies(exception -> assertThat(exception.getMessage()).contains("config needs to be set"));
    }

    @Test
    public void absentNamespaceRequiresTimelockClient() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(exception -> assertThat(exception.getMessage()).contains("config should be present"));
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
    }

    @Test
    public void inMemoryConfigCanHaveEmptyNamespace() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertThat(kvsConfig.namespace())
                .describedAs("This test assumes the InMemoryAtlasDbConfig has no namespace by default")
                .isNotPresent();
        ImmutableAtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace(Optional.empty())
                .keyValueService(kvsConfig)
                .build();
    }

    @Test
    public void inMemoryConfigWorksWithNonTestNamespace() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertThat(kvsConfig.namespace())
                .describedAs("This test assumes the InMemoryAtlasDbConfig has no namespace by default")
                .isNotPresent();
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace("clive")
                .keyValueService(kvsConfig)
                .build();
    }

    @Test
    public void inMemoryConfigCannotHaveEmptyNamespaceWithEmptyTimelockClient() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertThat(kvsConfig.namespace())
                .describedAs("This test assumes the InMemoryAtlasDbConfig has no namespace by default")
                .isNotPresent();
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .namespace(Optional.empty())
                        .keyValueService(kvsConfig)
                        .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(exception ->
                        assertThat(exception.getMessage()).contains("TimeLock client should not be empty"));
    }

    // Documenting that for in-memory, we don't care what the timelock client is - it just has to be non-empty.
    @Test
    public void inMemoryKeyspaceAndTimelockClientCanBeDifferent() {
        InMemoryAtlasDbConfig kvsConfig = new InMemoryAtlasDbConfig();
        assertThat(kvsConfig.namespace())
                .describedAs("This test assumes the InMemoryAtlasDbConfig has no namespace by default")
                .isNotPresent();
        ImmutableAtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(kvsConfig)
                .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                .build();
    }

    @Test
    public void namespaceAndTimelockClientShouldMatch() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .namespace(TEST_NAMESPACE)
                        .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(exception -> assertThat(exception.getMessage()).contains("config should be the same"));
    }

    @Test
    public void namespaceAndKvsNamespaceShouldMatch() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .namespace(TEST_NAMESPACE)
                        .keyValueService(KVS_CONFIG_WITH_OTHER_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG_WITH_OPTIONAL_EMPTY_CLIENT)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(exception -> assertThat(exception.getMessage()).contains("config should be the same"));
    }

    @Test
    public void kvsAndTimelockNamespacesCanDifferIfExpresslyPermittedAndNotUsingCassandra() {
        assertThatCode(() -> ImmutableAtlasDbConfig.builder()
                        .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                        .enableNonstandardAndPossiblyErrorProneTopologyAllowDifferentKvsAndTimelockNamespaces(true)
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void kvsAndTimelockNamespacesCannotDifferIfUsingCassandra() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .keyValueService(CASSANDRA_CONFIG_WITH_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                        .enableNonstandardAndPossiblyErrorProneTopologyAllowDifferentKvsAndTimelockNamespaces(true)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(exception -> assertThat(exception.getMessage()).contains("avoid potential data corruption"));
    }

    @Test
    public void globalNamespaceAlwaysReportsConflictsIfPresentForCoherentKvsAndTimeLockConfig() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .namespace(OTHER_CLIENT)
                        .keyValueService(CASSANDRA_CONFIG_WITH_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG)
                        .enableNonstandardAndPossiblyErrorProneTopologyAllowDifferentKvsAndTimelockNamespaces(true)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(
                        exception -> assertThat(exception.getMessage()).contains("atlas root-level namespace config"));
    }

    @Test
    public void globalNamespaceAlwaysReportsConflictsIfPresentForTimeLock() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .namespace(OTHER_CLIENT)
                        .keyValueService(KVS_CONFIG_WITH_OTHER_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG)
                        .enableNonstandardAndPossiblyErrorProneTopologyAllowDifferentKvsAndTimelockNamespaces(true)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(
                        exception -> assertThat(exception.getMessage()).contains("atlas root-level namespace config"));
    }

    @Test
    public void globalNamespaceAlwaysReportsConflictsIfPresentForKvs() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .namespace(OTHER_CLIENT)
                        .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                        .timelock(TIMELOCK_CONFIG_WITH_OTHER_CLIENT)
                        .enableNonstandardAndPossiblyErrorProneTopologyAllowDifferentKvsAndTimelockNamespaces(true)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .satisfies(
                        exception -> assertThat(exception.getMessage()).contains("atlas root-level namespace config"));
    }

    @Test
    public void addingFallbackSslAddsItToLeaderBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration()).isEqualTo(SSL_CONFIG);
    }

    @Test
    public void addingFallbackSslAddsItToLockBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.lock().get().sslConfiguration()).isEqualTo(SSL_CONFIG);
    }

    @Test
    public void addingFallbackSslAddsItToTimelockServersBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .timelock(TIMELOCK_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.timelock().get().serversList().sslConfiguration()).isEqualTo(SSL_CONFIG);
    }

    @Test
    public void addingFallbackSslAddsItToTimestampBlock() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .lock(SINGLETON_SERVER_LIST)
                .timestamp(SINGLETON_SERVER_LIST)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, SSL_CONFIG);
        assertThat(withSsl.timestamp().get().sslConfiguration()).isEqualTo(SSL_CONFIG);
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
        assertThat(withSsl.leader().get().sslConfiguration()).isEqualTo(SSL_CONFIG);
    }

    @Test
    public void addingAbsentFallbackSslWhenItDoesntExistsLeavesItAsAbsent() {
        AtlasDbConfig withoutSsl = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG_WITH_NAMESPACE)
                .leader(LEADER_CONFIG)
                .build();
        AtlasDbConfig withSsl = AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig(withoutSsl, NO_SSL_CONFIG);
        assertThat(withSsl.leader().get().sslConfiguration()).isEqualTo(NO_SSL_CONFIG);
    }

    @Test
    public void cannotSpecifyZeroServersIfUsingTimestampBlock() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                        .namespace(CLIENT_NAMESPACE)
                        .timestamp(ImmutableServerListConfig.builder().build())
                        .lock(SINGLETON_SERVER_LIST)
                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void cannotSpecifyZeroServersIfUsingLockBlock() {
        assertThatThrownBy(() -> ImmutableAtlasDbConfig.builder()
                        .keyValueService(KVS_CONFIG_WITHOUT_NAMESPACE)
                        .namespace(CLIENT_NAMESPACE)
                        .timestamp(SINGLETON_SERVER_LIST)
                        .lock(ImmutableServerListConfig.builder().build())
                        .build())
                .isInstanceOf(IllegalStateException.class);
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
