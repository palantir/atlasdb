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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.refreshable.Refreshable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Optional;
import org.junit.Test;

public class CassandraKeyValueServiceConfigsTest {
    private static final String KEYSPACE = "ks";
    private static final String KEYSPACE_2 = "ks2";
    private static final ImmutableSet<InetSocketAddress> SERVER_ADDRESSES =
            ImmutableSet.of(InetSocketAddress.createUnresolved("foo", 42));
    public static final ImmutableDefaultConfig SERVERS =
            ImmutableDefaultConfig.builder().addAllThriftHosts(SERVER_ADDRESSES).build();
    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();
    private static final CassandraKeyValueServiceConfig CONFIG_WITHOUT_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .replicationFactor(10)
                    .servers(SERVERS)
                    .credentials(CREDENTIALS)
                    .build();
    private static final CassandraKeyValueServiceConfig CONFIG_WITH_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .replicationFactor(10)
                    .servers(SERVERS)
                    .keyspace(KEYSPACE)
                    .credentials(CREDENTIALS)
                    .build();

    private static final CassandraKeyValueServiceConfigs CONFIGS_WITHOUT_KEYSPACE =
            CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                    CONFIG_WITHOUT_KEYSPACE, Refreshable.only(Optional.empty()));

    private static final CassandraKeyValueServiceConfigs CONFIGS_WITH_KEYSPACE =
            CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                    CONFIG_WITH_KEYSPACE, Refreshable.only(Optional.empty()));

    @Test
    public void canDeserialize() throws IOException {
        CassandraKeyValueServiceConfig testConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .servers(SERVERS)
                .addressTranslation(ImmutableMap.of("test", Iterables.getOnlyElement(SERVER_ADDRESSES)))
                .replicationFactor(1)
                .credentials(CREDENTIALS)
                .build();

        URL configUrl =
                CassandraKeyValueServiceConfigsTest.class.getClassLoader().getResource("testConfig.yml");
        CassandraKeyValueServiceConfig deserializedTestConfig = AtlasDbConfigs.OBJECT_MAPPER.readValue(
                new File(configUrl.getPath()), CassandraKeyValueServiceConfig.class);

        assertThat(deserializedTestConfig).isEqualTo(testConfig);
    }

    @Test
    public void copyWithKeyspaceCanAddKeyspace() {
        CassandraKeyValueServiceConfigs newConfigs = CONFIGS_WITHOUT_KEYSPACE.copyWithKeyspace(KEYSPACE);
        assertThat(newConfigs.installConfig().getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void copyWithKeyspacePreserversOtherProperties() {
        CassandraKeyValueServiceConfigs newConfigs = CONFIGS_WITHOUT_KEYSPACE.copyWithKeyspace(KEYSPACE);
        assertThat(newConfigs.installConfig().credentials()).isEqualTo(CREDENTIALS);
    }

    @Test
    public void copyWithKeyspaceCanReplaceKeyspace() {
        CassandraKeyValueServiceConfigs newConfigs = CONFIGS_WITHOUT_KEYSPACE.copyWithKeyspace(KEYSPACE_2);
        assertThat(newConfigs.installConfig().getKeyspaceOrThrow()).isEqualTo(KEYSPACE_2);
    }

    @Test
    public void fromKeyValueServiceConfigsOrThrowThrowsWhenPreprocessingNonCassandraKvsConfig() {
        assertThatThrownBy(() -> {
                    KeyValueServiceConfigHelper keyValueServiceConfig = () -> "Fake KVS";
                    CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                            keyValueServiceConfig, Refreshable.only(Optional.empty()));
                })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void fromKeyValueServiceConfigsReturnsEmptyWhenPreprocessingNonCassandraKvsConfig() {
        KeyValueServiceConfigHelper keyValueServiceConfig = () -> "Fake KVS";
        assertThat(CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigs(
                        keyValueServiceConfig, Refreshable.only(Optional.empty())))
                .isEmpty();
    }

    @Test
    public void copyWithResolvedKeyspaceThrowsWhenPreprocessingConfigWithNoKeyspaceAndNoNamespace() {
        assertThatThrownBy(() -> CONFIGS_WITHOUT_KEYSPACE.copyWithResolvedKeyspace(Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void copyWithResolvedKeyspaceThrowsWhenPreprocessingConfigWithKeyspaceAndDifferentNamespace() {
        assertThatThrownBy(() -> CONFIGS_WITH_KEYSPACE.copyWithResolvedKeyspace(Optional.of(KEYSPACE_2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void copyWithResolvedKeyspaceResolvesConfigWithOriginalKeyspaceIfNoNamespaceProvided() {
        CassandraKeyValueServiceConfigs newConfigs = CONFIGS_WITH_KEYSPACE.copyWithResolvedKeyspace(Optional.empty());
        assertThat(newConfigs.installConfig().getKeyspaceOrThrow())
                .isEqualTo(CONFIG_WITH_KEYSPACE.getKeyspaceOrThrow());
    }

    @Test
    public void copyWithResolvedKeyspaceResolvesConfigWithNamespaceIfNoKeyspaceProvided() {
        CassandraKeyValueServiceConfigs newConfigs =
                CONFIGS_WITHOUT_KEYSPACE.copyWithResolvedKeyspace(Optional.of(KEYSPACE));
        assertThat(newConfigs.installConfig().getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void copyWithResolvedKeyspaceResolvesConfigIfKeyspaceAndNamespaceProvidedAndMatch() {
        CassandraKeyValueServiceConfigs newConfigs =
                CONFIGS_WITH_KEYSPACE.copyWithResolvedKeyspace(Optional.of(KEYSPACE));
        assertThat(newConfigs.installConfig().getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void fromKeyValueServiceConfigMergesInstallAndRuntime() {
        CassandraKeyValueServiceConfigs configs = CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                CONFIG_WITHOUT_KEYSPACE,
                Refreshable.only(Optional.of(ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                        .replicationFactor(1010101)
                        .build())));
        assertThat(configs.runtimeConfig().get().replicationFactor())
                .isEqualTo(configs.installConfig().replicationFactor().orElseThrow());
    }

    @Test
    public void emptyRuntimeConfigShouldResolveToDefaultRuntimeConfig() {
        CassandraKeyValueServiceConfigs returnedConfigs =
                CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                        CONFIG_WITHOUT_KEYSPACE, Refreshable.only(Optional.empty()));

        assertThat(returnedConfigs.runtimeConfig().get().mutationBatchCount())
                .isEqualTo(CassandraKeyValueServiceRuntimeConfig.getDefault().mutationBatchCount());
    }

    @Test
    public void derivedSnapshotConfigDefaultGetRangesConcurrencyOverriddenWhenInstallOverrideIsPresent() {
        int defaultGetRangesConcurrencyOverride = 200;
        CassandraKeyValueServiceConfig installConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CONFIG_WITH_KEYSPACE)
                .defaultGetRangesConcurrency(defaultGetRangesConcurrencyOverride)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                .from(CassandraKeyValueServiceRuntimeConfig.getDefault())
                .servers(SERVERS)
                .replicationFactor(1)
                .build();

        CassandraKeyValueServiceConfigs returnedConfigs =
                CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                        installConfig, Refreshable.only(Optional.of(runtimeConfig)));
        assertThat(returnedConfigs.derivedSnapshotConfig().defaultGetRangesConcurrency())
                .isEqualTo(defaultGetRangesConcurrencyOverride);
    }

    @Test
    public void canParseRuntimeDeprecatedConfigType() {
        assertThatNoException()
                .isThrownBy(() -> AtlasDbConfigs.OBJECT_MAPPER.readValue(
                        "type: CassandraKeyValueServiceRuntimeConfig", KeyValueServiceRuntimeConfig.class));
    }

    @Test
    public void canDeserializeRuntimeConfig() throws IOException {
        CassandraKeyValueServiceRuntimeConfig expectedConfig = ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                .numberOfRetriesOnSameHost(4)
                .numberOfRetriesOnAllHosts(8)
                .cellLoadingConfig(ImmutableCassandraCellLoadingConfig.builder()
                        .crossColumnLoadBatchLimit(42)
                        .singleQueryLoadBatchLimit(424242)
                        .build())
                .conservativeRequestExceptionHandler(true)
                .servers(SERVERS)
                .replicationFactor(1)
                .build();

        URL configUrl =
                CassandraKeyValueServiceConfigsTest.class.getClassLoader().getResource("testRuntimeConfig.yml");

        CassandraKeyValueServiceRuntimeConfig deserializedConfig = AtlasDbConfigs.OBJECT_MAPPER.readValue(
                new File(configUrl.getPath()), CassandraKeyValueServiceRuntimeConfig.class);

        assertThat(deserializedConfig).isEqualTo(expectedConfig);
    }
}
