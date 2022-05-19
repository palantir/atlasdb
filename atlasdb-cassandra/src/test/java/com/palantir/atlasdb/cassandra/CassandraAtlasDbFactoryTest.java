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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class CassandraAtlasDbFactoryTest {
    private static final String KEYSPACE = "ks";
    private static final String KEYSPACE_2 = "ks2";
    private static final ImmutableSet<InetSocketAddress> SERVERS =
            ImmutableSet.of(InetSocketAddress.createUnresolved("foo", 42));
    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();

    private static final CassandraKeyValueServiceConfig CONFIG_WITHOUT_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .replicationFactor(1)
                    .credentials(CREDENTIALS)
                    .build();
    private static final CassandraKeyValueServiceConfig CONFIG_WITH_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .keyspace(KEYSPACE)
                    .replicationFactor(1)
                    .credentials(CREDENTIALS)
                    .build();

    private static final CassandraKeyValueServiceRuntimeConfig DEFAULT_CKVS_RUNTIME_CONFIG =
            ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                    .servers(ImmutableDefaultConfig.builder()
                            .addAllThriftHosts(SERVERS)
                            .build())
                    .build();

    private static final KeyValueServiceRuntimeConfig INVALID_CKVS_RUNTIME_CONFIG = () -> "test";
    private CassandraAtlasDbFactory factory;

    @Before
    public void setUp() {
        factory = new CassandraAtlasDbFactory();
    }

    @Test
    public void throwsWhenPreprocessingNonCassandraKvsConfig() {
        assertThatThrownBy(() -> {
                    KeyValueServiceConfigHelper keyValueServiceConfig = () -> "Fake KVS";
                    factory.getConfigWithNamespace(keyValueServiceConfig, Optional.empty());
                })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithNoKeyspaceAndNoNamespace() {
        assertThatThrownBy(() -> factory.getConfigWithNamespace(CONFIG_WITHOUT_KEYSPACE, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithKeyspaceAndDifferentNamespace() {
        assertThatThrownBy(() -> factory.getConfigWithNamespace(CONFIG_WITH_KEYSPACE, Optional.of(KEYSPACE_2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void resolvesConfigWithOriginalKeyspaceIfNoNamespaceProvided() {
        CassandraKeyValueServiceConfig newConfig =
                factory.getConfigWithNamespace(CONFIG_WITH_KEYSPACE, Optional.empty());
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(CONFIG_WITH_KEYSPACE.getKeyspaceOrThrow());
    }

    @Test
    public void resolvesConfigWithNamespaceIfNoKeyspaceProvided() {
        CassandraKeyValueServiceConfig newConfig =
                factory.getConfigWithNamespace(CONFIG_WITHOUT_KEYSPACE, Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void preservesOtherPropertiesOnResolvedConfigWithNamespace() {
        CassandraKeyValueServiceConfig newConfig =
                factory.getConfigWithNamespace(CONFIG_WITHOUT_KEYSPACE, Optional.of(KEYSPACE));
        assertThat(newConfig.credentials()).isEqualTo(CREDENTIALS);
        assertThat(newConfig.replicationFactor()).isEqualTo(1);
    }

    @Test
    public void resolvesConfigIfKeyspaceAndNamespaceProvidedAndMatch() {
        CassandraKeyValueServiceConfig newConfig =
                factory.getConfigWithNamespace(CONFIG_WITH_KEYSPACE, Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void emptyRuntimeConfigShouldResolveToDefaultConfig() {
        CassandraKeyValueServiceRuntimeConfig returnedConfig = factory.preprocessKvsRuntimeConfig(
                        Refreshable.only(Optional.empty()))
                .get();

        assertThat(returnedConfig)
                .describedAs("Empty config should resolve to default")
                .isEqualTo(DEFAULT_CKVS_RUNTIME_CONFIG);
    }

    @Test
    public void preservesValidRuntimeConfigIfFollowingLaterConfigIsNotValid() {
        CassandraKeyValueServiceRuntimeConfig baseRuntimeConfig =
                ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                        .sweepReadThreads(12341)
                        .build();
        SettableRefreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig =
                Refreshable.create(Optional.of(baseRuntimeConfig));

        Refreshable<CassandraKeyValueServiceRuntimeConfig> processedRuntimeConfig =
                factory.preprocessKvsRuntimeConfig(runtimeConfig);
        CassandraKeyValueServiceRuntimeConfig firstReturnedConfig = processedRuntimeConfig.get();

        runtimeConfig.update(Optional.of(INVALID_CKVS_RUNTIME_CONFIG));
        CassandraKeyValueServiceRuntimeConfig secondReturnedConfig = processedRuntimeConfig.get();

        assertThat(firstReturnedConfig)
                .describedAs("First returned config should be valid")
                .isEqualTo(baseRuntimeConfig);
        assertThat(secondReturnedConfig)
                .describedAs("Second invalid config should be ignored")
                .isEqualTo(baseRuntimeConfig);
    }

    @Test
    public void firstConfigInvalidShouldResolveToDefault() {
        CassandraKeyValueServiceRuntimeConfig returnedConfig = factory.preprocessKvsRuntimeConfig(
                        Refreshable.only(Optional.of(INVALID_CKVS_RUNTIME_CONFIG)))
                .get();

        assertThat(returnedConfig)
                .describedAs("First invalid config should resolve to default")
                .isEqualTo(DEFAULT_CKVS_RUNTIME_CONFIG);
    }

    @Test
    public void derivedSnapshotConfigDefaultGetRangesConcurrencyOverriddenWhenInstallOverrideIsPresent() {
        int defaultGetRangesConcurrencyOverride = 200;
        CassandraKeyValueServiceConfig installConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CONFIG_WITH_KEYSPACE)
                .defaultGetRangesConcurrency(defaultGetRangesConcurrencyOverride)
                .build();
        DerivedSnapshotConfig derivedSnapshotConfig =
                factory.createDerivedSnapshotConfig(installConfig, Optional.of(DEFAULT_CKVS_RUNTIME_CONFIG));
        assertThat(derivedSnapshotConfig.defaultGetRangesConcurrency()).isEqualTo(defaultGetRangesConcurrencyOverride);
    }
}
