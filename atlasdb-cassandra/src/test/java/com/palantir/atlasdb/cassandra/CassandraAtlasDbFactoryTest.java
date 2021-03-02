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
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.Test;

public class CassandraAtlasDbFactoryTest {
    private static final String KEYSPACE = "ks";
    private static final String KEYSPACE_2 = "ks2";
    private static final ImmutableSet<InetSocketAddress> SERVERS = ImmutableSet.of(new InetSocketAddress("foo", 42));
    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();

    private static final CassandraKeyValueServiceConfig CONFIG_WITHOUT_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(ImmutableDefaultConfig.builder()
                            .addAllThriftHosts(SERVERS)
                            .build())
                    .replicationFactor(1)
                    .credentials(CREDENTIALS)
                    .build();
    private static final CassandraKeyValueServiceConfig CONFIG_WITH_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(ImmutableDefaultConfig.builder()
                            .addAllThriftHosts(SERVERS)
                            .build())
                    .keyspace(KEYSPACE)
                    .replicationFactor(1)
                    .credentials(CREDENTIALS)
                    .build();

    private static final KeyValueServiceRuntimeConfig INVALID_CKVS_RUNTIME_CONFIG = () -> "test";
    private static final KeyValueServiceRuntimeConfig DEFAULT_CKVS_RUNTIME_CONFIG =
            CassandraKeyValueServiceRuntimeConfig.getDefault();
    private static final CassandraAtlasDbFactory FACTORY = new CassandraAtlasDbFactory();

    @Test
    public void throwsWhenPreprocessingNonCassandraKvsConfig() {
        assertThatThrownBy(() -> {
                    KeyValueServiceConfigHelper keyValueServiceConfig = () -> "Fake KVS";
                    FACTORY.createMergedKeyValueServiceConfig(
                            keyValueServiceConfig, Refreshable.only(Optional.empty()), Optional.empty());
                })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithNoKeyspaceAndNoNamespace() {
        assertThatThrownBy(() -> FACTORY.createMergedKeyValueServiceConfig(
                        CONFIG_WITHOUT_KEYSPACE, Refreshable.only(Optional.empty()), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithKeyspaceAndDifferentNamespace() {
        assertThatThrownBy(() -> FACTORY.createMergedKeyValueServiceConfig(
                        CONFIG_WITH_KEYSPACE, Refreshable.only(Optional.empty()), Optional.of(KEYSPACE_2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void resolvesConfigWithOriginalKeyspaceIfNoNamespaceProvided() {
        CassandraKeyValueServiceConfig newConfig = FACTORY.createMergedKeyValueServiceConfig(
                CONFIG_WITH_KEYSPACE, Refreshable.only(Optional.empty()), Optional.empty());
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(CONFIG_WITH_KEYSPACE.getKeyspaceOrThrow());
    }

    @Test
    public void resolvesConfigWithNamespaceIfNoKeyspaceProvided() {
        CassandraKeyValueServiceConfig newConfig = FACTORY.createMergedKeyValueServiceConfig(
                CONFIG_WITHOUT_KEYSPACE, Refreshable.only(Optional.empty()), Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void preservesOtherPropertiesOnResolvedConfigWithNamespace() {
        CassandraKeyValueServiceConfig newConfig = FACTORY.createMergedKeyValueServiceConfig(
                CONFIG_WITHOUT_KEYSPACE, Refreshable.only(Optional.empty()), Optional.of(KEYSPACE));
        assertThat(newConfig.servers())
                .isEqualTo(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(SERVERS)
                        .build());
        assertThat(newConfig.replicationFactor()).isEqualTo(1);
    }

    @Test
    public void resolvesConfigIfKeyspaceAndNamespaceProvidedAndMatch() {
        CassandraKeyValueServiceConfig newConfig = FACTORY.createMergedKeyValueServiceConfig(
                CONFIG_WITH_KEYSPACE, Refreshable.only(Optional.empty()), Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void emptyRuntimeConfigShouldResolveToDefaultConfig() {
        CassandraKeyValueServiceRuntimeConfig returnedConfig =
                FACTORY.preprocessKvsRuntimeConfig(Optional::empty).get();

        assertThat(returnedConfig)
                .describedAs("Empty config should resolve to default")
                .isEqualTo(DEFAULT_CKVS_RUNTIME_CONFIG);
    }

    @Test
    public void preservesValidRuntimeConfigIfFollowingLaterConfigIsNotValid() {
        AtomicBoolean first = new AtomicBoolean(true);
        Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigSupplier = () -> {
            if (first.compareAndSet(true, false)) {
                return Optional.of(DEFAULT_CKVS_RUNTIME_CONFIG);
            }
            return Optional.of(INVALID_CKVS_RUNTIME_CONFIG);
        };

        Supplier<CassandraKeyValueServiceRuntimeConfig> processedRuntimeConfig =
                FACTORY.preprocessKvsRuntimeConfig(runtimeConfigSupplier);
        CassandraKeyValueServiceRuntimeConfig firstReturnedConfig = processedRuntimeConfig.get();
        CassandraKeyValueServiceRuntimeConfig secondReturnedConfig = processedRuntimeConfig.get();

        assertThat(firstReturnedConfig)
                .describedAs("First returned config should be valid")
                .isEqualTo(DEFAULT_CKVS_RUNTIME_CONFIG);
        assertThat(secondReturnedConfig)
                .describedAs("Second invalid config should be ignored")
                .isEqualTo(DEFAULT_CKVS_RUNTIME_CONFIG);
    }

    @Test
    public void firstConfigInvalidShouldResolveToDefault() {
        CassandraKeyValueServiceRuntimeConfig returnedConfig = FACTORY.preprocessKvsRuntimeConfig(
                        () -> Optional.of(INVALID_CKVS_RUNTIME_CONFIG))
                .get();

        assertThat(returnedConfig)
                .describedAs("First invalid config should resolve to default")
                .isEqualTo(DEFAULT_CKVS_RUNTIME_CONFIG);
    }
}
