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
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.Test;

public class CassandraAtlasDbFactoryTest {
    private static final String KEYSPACE = "ks";
    private static final String KEYSPACE_2 = "ks2";
    private static final ImmutableSet<InetSocketAddress> SERVERS = ImmutableSet.of(new InetSocketAddress("foo", 42));
    private static final CassandraCredentialsConfig CREDENTIALS =
            ImmutableCassandraCredentialsConfig.builder()
                    .username("username")
                    .password("password")
                    .build();

    private static final CassandraKeyValueServiceConfig CONFIG_WITHOUT_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(ImmutableDefaultConfig
                            .builder().addAllThriftHosts(SERVERS).build())
                    .replicationFactor(1)
                    .credentials(CREDENTIALS)
                    .build();
    private static final CassandraKeyValueServiceConfig CONFIG_WITH_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(ImmutableDefaultConfig
                            .builder().addAllThriftHosts(SERVERS).build())
                    .keyspace(KEYSPACE)
                    .replicationFactor(1)
                    .credentials(CREDENTIALS)
                    .build();

    private static final KeyValueServiceRuntimeConfig INVALID_CKVS_RUNTIME_CONFIG = () -> "test";
    private static final KeyValueServiceRuntimeConfig DEFAULT_CKVS_RUNTIME_CONFIG =
            CassandraKeyValueServiceRuntimeConfig.getDefault();

    @Test
    public void throwsWhenPreprocessingNonCassandraKvsConfig() {
        assertThatThrownBy(() -> {
            KeyValueServiceConfigHelper keyValueServiceConfig = () -> "Fake KVS";
            CassandraAtlasDbFactory.preprocessKvsConfig(keyValueServiceConfig, Optional::empty, Optional.empty());
        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithNoKeyspaceAndNoNamespace() {
        assertThatThrownBy(() -> CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITHOUT_KEYSPACE, Optional::empty,
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithKeyspaceAndDifferentNamespace() {
        assertThatThrownBy(
                () -> CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITH_KEYSPACE, Optional::empty,
                        Optional.of(KEYSPACE_2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void resolvesConfigWithOriginalKeyspaceIfNoNamespaceProvided() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITH_KEYSPACE, Optional::empty, Optional.empty());
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(CONFIG_WITH_KEYSPACE.getKeyspaceOrThrow());
    }

    @Test
    public void resolvesConfigWithNamespaceIfNoKeyspaceProvided() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITHOUT_KEYSPACE, Optional::empty,
                        Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void preservesOtherPropertiesOnResolvedConfigWithNamespace() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITHOUT_KEYSPACE, Optional::empty,
                        Optional.of(KEYSPACE));
        assertThat(newConfig.servers())
                .isEqualTo(ImmutableDefaultConfig
                        .builder().addAllThriftHosts(SERVERS).build());
        assertThat(newConfig.replicationFactor()).isEqualTo(1);
    }

    @Test
    public void resolvesConfigIfKeyspaceAndNamespaceProvidedAndMatch() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITH_KEYSPACE, Optional::empty,
                        Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void emptyRuntimeConfigShouldResolveToDefaultConfig() {
        CassandraKeyValueServiceRuntimeConfig returnedConfig = new CassandraAtlasDbFactory()
                .preprocessKvsRuntimeConfig(Optional::empty)
                .get();

        assertEquals("Empty config should resolve to default", DEFAULT_CKVS_RUNTIME_CONFIG, returnedConfig);
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

        Supplier<CassandraKeyValueServiceRuntimeConfig> processedRuntimeConfig = new CassandraAtlasDbFactory()
                .preprocessKvsRuntimeConfig(runtimeConfigSupplier);
        CassandraKeyValueServiceRuntimeConfig firstReturnedConfig = processedRuntimeConfig.get();
        CassandraKeyValueServiceRuntimeConfig secondReturnedConfig = processedRuntimeConfig.get();

        assertEquals("First returned config should be valid", DEFAULT_CKVS_RUNTIME_CONFIG, firstReturnedConfig);
        assertEquals("Second invalid config should be ignored", DEFAULT_CKVS_RUNTIME_CONFIG, secondReturnedConfig);
    }

    @Test
    public void firstConfigInvalidShouldResolveToDefault() {
        CassandraKeyValueServiceRuntimeConfig returnedConfig = new CassandraAtlasDbFactory()
                .preprocessKvsRuntimeConfig(() -> Optional.of(INVALID_CKVS_RUNTIME_CONFIG))
                .get();

        assertEquals("First invalid config should resolve to default", DEFAULT_CKVS_RUNTIME_CONFIG, returnedConfig);
    }
}
