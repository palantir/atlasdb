/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;

public class CassandraAtlasDbFactoryTest {
    private static final String KEYSPACE = "ks";
    private static final String KEYSPACE_2 = "ks2";
    private static final ImmutableSet<InetSocketAddress> SERVERS = ImmutableSet.of(new InetSocketAddress("foo", 42));
    private static final CassandraKeyValueServiceConfig CONFIG_WITHOUT_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(SERVERS)
                    .replicationFactor(1)
                    .build();
    private static final CassandraKeyValueServiceConfig CONFIG_WITH_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(SERVERS)
                    .keyspace(KEYSPACE)
                    .replicationFactor(1)
                    .build();

    @Test
    public void throwsWhenPreprocessingNonCassandraKvsConfig() {
        assertThatThrownBy(() -> {
            KeyValueServiceConfigHelper keyValueServiceConfig = () -> "Fake KVS";
            CassandraAtlasDbFactory.preprocessKvsConfig(keyValueServiceConfig, runtimeConfig, Optional.empty());
        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithNoKeyspaceAndNoNamespace() {
        assertThatThrownBy(() -> CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITHOUT_KEYSPACE, runtimeConfig,
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsWhenPreprocessingConfigWithKeyspaceAndDifferentNamespace() {
        assertThatThrownBy(
                () -> CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITH_KEYSPACE, runtimeConfig, Optional.of(KEYSPACE_2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void resolvesConfigWithOriginalKeyspaceIfNoNamespaceProvided() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITH_KEYSPACE, runtimeConfig, Optional.empty());
        assertThat(newConfig).isEqualTo(CONFIG_WITH_KEYSPACE);
    }

    @Test
    public void resolvesConfigWithNamespaceIfNoKeyspaceProvided() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITHOUT_KEYSPACE, runtimeConfig, Optional.of(KEYSPACE));
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void preservesOtherPropertiesOnResolvedConfigWithNamespace() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITHOUT_KEYSPACE, runtimeConfig, Optional.of(KEYSPACE));
        assertThat(newConfig.servers()).isEqualTo(SERVERS);
        assertThat(newConfig.replicationFactor()).isEqualTo(1);
    }

    @Test
    public void resolvesConfigIfKeyspaceAndNamespaceProvidedAndMatch() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraAtlasDbFactory.preprocessKvsConfig(CONFIG_WITH_KEYSPACE, runtimeConfig, Optional.of(KEYSPACE));
        assertThat(newConfig).isEqualTo(CONFIG_WITH_KEYSPACE);
    }
}
