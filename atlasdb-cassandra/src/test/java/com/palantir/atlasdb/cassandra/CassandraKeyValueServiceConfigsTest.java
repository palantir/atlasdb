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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import org.junit.Test;

public class CassandraKeyValueServiceConfigsTest {
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

    @Test
    public void canDeserialize() throws IOException, URISyntaxException {
        CassandraKeyValueServiceConfig testConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .servers(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(SERVERS)
                        .build())
                .addressTranslation(ImmutableMap.of("test", Iterables.getOnlyElement(SERVERS)))
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
    public void canAddKeyspace() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraKeyValueServiceConfigs.copyWithKeyspace(CONFIG_WITHOUT_KEYSPACE, KEYSPACE);
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void otherPropertiesConservedWhenAddingKeyspace() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraKeyValueServiceConfigs.copyWithKeyspace(CONFIG_WITHOUT_KEYSPACE, KEYSPACE);
        assertThat(newConfig.replicationFactor()).isEqualTo(1);
        assertThat(newConfig.servers())
                .isEqualTo(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(SERVERS)
                        .build());
    }

    @Test
    public void canReplaceKeyspace() {
        CassandraKeyValueServiceConfig newConfig =
                CassandraKeyValueServiceConfigs.copyWithKeyspace(CONFIG_WITH_KEYSPACE, KEYSPACE_2);
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE_2);
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
                .build();

        URL configUrl =
                CassandraKeyValueServiceConfigsTest.class.getClassLoader().getResource("testRuntimeConfig.yml");

        CassandraKeyValueServiceRuntimeConfig deserializedConfig = AtlasDbConfigs.OBJECT_MAPPER.readValue(
                new File(configUrl.getPath()), CassandraKeyValueServiceRuntimeConfig.class);

        assertThat(deserializedConfig).isEqualTo(expectedConfig);
    }

    @Test
    public void canParseRuntimeDeprecatedConfigType() throws IOException {
        AtlasDbConfigs.OBJECT_MAPPER.readValue(
                "type: CassandraKeyValueServiceRuntimeConfig", KeyValueServiceRuntimeConfig.class);
    }
}
