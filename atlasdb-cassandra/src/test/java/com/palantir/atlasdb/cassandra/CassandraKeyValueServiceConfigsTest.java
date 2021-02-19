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
    private static final ImmutableSet<InetSocketAddress> SERVERS = ImmutableSet.of(new InetSocketAddress("foo", 42));
    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
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
        KeyValueServiceRuntimeConfig config = AtlasDbConfigs.OBJECT_MAPPER.readValue(
                "type: CassandraKeyValueServiceRuntimeConfig", KeyValueServiceRuntimeConfig.class);
    }
}
