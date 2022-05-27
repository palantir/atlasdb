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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class CassandraAtlasDbFactoryTest {
    private static final String KEYSPACE = "ks";
    private static final CassandraServersConfig SERVERS = ImmutableDefaultConfig.builder()
            .addAllThriftHosts(ImmutableSet.of(InetSocketAddress.createUnresolved("foo", 42)))
            .build();
    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();

    private static final CassandraKeyValueServiceConfig CONFIG_WITH_KEYSPACE =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .keyspace(KEYSPACE)
                    .credentials(CREDENTIALS)
                    .build();

    private static final CassandraKeyValueServiceRuntimeConfig DEFAULT_CKVS_RUNTIME_CONFIG =
            CassandraKeyValueServiceRuntimeConfig.getDefault();

    private CassandraAtlasDbFactory factory;

    @Before
    public void setUp() {
        factory = new CassandraAtlasDbFactory();
    }

    @Test
    public void derivedSnapshotConfigDefaultGetRangesConcurrencyOverriddenWhenInstallOverrideIsPresent() {
        int defaultGetRangesConcurrencyOverride = 200;
        CassandraKeyValueServiceConfig installConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CONFIG_WITH_KEYSPACE)
                .defaultGetRangesConcurrency(defaultGetRangesConcurrencyOverride)
                .build();
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                .from(DEFAULT_CKVS_RUNTIME_CONFIG)
                .servers(SERVERS)
                .replicationFactor(1)
                .build();
        DerivedSnapshotConfig derivedSnapshotConfig =
                factory.createDerivedSnapshotConfig(installConfig, Optional.of(runtimeConfig));
        assertThat(derivedSnapshotConfig.defaultGetRangesConcurrency()).isEqualTo(defaultGetRangesConcurrencyOverride);
    }
}
