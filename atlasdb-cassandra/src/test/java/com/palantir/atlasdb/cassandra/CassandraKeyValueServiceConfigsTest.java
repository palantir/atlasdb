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

import java.net.InetSocketAddress;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class CassandraKeyValueServiceConfigsTest {
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
    public void canAddKeyspace() {
        CassandraKeyValueServiceConfig newConfig = CassandraKeyValueServiceConfigs.copyWithKeyspace(
                CONFIG_WITHOUT_KEYSPACE, KEYSPACE);
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE);
    }

    @Test
    public void otherPropertiesConservedWhenAddingKeyspace() {
        CassandraKeyValueServiceConfig newConfig = CassandraKeyValueServiceConfigs.copyWithKeyspace(
                CONFIG_WITHOUT_KEYSPACE, KEYSPACE);
        assertThat(newConfig.replicationFactor()).isEqualTo(1);
        assertThat(newConfig.servers()).isEqualTo(SERVERS);
    }

    @Test
    public void canReplaceKeyspace() {
        CassandraKeyValueServiceConfig newConfig = CassandraKeyValueServiceConfigs.copyWithKeyspace(
                CONFIG_WITH_KEYSPACE, KEYSPACE_2);
        assertThat(newConfig.getKeyspaceOrThrow()).isEqualTo(KEYSPACE_2);
    }
}
