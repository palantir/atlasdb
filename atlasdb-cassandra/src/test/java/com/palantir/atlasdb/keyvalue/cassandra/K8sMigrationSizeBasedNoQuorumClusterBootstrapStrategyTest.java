/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.ClusterTopologyResult;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategyTest {
    private static final CassandraServer CASSANDRA_SERVER_1 =
            CassandraServer.of("cassandra1", InetSocketAddress.createUnresolved("one", 1234));
    private static final CassandraServer CASSANDRA_SERVER_2 =
            CassandraServer.of("cassandra2", InetSocketAddress.createUnresolved("two", 1234));
    private static final CassandraServer CASSANDRA_SERVER_3 =
            CassandraServer.of("cassandra3", InetSocketAddress.createUnresolved("three", 1234));

    private final AtomicReference<CassandraServersConfig> config = new AtomicReference<>();
    private final NoQuorumClusterBootstrapStrategy strategy =
            new K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategy(config::get);

    @Test
    public void returnsNoQuorumIfNoResultsProvided() {
        assertThat(strategy.accept(ImmutableMap.of())).isEqualTo(ClusterTopologyResult.noQuorum());
    }

    @Test
    public void returnsNoQuorumIfOnlyHardFailuresProvided() {
        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1,
                        HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_2,
                        HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_3,
                        HostIdResult.hardFailure())))
                .isEqualTo(ClusterTopologyResult.noQuorum());
    }

    @Test
    public void returnsNoQuorumIfOnlySoftFailuresProvided() {
        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1,
                        HostIdResult.softFailure(),
                        CASSANDRA_SERVER_2,
                        HostIdResult.softFailure(),
                        CASSANDRA_SERVER_3,
                        HostIdResult.softFailure())))
                .isEqualTo(ClusterTopologyResult.noQuorum());
    }

    @Test
    public void returnsDissentIfTwoTopologiesAvailable() {
        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1,
                        HostIdResult.success(ImmutableList.of("tom")),
                        CASSANDRA_SERVER_2,
                        HostIdResult.success(ImmutableList.of("tom")),
                        CASSANDRA_SERVER_3,
                        HostIdResult.success(ImmutableList.of("harry")))))
                .isEqualTo(ClusterTopologyResult.dissent());
    }

    @Test
    public void returnsDissentIfThreeTopologiesAvailable() {
        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1,
                        HostIdResult.success(ImmutableList.of("tom")),
                        CASSANDRA_SERVER_2,
                        HostIdResult.success(ImmutableList.of("dick")),
                        CASSANDRA_SERVER_3,
                        HostIdResult.success(ImmutableList.of("harry")))))
                .isEqualTo(ClusterTopologyResult.dissent());
    }

    @Test
    public void returnsNoQuorumIfHostIdsSizeDoesNotMatchConfig() {
        config.set(ImmutableDefaultConfig.builder()
                .addThriftHosts(CASSANDRA_SERVER_1.proxy(), CASSANDRA_SERVER_2.proxy(), CASSANDRA_SERVER_3.proxy())
                .build());
    }

    @Test
    public void returnsNoQuorumIfNoPossibleQuorumInOriginalCloud() {
        // TODO
    }

    @Test
    public void returnsConsensusIfAllConditionsSatisfied() {
        // TODO
    }
}
