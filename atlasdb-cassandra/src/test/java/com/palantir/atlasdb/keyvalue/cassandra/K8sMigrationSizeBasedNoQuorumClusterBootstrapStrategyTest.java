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
    private static final CassandraServer CASSANDRA_SERVER_4 =
            CassandraServer.of("cassandra4", InetSocketAddress.createUnresolved("four", 1234));
    private static final CassandraServer CASSANDRA_SERVER_5 =
            CassandraServer.of("cassandra5", InetSocketAddress.createUnresolved("five", 1234));
    private static final CassandraServer CASSANDRA_SERVER_6 =
            CassandraServer.of("cassandra6", InetSocketAddress.createUnresolved("six", 1234));

    public static final ImmutableDefaultConfig CONFIGURATION_WITH_THREE_NODES = ImmutableDefaultConfig.builder()
            .addThriftHosts(CASSANDRA_SERVER_1.proxy(), CASSANDRA_SERVER_2.proxy(), CASSANDRA_SERVER_3.proxy())
            .build();
    public static final ImmutableDefaultConfig CONFIGURATION_WITH_SIX_NODES = ImmutableDefaultConfig.builder()
            .addThriftHosts(
                    CASSANDRA_SERVER_1.proxy(),
                    CASSANDRA_SERVER_2.proxy(),
                    CASSANDRA_SERVER_3.proxy(),
                    CASSANDRA_SERVER_4.proxy(),
                    CASSANDRA_SERVER_5.proxy(),
                    CASSANDRA_SERVER_6.proxy())
            .build();
    public static final HostIdResult SUCCESS_WITH_SIX_IDS =
            HostIdResult.success(ImmutableList.of("1", "2", "3", "4", "5", "6"));

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
                        HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_4,
                        HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_5,
                        HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_6,
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
                        HostIdResult.softFailure(),
                        CASSANDRA_SERVER_4,
                        HostIdResult.softFailure(),
                        CASSANDRA_SERVER_5,
                        HostIdResult.softFailure(),
                        CASSANDRA_SERVER_6,
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
                        HostIdResult.success(ImmutableList.of("tom")),
                        CASSANDRA_SERVER_4,
                        HostIdResult.success(ImmutableList.of("tom")),
                        CASSANDRA_SERVER_5,
                        HostIdResult.success(ImmutableList.of("tom")),
                        CASSANDRA_SERVER_6,
                        HostIdResult.success(ImmutableList.of("harry")))))
                .isEqualTo(ClusterTopologyResult.dissent());
    }

    @Test
    public void returnsDissentIfMultipleTopologiesAvailable() {
        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1,
                        HostIdResult.success(ImmutableList.of("one")),
                        CASSANDRA_SERVER_2,
                        HostIdResult.success(ImmutableList.of("two")),
                        CASSANDRA_SERVER_3,
                        HostIdResult.success(ImmutableList.of("three")),
                        CASSANDRA_SERVER_4,
                        HostIdResult.success(ImmutableList.of("four")),
                        CASSANDRA_SERVER_5,
                        HostIdResult.success(ImmutableList.of("five")),
                        CASSANDRA_SERVER_6,
                        HostIdResult.success(ImmutableList.of("six")))))
                .isEqualTo(ClusterTopologyResult.dissent());
    }

    @Test
    public void returnsConsensusIfNumberOfHostIdsMatchesConfigurationAndQuorumInOldCloudPossible() {
        config.set(CONFIGURATION_WITH_SIX_NODES);

        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1, SUCCESS_WITH_SIX_IDS,
                        CASSANDRA_SERVER_2, SUCCESS_WITH_SIX_IDS,
                        CASSANDRA_SERVER_3, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_4, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_5, HostIdResult.hardFailure())))
                .isEqualTo(ClusterTopologyResult.consensus(ImmutableConsistentClusterTopology.builder()
                        .addServersInConsensus(CASSANDRA_SERVER_1, CASSANDRA_SERVER_2)
                        .hostIds(SUCCESS_WITH_SIX_IDS.hostIds())
                        .build()));
    }

    @Test
    public void returnsNoQuorumIfNotEnoughNodesAgreeToConstituteQuorumInOldCloud() {
        config.set(CONFIGURATION_WITH_SIX_NODES);

        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1, SUCCESS_WITH_SIX_IDS,
                        CASSANDRA_SERVER_2, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_3, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_4, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_5, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_6, HostIdResult.hardFailure())))
                .isEqualTo(ClusterTopologyResult.noQuorum());
    }

    @Test
    public void returnsConsensusIfNumberOfHostIdsMatchesDiscoveredAndQuorumInNewCloudPossible() {
        config.set(CONFIGURATION_WITH_THREE_NODES);

        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1, SUCCESS_WITH_SIX_IDS,
                        CASSANDRA_SERVER_2, SUCCESS_WITH_SIX_IDS,
                        CASSANDRA_SERVER_3, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_4, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_5, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_6, HostIdResult.hardFailure())))
                .isEqualTo(ClusterTopologyResult.consensus(ImmutableConsistentClusterTopology.builder()
                        .addServersInConsensus(CASSANDRA_SERVER_1, CASSANDRA_SERVER_2)
                        .hostIds(SUCCESS_WITH_SIX_IDS.hostIds())
                        .build()));
    }

    @Test
    public void returnsNoQuorumIfNotEnoughNodesAgreeToConstituteQuorumInNewCloud() {
        config.set(CONFIGURATION_WITH_THREE_NODES);

        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1, SUCCESS_WITH_SIX_IDS,
                        CASSANDRA_SERVER_2, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_3, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_4, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_5, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_6, HostIdResult.hardFailure())))
                .isEqualTo(ClusterTopologyResult.noQuorum());
    }

    @Test
    public void returnsNoQuorumIfNumberOfHostIdsMatchesNeitherConfigurationNorDiscovery() {
        config.set(CONFIGURATION_WITH_SIX_NODES);

        HostIdResult successWithSevenIds = HostIdResult.success(ImmutableList.of("1", "2", "3", "4", "5", "6", "7"));
        assertThat(strategy.accept(ImmutableMap.of(
                        CASSANDRA_SERVER_1, successWithSevenIds,
                        CASSANDRA_SERVER_2, successWithSevenIds,
                        CASSANDRA_SERVER_3, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_4, HostIdResult.hardFailure(),
                        CASSANDRA_SERVER_5, HostIdResult.hardFailure())))
                .isEqualTo(ClusterTopologyResult.noQuorum());
    }
}
