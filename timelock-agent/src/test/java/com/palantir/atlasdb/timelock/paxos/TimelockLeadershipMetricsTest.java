/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.UUID;
import java.util.function.IntSupplier;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimelockLeadershipMetricsTest {

    private final Client proxyClient = Client.of("test-proxy-client");

    @Mock
    PingableLeader localPingableLeader;

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final DefaultTaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
    private final MetricsManager metricsManager = new MetricsManager(metricRegistry, taggedMetricRegistry, table -> {
        assertThat(table).isNotNull();
        return true;
    });

    @Test
    void testMetrics() {
        PaxosUseCase paxosUseCase = PaxosUseCase.LEADER_FOR_ALL_CLIENTS;
        TimelockPaxosMetrics metrics = ImmutableTimelockPaxosMetrics.of(paxosUseCase, metricsManager);
        assertThat(metrics.asMetricsManager()).isNotNull();
        assertThat(metrics.legacyMetrics()).isEqualTo(metricRegistry);
        assertThat(metrics.asMetricsManager().getRegistry()).isEqualTo(metricRegistry);
        assertThat(metrics.metrics()).isNotEqualTo(taggedMetricRegistry);
        assertThat(metrics.asMetricsManager().getTaggedRegistry())
                .isNotEqualTo(taggedMetricRegistry)
                .satisfies(
                        taggedMetrics -> assertThat(taggedMetrics.getMetrics()).isEmpty());

        assertThat(metrics.paxosUseCase()).isEqualTo(paxosUseCase);
        assertThat(metrics.metrics().getMetrics()).isEmpty();

        assertThat(metrics.asMetricsManager().getTaggedRegistry().getMetrics()).isEmpty();
        TimelockLeadershipMetrics leadershipMetrics = ImmutableTimelockLeadershipMetrics.builder()
                .metrics(metrics)
                .leaderUuid(UUID.randomUUID())
                .localPingableLeader(localPingableLeader)
                .leadershipObserverFactory(
                        AutobatchingLeadershipObserverFactory.create(leadershipEventClientSetMultimap -> {
                            assertThat(leadershipEventClientSetMultimap).isNotNull();
                            assertThat(leadershipEventClientSetMultimap.asMap()).isNotEmpty();
                        }))
                .proxyClient(proxyClient)
                .build();
        LeadershipObserver leadershipObserver = leadershipMetrics.leadershipObserver();
        assertThat(leadershipObserver).isNotNull();
        assertThat(metrics.asMetricsManager().getTaggedRegistry().getMetrics()).isEmpty();

        assertThat(leadershipMetrics.suspectedLeaderTag().apply(null))
                .isNotNull()
                .isNotEmpty()
                .hasSize(1)
                .containsEntry("isCurrentSuspectedLeader", "false");

        TaggedMetricRegistry leadershipTaggedMetrics = leadershipMetrics.taggedMetrics();
        assertThat(leadershipTaggedMetrics).isNotNull().isNotEqualTo(taggedMetricRegistry);

        assertThat(leadershipMetrics.namespaceAsLoggingArgs())
                .contains(SafeArg.of("paxosUseCase", "leaderPaxos"), SafeArg.of("client", "test-proxy-client"));

        PaxosLeadershipEventRecorder eventRecorder = leadershipMetrics.eventRecorder();
        assertThat(eventRecorder).isNotNull();
        eventRecorder.recordLeaderPingTimeout();

        leadershipObserver.gainedLeadership();
        leadershipObserver.lostLeadership();

        assertThat(metrics.asMetricsManager().getTaggedRegistry().getMetrics())
                .satisfies(metricNameMetricMap -> assertThat(
                                metricNameMetricMap.keySet().stream().map(MetricName::safeName))
                        .contains("leaderElectionService.leaderPingTimeout"));
    }

    @Test
    void testInstrument() {
        PaxosUseCase paxosUseCase = PaxosUseCase.LEADER_FOR_ALL_CLIENTS;
        TimelockPaxosMetrics metrics = ImmutableTimelockPaxosMetrics.of(paxosUseCase, metricsManager);
        assertThat(metrics.instrument(IntSupplier.class, () -> 42))
                .extracting(IntSupplier::getAsInt)
                .asInstanceOf(InstanceOfAssertFactories.INTEGER)
                .isEqualTo(42);
        assertThat(metrics.metrics().getMetrics())
                .isNotEmpty()
                .extractingByKey(MetricName.builder()
                        .safeName("java.util.function.IntSupplier.getAsInt")
                        .build())
                .asInstanceOf(InstanceOfAssertFactories.type(Counting.class))
                .satisfies(counter -> assertThat(counter.getCount()).isOne());
    }

    @Test
    void testFactory() {
        PaxosUseCase paxosUseCase = PaxosUseCase.LEADER_FOR_ALL_CLIENTS;
        TimelockPaxosMetrics metrics = ImmutableTimelockPaxosMetrics.of(paxosUseCase, metricsManager);
        try (AutobatchingLeadershipObserverFactory factory = TimelockLeadershipMetrics.createFactory(metrics)) {
            LeadershipObserver leadershipObserver = factory.create(proxyClient);
            leadershipObserver.gainedLeadership();
            leadershipObserver.lostLeadership();
        }
    }
}
