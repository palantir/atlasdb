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
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.paxos.LeadershipComponents.LeadershipContext;
import com.palantir.atlasdb.timelock.paxos.LeadershipComponents.LeadershipProxies;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.PaxosLeadershipToken;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.LeadershipCoordinator;
import com.palantir.lock.LockService;
import com.palantir.paxos.Client;
import com.palantir.timelock.paxos.HealthCheckPinger;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LeadershipComponentsTest {

    private final Client proxyClient = Client.of("test-proxy-client");

    @Mock
    LeaderElectionService leaderElectionService;

    @Mock
    HealthCheckPinger local;

    @Mock
    HealthCheckPinger remote1;

    @Mock
    HealthCheckPinger remote2;

    @Mock
    PingableLeader localPingableLeader;

    @Mock
    AsyncTimelockService asyncTimeLockService;

    @Mock
    LockService lockService;

    @Mock
    PaxosLeadershipToken leadershipToken;

    @BeforeEach
    public void before() throws InterruptedException {
        when(leaderElectionService.blockOnBecomingLeader()).thenReturn(leadershipToken);
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.empty());
        when(leaderElectionService.isStillLeading(leadershipToken))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.LEADING));
    }

    @Test
    void testInstrumentation() throws IOException {
        LeadershipCoordinator leadershipCoordinator = LeadershipCoordinator.create(leaderElectionService);
        MetricRegistry metricRegistry = new MetricRegistry();
        DefaultTaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
        MetricsManager metricsManager = new MetricsManager(metricRegistry, taggedMetricRegistry, table -> {
            assertThat(table).isNotNull();
            return true;
        });
        TimelockPaxosMetrics metrics =
                ImmutableTimelockPaxosMetrics.of(PaxosUseCase.LEADER_FOR_ALL_CLIENTS, metricsManager);
        AutobatchingLeadershipObserverFactory leadershipObserverFactory =
                AutobatchingLeadershipObserverFactory.create(leadershipEventClientSetMultimap -> {
                    assertThat(leadershipEventClientSetMultimap).isNotNull();
                    assertThat(leadershipEventClientSetMultimap.asMap()).isNotEmpty();
                });
        TimelockLeadershipMetrics leadershipMetrics = ImmutableTimelockLeadershipMetrics.builder()
                .metrics(metrics)
                .leaderUuid(UUID.randomUUID())
                .localPingableLeader(localPingableLeader)
                .leadershipObserverFactory(leadershipObserverFactory)
                .proxyClient(proxyClient)
                .build();
        LeadershipContext leadershipContext = ImmutableLeadershipContext.builder()
                .leadershipCoordinator(leadershipCoordinator)
                .leaderElectionService(leaderElectionService)
                .leadershipMetrics(leadershipMetrics)
                .build();
        NetworkClientFactories.Factory<LeadershipContext> leadershipContextFactory = client -> {
            assertThat(client).isNotNull();
            return leadershipContext;
        };

        LeadershipComponents leadershipComponents = new LeadershipComponents(
                leadershipContextFactory, LocalAndRemotes.of(local, List.of(remote1, remote2)));
        try {
            assertThat(leadershipComponents).isNotNull();
            LeadershipProxies services =
                    leadershipComponents.createServices(proxyClient, () -> asyncTimeLockService, () -> lockService);
            try (AsyncTimelockService asyncTimeLock = services.asyncTimelockService()) {
                when(this.lockService.currentTimeMillis()).thenReturn(24L);
                assertThat(services.lockService().currentTimeMillis()).isEqualTo(24);
                verify(this.lockService).currentTimeMillis();
                verifyNoMoreInteractions(this.lockService);

                when(this.asyncTimeLockService.currentTimeMillis()).thenReturn(42L);
                assertThat(asyncTimeLock.currentTimeMillis()).isEqualTo(42);
                verify(this.asyncTimeLockService).currentTimeMillis();
                verifyNoMoreInteractions(this.asyncTimeLockService);
            }

            Map<MetricName, Metric> taggedMetrics = taggedMetricRegistry.getMetrics();
            assertThat(taggedMetrics.keySet().stream().map(MetricName::safeName))
                    .containsExactlyInAnyOrder(
                            "com.palantir.atlasdb.timelock.AsyncTimelockService.close",
                            "com.palantir.atlasdb.timelock.AsyncTimelockService.currentTimeMillis",
                            "com.palantir.lock.LockService.currentTimeMillis");

            assertThat(taggedMetrics)
                    .isNotEmpty()
                    .hasSize(3)
                    .extractingByKey(MetricName.builder()
                            .safeName("com.palantir.lock.LockService.currentTimeMillis")
                            .safeTags(Map.of(
                                    "client",
                                    "test-proxy-client",
                                    "isCurrentSuspectedLeader",
                                    "false",
                                    "paxosUseCase",
                                    "leaderPaxos"))
                            .build())
                    .asInstanceOf(type(Timer.class))
                    .satisfies(meter -> assertThat(meter.getCount()).isOne());
        } finally {
            leadershipComponents.shutdown();
        }
    }
}
