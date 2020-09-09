/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.palantir.leader.health.LeaderElectionHealthCheck;
import com.palantir.leader.health.LeaderElectionHealthStatus;
import com.palantir.paxos.Client;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class LeadershipElectionCheckTest {
    private final FakeTimeClock fakeTimeClock = new FakeTimeClock();
    private final TaggedMetricRegistry registry = mock(TaggedMetricRegistry.class);
    private final TaggedMetricRegistry registry2 = mock(TaggedMetricRegistry.class);

    private final LeaderElectionServiceMetrics leaderElectionServiceMetrics =
            LeaderElectionServiceMetrics.of(registry);
    private final LeaderElectionServiceMetrics leaderElectionServiceMetrics2 =
            LeaderElectionServiceMetrics.of(registry2);

    private final LeaderElectionHealthCheck leaderElectionHealthCheck = new LeaderElectionHealthCheck();
    private final LeaderElectionHealthCheck leaderElectionHealthCheckForOnlyClient1 = new LeaderElectionHealthCheck();
    private final LeaderElectionHealthCheck leaderElectionHealthCheckForOnlyClient2 = new LeaderElectionHealthCheck();

    private static final Client CLIENT_1 = Client.of("abc");
    private static final Client CLIENT_2 = Client.of("abc_2");

    private Map<Client, LeaderElectionServiceMetrics> clientLeaderElectionServiceMetricsMap
            = ImmutableMap.of(CLIENT_1, leaderElectionServiceMetrics, CLIENT_2, leaderElectionServiceMetrics2);

    @Before
    public void setup() {
        leaderElectionHealthCheck.registerClient(CLIENT_1, leaderElectionServiceMetrics);
        leaderElectionHealthCheck.registerClient(CLIENT_2, leaderElectionServiceMetrics2);

        leaderElectionHealthCheckForOnlyClient1.registerClient(CLIENT_1, leaderElectionServiceMetrics);
        leaderElectionHealthCheckForOnlyClient2.registerClient(CLIENT_2, leaderElectionServiceMetrics2);

        when(registry.meter(any())).thenReturn(new Meter(fakeTimeClock));
        when(registry2.meter(any())).thenReturn(new Meter(fakeTimeClock));
    }

    @Test
    public void shouldBeHealthyForOneLeaderElectionPerMinute() {
        markLeaderElectionsAtSpecifiedInterval(CLIENT_1, 5, Duration.ofSeconds(60));
        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);
    }

    @Test
    public void shouldBeUnhealthyForMoreThanOneLeaderElectionPerMinute() {
        markLeaderElectionsAtSpecifiedInterval(CLIENT_1, 5,  Duration.ofSeconds(10));
        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.UNHEALTHY);
    }

    @Test
    public void shouldBeHealthyForOneLeaderElectionPerMinuteAcrossClients() {
        markLeaderElectionsAtSpecifiedInterval(CLIENT_1, 5,  Duration.ofSeconds(60));
        markLeaderElectionsAtSpecifiedInterval(CLIENT_2, 5,  Duration.ofSeconds(60));

        assertThat(leaderElectionHealthCheckForOnlyClient1.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);

        assertThat(leaderElectionHealthCheckForOnlyClient2.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);

        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);
    }

    @Test
    public void shouldBeUnhealthyOverallEvenIfIndividualClientsAreHealthy() {
        markLeaderElectionsAtSpecifiedInterval(CLIENT_1, 2,  Duration.ofSeconds(10));
        markLeaderElectionsAtSpecifiedInterval(CLIENT_2, 3,  Duration.ofSeconds(10));

        assertThat(leaderElectionHealthCheckForOnlyClient1.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);

        assertThat(leaderElectionHealthCheckForOnlyClient2.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);

        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.UNHEALTHY);
    }

    private void markLeaderElectionsAtSpecifiedInterval(Client client,
            int leaderElectionCount,
            Duration timeIntervalInSeconds) {
        // The rate is initialized after first tick (5 second interval) of meter with number of marks / interval.
        // Marking before the first interval has passed sets the rate very high, which should not happen in practice.
        fakeTimeClock.advance(6, TimeUnit.SECONDS);
        LeaderElectionServiceMetrics metrics = clientLeaderElectionServiceMetricsMap.get(client);

        IntStream.range(0, leaderElectionCount).forEach(idx -> {
            metrics.proposedLeadership().mark();
            fakeTimeClock.advance(timeIntervalInSeconds.getSeconds(), TimeUnit.SECONDS);
        });
    }

    public static class FakeTimeClock extends Clock {
        private final AtomicLong nanos = new AtomicLong();

        public void advance(long time, TimeUnit timeUnit) {
            nanos.addAndGet(timeUnit.toNanos(time));
        }

        @Override
        public long getTick() {
            return nanos.get();
        }
    }
}


