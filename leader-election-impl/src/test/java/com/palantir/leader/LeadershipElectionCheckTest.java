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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.palantir.leader.health.LeaderElectionHealthCheck;
import com.palantir.leader.health.LeaderElectionHealthStatus;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class LeadershipElectionCheckTest {
    private final FakeTimeClock fakeTimeClock = new FakeTimeClock();
    private final TaggedMetricRegistry registry1 = mock(TaggedMetricRegistry.class);
    private final LeaderElectionServiceMetrics leaderElectionServiceMetrics =
            LeaderElectionServiceMetrics.of(registry1);
    private final LeaderElectionHealthCheck leaderElectionHealthCheck = new LeaderElectionHealthCheck(
        leaderElectionServiceMetrics);

    @Before
    public void setup() {
        when(registry1.meter(any())).thenReturn(new Meter(fakeTimeClock));
    }

    @Test
    public void shouldBeHealthyForLeaderElectionRateLessThanOne() {
        markLeaderElections(1);
        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);
        assertThat(leaderElectionServiceMetrics.proposedLeadership().getFiveMinuteRate())
                .isEqualTo(0.2);
    }

    @Test
    public void shouldBeUnhealthyForLeaderElectionRateGreaterThanOne() {
        markLeaderElections(5);
        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.UNHEALTHY);
        assertThat(leaderElectionServiceMetrics.proposedLeadership().getFiveMinuteRate())
                .isEqualTo(1.0);
    }

    public void markLeaderElections(int count) {
        IntStream.range(0, count).forEach(idx -> leaderElectionServiceMetrics.proposedLeadership().mark());

        // com.codahale.metrics.Meter.TICK_INTERVAL < advanceTime < com.codahale.metrics.Meter.TICK_INTERVAL * 2
        // to maintain single tick
        fakeTimeClock.advance(6, TimeUnit.SECONDS);
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


