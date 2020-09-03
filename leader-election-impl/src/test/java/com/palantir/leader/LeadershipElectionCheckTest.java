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
    private final TaggedMetricRegistry registry = mock(TaggedMetricRegistry.class);
    private final LeaderElectionServiceMetrics leaderElectionServiceMetrics =
            LeaderElectionServiceMetrics.of(registry);
    private final LeaderElectionHealthCheck leaderElectionHealthCheck =
            new LeaderElectionHealthCheck();

    @Before
    public void setup() {
        when(registry.meter(any())).thenReturn(new Meter(fakeTimeClock));
        leaderElectionHealthCheck.registerClient("abc", leaderElectionServiceMetrics);
    }

    @Test
    public void shouldBeHealthyForOneLeaderElectionPerMinute() {
        markLeaderElectionsAtSpecifiedInterval(5, Duration.ofSeconds(60));
        System.out.print(leaderElectionServiceMetrics.proposedLeadership().getFiveMinuteRate());

        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.HEALTHY);
    }

    @Test
    public void shouldBeUnhealthyForMoreThanOneLeaderElectionPerMinute() {
//        markLeaderElectionsAtSpecifiedInterval(5,  Duration.ofSeconds(15));
        DO();
        System.out.print(leaderElectionServiceMetrics.proposedLeadership().getFiveMinuteRate());
        assertThat(leaderElectionHealthCheck.leaderElectionRateHealthStatus())
                .isEqualTo(LeaderElectionHealthStatus.UNHEALTHY);
    }

    private void markLeaderElectionsAtSpecifiedInterval(int leaderElectionCount, Duration timeIntervalInSeconds) {
        // The rate is initialized after first tick (5 second interval) of meter with number of marks / interval.
        // Marking before the first interval has passed sets the rate very high, which should not happen in practice.
        fakeTimeClock.advance(80, TimeUnit.MINUTES);

        IntStream.range(0, leaderElectionCount).forEach(idx -> {
            leaderElectionServiceMetrics.proposedLeadership().mark();
            fakeTimeClock.advance(timeIntervalInSeconds.getSeconds(), TimeUnit.SECONDS);
        });
    }

    private void DO() {
        // The rate is initialized after first tick (5 second interval) of meter with number of marks / interval.
        // Marking before the first interval has passed sets the rate very high, which should not happen in practice.
        fakeTimeClock.advance(80, TimeUnit.MINUTES);

        doMore(2);

        doMore(2);

        doMore(28);

        doMore(7);

        doMore(10);

        doMore(10);

        doMore(10);

        doMore(13);

        doMore(15);

        doMore(12);

        doMore(11);

        doMore(10);
    }

    private void doMore(int i) {
        leaderElectionServiceMetrics.proposedLeadership().mark();
        fakeTimeClock.advance(i, TimeUnit.SECONDS);
        System.out.println(leaderElectionServiceMetrics.proposedLeadership().getFiveMinuteRate());
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


