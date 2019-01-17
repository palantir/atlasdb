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

package com.palantir.leader.lease;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

@RunWith(MockitoJUnitRunner.class)
public final class LeasingLeadershipTokenTests {
    private static final Duration TIMEOUT = Duration.ofNanos(10);

    @Mock private LeasingRequirements leasingRequirements;
    @Mock private Supplier<NanoTime> clock;
    @Mock private Supplier<StillLeadingStatus> stillLeadingStatus;
    @Mock private LeadershipToken wrappedToken;

    private LeasingLeadershipToken token;

    @Before
    public void before() {
        token = new LeasingLeadershipToken(leasingRequirements, clock, wrappedToken, TIMEOUT, stillLeadingStatus);
        when(stillLeadingStatus.get()).thenReturn(StillLeadingStatus.LEADING);
        when(leasingRequirements.canUseLeadershipLeases()).thenReturn(true);
    }

    @Test
    public void testCaches() {
        when(clock.get()).thenReturn(new NanoTime(0));
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.LEADING);
        when(clock.get()).thenReturn(new NanoTime(1));
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.LEADING);
        verify(stillLeadingStatus, times(1)).get();
    }

    @Test
    public void testFetchesNewStateIfCacheOld() {
        when(clock.get()).thenReturn(new NanoTime(0));
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.LEADING);
        when(stillLeadingStatus.get()).thenReturn(StillLeadingStatus.NOT_LEADING);
        when(clock.get()).thenReturn(new NanoTime(11));
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.NOT_LEADING);
    }

    @Test
    public void testSelectsTimeBeforeFetchingNewState() {
        when(clock.get()).thenReturn(new NanoTime(0));
        when(stillLeadingStatus.get()).thenAnswer(inv -> {
            when(clock.get()).thenReturn(new NanoTime(5));
            return StillLeadingStatus.LEADING;
        });
        token.getLeadershipStatus();
        when(clock.get()).thenReturn(new NanoTime(12));
        doReturn(StillLeadingStatus.NOT_LEADING).when(stillLeadingStatus).get();
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.NOT_LEADING);
    }

    @Test
    public void testDoesNotLeaseIfLeasingRequirementsSayNo() {
        when(leasingRequirements.canUseLeadershipLeases()).thenReturn(false);
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(token.getLeadershipStatus()).isEqualTo(StillLeadingStatus.LEADING);
        verify(stillLeadingStatus, times(2)).get();
    }
}
