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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

@RunWith(MockitoJUnitRunner.class)
public final class LeasingLeaderElectionServiceTests {
    private static final Duration TIMEOUT = Duration.ofMillis(100);
    @Mock private LeaderElectionService delegate;
    @Mock private LeadershipToken token;

    private LeaderElectionService leased;

    @Before
    public void before() throws InterruptedException {
        // cache forever
        leased = new LeasingLeaderElectionService(() -> true, delegate, Duration.ofDays(1), TIMEOUT);
        when(delegate.blockOnBecomingLeader()).thenReturn(token);
        when(delegate.getCurrentTokenIfLeading()).thenReturn(Optional.of(token));
    }

    @Test
    public void testThrowsInterruptedExceptionIfDelegateThrowsInterruptedException() throws InterruptedException {
        when(delegate.blockOnBecomingLeader()).thenThrow(new InterruptedException());
        assertThatExceptionOfType(InterruptedException.class).isThrownBy(() -> leased.blockOnBecomingLeader());
    }

    @Test
    public void testDelaysAcquiringLeadership() throws Exception {
        long before = System.nanoTime();
        leased.blockOnBecomingLeader();
        long after = System.nanoTime();
        assertThat(after - before).isGreaterThan(TIMEOUT.toNanos());
    }

    @Test
    public void testIsNotLeadingIfWaiting() {
        Thread thread = new Thread(() -> {
            try {
                leased.blockOnBecomingLeader();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        while (thread.getState() != Thread.State.TIMED_WAITING) {
            Thread.yield();
        }
        assertThat(leased.getCurrentTokenIfLeading()).isEmpty();
    }

    @Test
    public void leasesLeadershipState() throws InterruptedException {
        LeadershipToken leasedToken = leased.blockOnBecomingLeader();
        when(delegate.isStillLeading(token)).thenReturn(StillLeadingStatus.LEADING);
        assertThat(leased.isStillLeading(leasedToken)).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(leased.isStillLeading(leasedToken)).isEqualTo(StillLeadingStatus.LEADING);
        verify(delegate).isStillLeading(token);

    }

    @Test
    public void failsLoudlyIfWrongTypeOfLeadershipToken() {
        assertThatExceptionOfType(ClassCastException.class).isThrownBy(() -> leased.isStillLeading(token));
    }
}
