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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

@RunWith(MockitoJUnitRunner.class)
public final class LeadershipLeaseLeaderElectionServiceTests {
    private static final Duration TIMEOUT = Duration.ofMillis(100);
    @Mock private LeaderElectionService delegate;
    @Mock private LeadershipToken token;

    private LeaderElectionService leased;

    @Before
    public void before() throws InterruptedException {
        // cache forever
        leased = new LeadershipLeaseLeaderElectionService(delegate, Duration.ofDays(1), TIMEOUT);
        when(delegate.blockOnBecomingLeader()).thenReturn(token);
        when(delegate.getCurrentTokenIfLeading()).thenReturn(Optional.of(token));
    }

    @Test
    public void testDelaysAcquiringLeadership_block() throws Exception {
        testDelaysAcquiringLeadership(leased::blockOnBecomingLeader);
    }

    // Yes, this test is necessary; it stops race conditions from poisoning the well
    @Test
    public void testDelaysAcquiringLeadership_ifLeading() throws Exception {
        testDelaysAcquiringLeadership(() -> leased.getCurrentTokenIfLeading().get());
    }

    private void testDelaysAcquiringLeadership(Callable<LeadershipToken> tokenSupplier) throws Exception {
        long before = System.nanoTime();
        tokenSupplier.call();
        long after = System.nanoTime();
        assertThat(after - before).isGreaterThan(TimeUnit.MILLISECONDS.toNanos(100));
    }

    @Test
    public void leasesLeadershipState_block() throws InterruptedException {
        leasesLeadershipState(leased.blockOnBecomingLeader());
    }

    @Test
    public void leasesLeadershipState_ifLeading() {
        leasesLeadershipState(leased.getCurrentTokenIfLeading().get());
    }

    private void leasesLeadershipState(LeadershipToken leasedToken) {
        when(delegate.isStillLeading(token)).thenReturn(StillLeadingStatus.LEADING);
        assertThat(leased.isStillLeading(leasedToken)).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(leased.isStillLeading(leasedToken)).isEqualTo(StillLeadingStatus.LEADING);
        verify(delegate).isStillLeading(token);

    }

    @Test
    public void failsLoudlyIfWrongTypeOfLeadershipToken() {
        assertThatExceptionOfType(ClassCastException.class)
                .isThrownBy(() -> leased.isStillLeading(token));
    }
}
