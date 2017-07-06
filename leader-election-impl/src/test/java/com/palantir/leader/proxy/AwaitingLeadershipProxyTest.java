/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.leader.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.PaxosLeadershipToken;

public class AwaitingLeadershipProxyTest {
    private static final String TEST_MESSAGE = "test_message";

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenLeading() throws Exception {
        Runnable mockRunnable = mock(Runnable.class);
        Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);
        LeaderElectionService mockLeader = mock(LeaderElectionService.class);

        when(mockLeader.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeader.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.LEADING);

        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, mockLeader);

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy.equals(proxy)).isTrue();
        assertThat(proxy.equals(null)).isFalse();
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenNotLeading() throws Exception {
        Runnable mockRunnable = mock(Runnable.class);
        Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);
        LeaderElectionService mockLeader = mock(LeaderElectionService.class);

        when(mockLeader.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeader.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, mockLeader);

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy.equals(proxy)).isTrue();
        assertThat(proxy.equals(null)).isFalse();
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    public void shouldMapInterruptedExceptionToNCLEIfLeadingStatusChanges() throws Exception {
        //leadership lost while request in flight
        Callable proxy = getRunnableProxyWithCheckedException(
                new InterruptedException(TEST_MESSAGE),
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        assertThatThrownBy(proxy::call).isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("received an interrupt due to leader election.")
                .hasCauseExactlyInstanceOf(InterruptedException.class)
                .hasStackTraceContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapInterruptedExceptionToNCLEIfLeadingStatusDoesntChange() throws InterruptedException {
        //Always leading
        Callable proxy = getRunnableProxyWithCheckedException(
                new InterruptedException(TEST_MESSAGE),
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.LEADING);

        assertThatThrownBy(proxy::call).isInstanceOf(InterruptedException.class).hasMessage(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapOtherExceptionToNCLEIfLeadingStatusChanges() throws InterruptedException {
        //leadership lost while request in flight
        Callable proxy = getRunnableProxyWithCheckedException(
                new IOException(TEST_MESSAGE),
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        assertThatThrownBy(proxy::call).isNotInstanceOf(NotCurrentLeaderException.class).isInstanceOf(IOException.class);
    }

    private Callable getRunnableProxyWithCheckedException(
            Exception ex,
            LeaderElectionService.StillLeadingStatus status1,
            LeaderElectionService.StillLeadingStatus status2) throws InterruptedException {

        Callable runnableWithInterruptedException = () -> {
            throw ex;
        };

        Supplier<Callable> delegateSupplier = Suppliers.ofInstance(
                runnableWithInterruptedException);
        LeaderElectionService mockLeaderService = mock(LeaderElectionService.class);
        setUpTheLeaderElectionService(mockLeaderService, status1, status2);

        Callable proxy = AwaitingLeadershipProxy.newProxyInstance(
                Callable.class, delegateSupplier, mockLeaderService);

        //waiting for trytoGainLeadership
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
        return proxy;
    }

    private void setUpTheLeaderElectionService(
            LeaderElectionService mockLeaderService,
            LeaderElectionService.StillLeadingStatus status1,
            LeaderElectionService.StillLeadingStatus status2)
            throws InterruptedException {
        LeaderElectionService.LeadershipToken leadershipToken = mock(PaxosLeadershipToken.class);
        when(mockLeaderService.blockOnBecomingLeader()).thenReturn(leadershipToken);
        when(mockLeaderService.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeaderService.isStillLeading(leadershipToken)).thenReturn(status1, status2);
    }
}
