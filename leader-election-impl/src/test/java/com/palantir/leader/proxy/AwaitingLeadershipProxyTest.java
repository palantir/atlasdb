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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.PaxosLeadershipToken;

public class AwaitingLeadershipProxyTest {
    private static final String TEST_MESSAGE = "test_message";

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final LeaderElectionService.LeadershipToken leadershipToken = mock(PaxosLeadershipToken.class);
    private final LeaderElectionService leaderElectionService = mock(LeaderElectionService.class);

    @Before
    public void before() throws InterruptedException {
        when(leaderElectionService.blockOnBecomingLeader()).thenReturn(leadershipToken);
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.empty());
        when(leaderElectionService.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(leaderElectionService.isStillLeading(leadershipToken)).thenReturn(
                LeaderElectionService.StillLeadingStatus.LEADING);
    }

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenLeading() throws Exception {
        Runnable mockRunnable = mock(Runnable.class);
        Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);
        LeaderElectionService mockLeader = mock(LeaderElectionService.class);

        when(mockLeader.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeader.getCurrentTokenIfLeading()).thenReturn(Optional.empty());
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
        when(mockLeader.getCurrentTokenIfLeading()).thenReturn(Optional.empty());
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
        Callable<Void> delegate = () -> {
            throw new InterruptedException(TEST_MESSAGE);
        };

        assertThatThrownBy(() -> loseLeadershipDuringCallToProxyFor(delegate))
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("received an interrupt due to leader election.")
                .hasCauseExactlyInstanceOf(InterruptedException.class)
                .hasStackTraceContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapOtherExceptionToNCLEIfLeadingStatusChanges()  {
        Callable<Void> delegate = () -> {
            throw new RuntimeException(TEST_MESSAGE);
        };

        assertThatThrownBy(() -> loseLeadershipDuringCallToProxyFor(delegate))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapInterruptedExceptionToNCLEIfLeadingStatusDoesNotChange() throws InterruptedException {
        Callable<Void> proxy = proxyFor(() -> {
            throw new InterruptedException(TEST_MESSAGE);
        });
        waitForLeadershipToBeGained();

        assertThatThrownBy(() -> proxy.call())
                .isInstanceOf(InterruptedException.class)
                .hasMessage(TEST_MESSAGE);
    }

    @Test
    public void shouldGainLeadershipImmediatelyIfAlreadyLeading() throws Exception {
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.of(leadershipToken));

        Callable proxy = proxyFor(() -> null);

        proxy.call();

        verify(leaderElectionService, never()).blockOnBecomingLeader();
    }

    @Test
    public void shouldBlockOnGainingLeadershipIfNotCurrentlyLeading() throws Exception {
        Callable proxy = proxyFor(() -> null);
        waitForLeadershipToBeGained();

        proxy.call();

        verify(leaderElectionService).getCurrentTokenIfLeading();
        verify(leaderElectionService).blockOnBecomingLeader();
    }

    private Void loseLeadershipDuringCallToProxyFor(Callable<Void> delegate) throws Throwable {
        CountDownLatch delegateCallStarted = new CountDownLatch(1);
        CountDownLatch leadershipLost = new CountDownLatch(1);

        Callable<Void> proxy = proxyFor(() -> {
            delegateCallStarted.countDown();
            leadershipLost.await();

            return delegate.call();
        });

        waitForLeadershipToBeGained();

        Future<Void> blockingCall = executor.submit(proxy);
        delegateCallStarted.await();

        loseLeadership(proxy);
        leadershipLost.countDown();

        try {
            return blockingCall.get();
        } catch (Throwable e) {
            throw e.getCause();
        }
    }

    private void loseLeadership(Callable proxy) throws InterruptedException {
        when(leaderElectionService.isStillLeading(any()))
                .thenReturn(LeaderElectionService.StillLeadingStatus.NOT_LEADING);
        when(leaderElectionService.blockOnBecomingLeader()).then(invocation -> {
            // never return
            LockSupport.park();
            return null;
        });

        // make a call so the proxy will realize that it has lost leadership
        assertThatThrownBy(proxy::call).isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader (leadership lost)");
    }

    private Callable proxyFor(Callable fn) throws InterruptedException {
        return AwaitingLeadershipProxy.newProxyInstance(Callable.class, () -> fn, leaderElectionService);
    }

    private void waitForLeadershipToBeGained() throws InterruptedException {
        verify(leaderElectionService, timeout(5_000)).blockOnBecomingLeader();
        Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
    }

    private LeaderElectionService mockLeaderElectionServiceWithSequence(
            LeaderElectionService.StillLeadingStatus status1,
            LeaderElectionService.StillLeadingStatus status2)
            throws InterruptedException {
        LeaderElectionService mockLeaderService = mock(LeaderElectionService.class);
        LeaderElectionService.LeadershipToken leadershipToken = mock(PaxosLeadershipToken.class);
        when(mockLeaderService.blockOnBecomingLeader()).thenReturn(
                getToken(status1, leadershipToken),
                getToken(status2, leadershipToken));
        when(mockLeaderService.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeaderService.isStillLeading(leadershipToken)).thenReturn(status1, status2);
        return mockLeaderService;
    }

    private LeaderElectionService.LeadershipToken getToken(LeaderElectionService.StillLeadingStatus status,
            LeaderElectionService.LeadershipToken leadershipToken) {
        return (status == LeaderElectionService.StillLeadingStatus.LEADING) ? leadershipToken : null;
    }
}
