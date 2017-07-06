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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch leadershipToBeLost = new CountDownLatch(1);
        CountDownLatch leadershipChecked = new CountDownLatch(1);

        LeaderElectionService leaderElectionService = setUpTheLeaderElectionService(
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.NOT_LEADING);
        Callable proxy = proxyFor(() -> () -> {
            leadershipChecked.countDown();
            leadershipToBeLost.await();
            throw new InterruptedException(TEST_MESSAGE);
        }, leaderElectionService);


        Future<?> blockingCall = executor.submit(() -> proxy.call());

        leadershipChecked.await();
        loseLeadership(proxy);
        leadershipToBeLost.countDown();

        assertThat(catchThrowable(blockingCall::get).getCause())
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("received an interrupt due to leader election.")
                .hasCauseExactlyInstanceOf(InterruptedException.class)
                .hasStackTraceContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapOtherExceptionToNCLEIfLeadingStatusChanges() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch leadershipToBeLost = new CountDownLatch(1);
        CountDownLatch leadershipChecked = new CountDownLatch(1);

        LeaderElectionService leaderElectionService = setUpTheLeaderElectionService(
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.NOT_LEADING);
        Callable proxy = proxyFor(() -> () -> {
            leadershipChecked.countDown();
            leadershipToBeLost.await();
            throw new IOException(TEST_MESSAGE);
        }, leaderElectionService);

        Future<?> blockingCall = executor.submit(() -> proxy.call());
        leadershipChecked.await();
        loseLeadership(proxy);
        leadershipToBeLost.countDown();

        assertThat(catchThrowable(blockingCall::get).getCause())
                .isInstanceOf(IOException.class)
                .hasMessage(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapInterruptedExceptionToNCLEIfLeadingStatusDoesNotChange() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch leadershipChecked = new CountDownLatch(1);

        LeaderElectionService leaderElectionService = setUpTheLeaderElectionService(
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.LEADING);
        Callable proxy = proxyFor(() -> () -> {
            leadershipChecked.await();
            throw new InterruptedException(TEST_MESSAGE);
        }, leaderElectionService);


        Future<?> blockingCall = executor.submit(() -> proxy.call());

        leadershipChecked.countDown();

        assertThat(catchThrowable(blockingCall::get).getCause())
                .isInstanceOf(InterruptedException.class)
                .hasMessage(TEST_MESSAGE);
    }


    private void loseLeadership(Callable proxy) {
        assertThatThrownBy(proxy::call).isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader (leadership lost)");
    }

    private Callable proxyFor(Supplier<Callable> fn,
            LeaderElectionService leaderElectionService) throws InterruptedException {
        Callable proxy = AwaitingLeadershipProxy.newProxyInstance(Callable.class, fn, leaderElectionService);

        //waiting for trytoGainLeadership
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        return proxy;
    }

    private LeaderElectionService setUpTheLeaderElectionService(
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
