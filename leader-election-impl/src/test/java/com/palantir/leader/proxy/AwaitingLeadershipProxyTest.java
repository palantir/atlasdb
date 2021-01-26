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
package com.palantir.leader.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.core.Is.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.PaxosLeadershipToken;
import com.palantir.tracing.RenderTracingRule;
import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AwaitingLeadershipProxyTest {
    private static final String TEST_MESSAGE = "test_message";

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final LeadershipToken leadershipToken = mock(PaxosLeadershipToken.class);
    private final LeaderElectionService leaderElectionService = mock(LeaderElectionService.class);
    private final Runnable mockRunnable = mock(Runnable.class);
    private final Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);

    @Rule
    public final ExpectedException expect = ExpectedException.none();

    @Rule
    public final RenderTracingRule rule = new RenderTracingRule();

    @Before
    public void before() throws InterruptedException {
        when(leaderElectionService.blockOnBecomingLeader()).thenReturn(leadershipToken);
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.empty());
        when(leaderElectionService.isStillLeading(leadershipToken))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.LEADING));
    }

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect
    // the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenLeading() {
        Runnable proxy =
                AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, getLeadershipCoordinator());

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy).isEqualTo(proxy);
        assertThat(proxy).isNotEqualTo(null);
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    private interface ReturnsListenableFuture {
        ListenableFuture<?> future();
    }

    private static final class ReturnsListenableFutureImpl implements ReturnsListenableFuture {
        private final SettableFuture<?> future = SettableFuture.create();

        @Override
        public ListenableFuture<?> future() {
            return future;
        }
    }

    private interface MyCloseable extends Closeable {
        void val();
    }

    private static class CloseableImpl implements MyCloseable {
        @Override
        public void val() {}

        @Override
        public void close() {}
    }

    @Test
    public void listenableFutureMethodsDoNotBlockWhenNotLeading() throws ExecutionException, InterruptedException {
        ReturnsListenableFutureImpl listenableFuture = new ReturnsListenableFutureImpl();
        ReturnsListenableFuture proxy = AwaitingLeadershipProxy.newProxyInstance(
                ReturnsListenableFuture.class, () -> listenableFuture, getLeadershipCoordinator());
        waitForLeadershipToBeGained();

        SettableFuture<StillLeadingStatus> inProgressCheck = SettableFuture.create();
        when(leaderElectionService.isStillLeading(any(LeadershipToken.class))).thenReturn(inProgressCheck);

        ListenableFuture<?> future = proxy.future();
        assertThat(future).isNotDone();
        inProgressCheck.set(StillLeadingStatus.NOT_LEADING);
        expect.expectCause(isA(NotCurrentLeaderException.class));
        future.get();
    }

    @Test
    public void listenableFutureMethodsDoNotBlockWhenLeading() throws InterruptedException, ExecutionException {
        ReturnsListenableFutureImpl listenableFuture = new ReturnsListenableFutureImpl();
        ReturnsListenableFuture proxy = AwaitingLeadershipProxy.newProxyInstance(
                ReturnsListenableFuture.class, () -> listenableFuture, getLeadershipCoordinator());
        waitForLeadershipToBeGained();

        SettableFuture<StillLeadingStatus> inProgressCheck = SettableFuture.create();
        when(leaderElectionService.isStillLeading(any(LeadershipToken.class))).thenReturn(inProgressCheck);

        ListenableFuture<?> future = proxy.future();
        assertThat(future).isNotDone();
        inProgressCheck.set(StillLeadingStatus.LEADING);
        assertThat(future).isNotDone();
        listenableFuture.future.set(null);
        future.get();
    }

    @Test
    public void listenableFutureMethodsRetryProxyFailures() throws InterruptedException, ExecutionException {
        ReturnsListenableFutureImpl listenableFuture = new ReturnsListenableFutureImpl();
        ReturnsListenableFuture proxy = AwaitingLeadershipProxy.newProxyInstance(
                ReturnsListenableFuture.class, () -> listenableFuture, getLeadershipCoordinator());
        waitForLeadershipToBeGained();

        SettableFuture<StillLeadingStatus> inProgressCheck = SettableFuture.create();
        when(leaderElectionService.isStillLeading(any(LeadershipToken.class)))
                .thenAnswer($ -> {
                    // Strange number to be detectable in traces
                    Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(37));
                    return Futures.immediateFuture(StillLeadingStatus.NO_QUORUM);
                })
                .thenAnswer($ -> {
                    // Strange number to be detectable in traces
                    Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(29));
                    return Futures.immediateFuture(StillLeadingStatus.NO_QUORUM);
                })
                .thenReturn(inProgressCheck);

        ListenableFuture<?> future = proxy.future();
        assertThat(future).isNotDone();
        inProgressCheck.set(StillLeadingStatus.LEADING);
        assertThat(future).isNotDone();
        listenableFuture.future.set(null);
        future.get();
    }

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect
    // the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenNotLeading() {
        when(leaderElectionService.isStillLeading(any(LeadershipToken.class)))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.NOT_LEADING));

        Runnable proxy =
                AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, getLeadershipCoordinator());

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy).isEqualTo(proxy);
        assertThat(proxy).isNotEqualTo(null);
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    public void shouldMapInterruptedExceptionToNcleIfLeadingStatusChanges() {
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
    public void shouldNotMapOtherExceptionToNcleIfLeadingStatusChanges() {
        Callable<Void> delegate = () -> {
            throw new RuntimeException(TEST_MESSAGE);
        };

        assertThatThrownBy(() -> loseLeadershipDuringCallToProxyFor(delegate))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapInterruptedExceptionToNcleIfLeadingStatusDoesNotChange() throws InterruptedException {
        Callable<Void> proxy = proxyFor(() -> {
            throw new InterruptedException(TEST_MESSAGE);
        });
        waitForLeadershipToBeGained();

        assertThatThrownBy(proxy::call).isInstanceOf(InterruptedException.class).hasMessage(TEST_MESSAGE);
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

    @Test
    public void shouldRetryBecomingLeader() throws Exception {
        when(leaderElectionService.blockOnBecomingLeader())
                .thenThrow(new RuntimeException())
                .thenReturn(leadershipToken);

        Runnable proxy =
                AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, getLeadershipCoordinator());

        Thread.sleep(1000); // wait for retrying on gaining leadership

        proxy.run();
        verify(leaderElectionService, atLeast(2)).blockOnBecomingLeader();
    }

    @Test
    public void shouldClearDelegateUponLosingLeadership() throws Exception {
        CloseableImpl mock = mock(CloseableImpl.class);
        MyCloseable proxy =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, () -> mock, getLeadershipCoordinator());
        waitForLeadershipToBeGained();

        proxy.val();
        Callable callable = () -> {
            proxy.val();
            return null;
        };
        loseLeadership(callable);
        assertThatThrownBy(callable::call)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader");
        verify(mock, times(1)).close();
    }

    @Test
    public void shouldClearDelegateIfLeadershipLossIsRealizedByAnotherProxy() throws Exception {
        CloseableImpl mockA = mock(CloseableImpl.class);
        LeadershipCoordinator leadershipCoordinator = getLeadershipCoordinator();
        MyCloseable proxyA =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, () -> mockA, leadershipCoordinator);

        MyCloseable proxyB = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock(CloseableImpl.class), leadershipCoordinator);

        // Wait to gain leadership
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));

        proxyA.val();
        proxyB.val();

        loseLeadership(() -> {
            proxyB.val();
            return null;
        });

        assertThatThrownBy(proxyA::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader");
        verify(mockA, times(1)).close();
    }

    @Test
    public void shouldClearDelegateIfLeadershipTokenIsRefreshed() throws Exception {
        CloseableImpl mock = mock(CloseableImpl.class);
        MyCloseable proxy =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, () -> mock, getLeadershipCoordinator());
        waitForLeadershipToBeGained();

        proxy.val();
        refreshLeadershipToken(() -> {
            proxy.val();
            return null;
        });

        proxy.val();
        verify(mock, times(1)).close();
    }

    @Test
    public void shouldClearDelegateIfLeadershipTokenIsRefreshedByAnotherProxy() throws Exception {
        CloseableImpl mockA = mock(CloseableImpl.class);
        LeadershipCoordinator leadershipCoordinator = getLeadershipCoordinator();
        MyCloseable proxyA =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, () -> mockA, leadershipCoordinator);

        MyCloseable proxyB = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock(CloseableImpl.class), leadershipCoordinator);

        // Wait to gain leadership
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));

        proxyA.val();
        proxyB.val();

        refreshLeadershipToken(() -> {
            proxyB.val();
            return null;
        });

        // Wait to gain leadership
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));

        proxyA.val();
        verify(mockA, times(1)).close();
    }

    @SuppressWarnings("IllegalThrows")
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
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.NOT_LEADING));
        when(leaderElectionService.blockOnBecomingLeader()).then(invocation -> {
            // never return
            LockSupport.park();
            return null;
        });

        // make a call so the proxy will realize that it has lost leadership
        assertThatThrownBy(proxy::call)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader (leadership lost)");
    }

    private void refreshLeadershipToken(Callable proxy) throws InterruptedException {
        LeadershipToken newLeadershipToken = mock(LeadershipToken.class);
        when(leaderElectionService.isStillLeading(leadershipToken))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.NOT_LEADING));
        when(leaderElectionService.blockOnBecomingLeader()).thenAnswer(invocation -> newLeadershipToken);

        // make a call so the proxy will realize that it has lost leadership
        assertThatThrownBy(proxy::call)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader (leadership lost)");

        when(leaderElectionService.isStillLeading(newLeadershipToken))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.LEADING));
    }

    private Callable proxyFor(Callable fn) {
        return AwaitingLeadershipProxy.newProxyInstance(Callable.class, () -> fn, getLeadershipCoordinator());
    }

    private LeadershipCoordinator getLeadershipCoordinator() {
        return LeadershipCoordinator.create(leaderElectionService);
    }

    private void waitForLeadershipToBeGained() throws InterruptedException {
        verify(leaderElectionService, timeout(5_000)).blockOnBecomingLeader();
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));
    }
}
