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
    private final AwaitingLeadership awaitingLeadership = AwaitingLeadership.create(leaderElectionService);

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
        Runnable proxy = ServiceProxy.newProxyInstance(getAwaitingLeadership(), Runnable.class, delegateSupplier);

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy.equals(proxy)).isTrue();
        assertThat(proxy.equals(null)).isFalse();
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.ServiceProxy@");
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

    @Test
    public void listenableFutureMethodsDoNotBlockWhenNotLeading() throws ExecutionException, InterruptedException {
        ReturnsListenableFutureImpl listenableFuture = new ReturnsListenableFutureImpl();
        ReturnsListenableFuture proxy = ServiceProxy.newProxyInstance(
                getAwaitingLeadership(), ReturnsListenableFuture.class, () -> listenableFuture);
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
        ReturnsListenableFuture proxy = ServiceProxy.newProxyInstance(
                getAwaitingLeadership(), ReturnsListenableFuture.class, () -> listenableFuture);
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
        ReturnsListenableFuture proxy = ServiceProxy.newProxyInstance(
                getAwaitingLeadership(), ReturnsListenableFuture.class, () -> listenableFuture);
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

        Runnable proxy = ServiceProxy.newProxyInstance(getAwaitingLeadership(), Runnable.class, delegateSupplier);

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy.equals(proxy)).isTrue();
        assertThat(proxy.equals(null)).isFalse();
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.ServiceProxy@");
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

        Runnable proxy = ServiceProxy.newProxyInstance(getAwaitingLeadership(), Runnable.class, delegateSupplier);

        Thread.sleep(1000); // wait for retrying on gaining leadership

        proxy.run();
        verify(leaderElectionService, atLeast(2)).blockOnBecomingLeader();
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

    private Callable proxyFor(Callable fn) {
        return ServiceProxy.newProxyInstance(getAwaitingLeadership(), Callable.class, () -> fn);
    }

    private AwaitingLeadership getAwaitingLeadership() {
        return awaitingLeadership;
    }

    private void waitForLeadershipToBeGained() throws InterruptedException {
        verify(leaderElectionService, timeout(5_000)).blockOnBecomingLeader();
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));
    }
}
