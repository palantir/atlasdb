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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableException;
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.PaxosLeadershipToken;
import com.palantir.leader.SuspectedNotCurrentLeaderException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AwaitingLeadershipProxyTest {
    private static final String TEST_MESSAGE = "test_message";

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final LeadershipToken leadershipToken = mock(PaxosLeadershipToken.class);
    private final LeaderElectionService leaderElectionService = mock(LeaderElectionService.class);
    private final Runnable mockRunnable = mock(Runnable.class);
    private final Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);

    @BeforeEach
    public void before() throws InterruptedException {
        when(leaderElectionService.blockOnBecomingLeader()).thenReturn(leadershipToken);
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.empty());
        when(leaderElectionService.isStillLeading(leadershipToken))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.LEADING));
    }

    @AfterEach
    void after() {
        executor.shutdownNow();
    }

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect
    // the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenLeading() {
        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(
                Runnable.class, delegateSupplier, LeadershipCoordinator.create(leaderElectionService));

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy).isEqualTo(proxy);
        assertThat(proxy).isNotEqualTo(null);
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    public void listenableFutureMethodsDoNotBlockWhenNotLeading() throws InterruptedException {
        ReturnsListenableFutureImpl listenableFuture = new ReturnsListenableFutureImpl();
        ReturnsListenableFuture proxy = AwaitingLeadershipProxy.newProxyInstance(
                ReturnsListenableFuture.class,
                () -> listenableFuture,
                LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        SettableFuture<StillLeadingStatus> inProgressCheck = SettableFuture.create();
        when(leaderElectionService.isStillLeading(any(LeadershipToken.class))).thenReturn(inProgressCheck);

        ListenableFuture<?> future = proxy.future();
        assertThat(future).isNotDone();
        inProgressCheck.set(StillLeadingStatus.NOT_LEADING);

        assertThatThrownBy(future::get).cause().satisfies(exc -> assertThatLoggableException(
                        (Throwable & SafeLoggable) exc)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasLogMessage("method invoked on a non-leader (leadership lost)"));
    }

    @Test
    public void listenableFutureMethodsDoNotBlockWhenLeading() throws InterruptedException, ExecutionException {
        ReturnsListenableFutureImpl listenableFuture = new ReturnsListenableFutureImpl();
        ReturnsListenableFuture proxy = AwaitingLeadershipProxy.newProxyInstance(
                ReturnsListenableFuture.class,
                () -> listenableFuture,
                LeadershipCoordinator.create(leaderElectionService));
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
                ReturnsListenableFuture.class,
                () -> listenableFuture,
                LeadershipCoordinator.create(leaderElectionService));
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

        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(
                Runnable.class, delegateSupplier, LeadershipCoordinator.create(leaderElectionService));

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy).isEqualTo(proxy);
        assertThat(proxy).isNotEqualTo(null);
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    public void shouldThrowIfNotLeading() {
        Callable<Void> delegate = () -> {
            throw new ServiceNotAvailableException(new SafeRuntimeException(TEST_MESSAGE));
        };

        assertThatThrownBy(() -> callOnNonLeader(delegate))
                .isInstanceOf(ServiceNotAvailableException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)")
                .rootCause()
                .isInstanceOf(SafeRuntimeException.class)
                .hasMessageContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldThrowIfDelegateNotLeading() {
        Callable<Void> delegate = () -> {
            throw new NotCurrentLeaderException("not current leader", new SafeRuntimeException(TEST_MESSAGE));
        };

        assertThatThrownBy(() -> callOnNonLeader(delegate))
                .isInstanceOf(ServiceNotAvailableException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)")
                .rootCause()
                .isInstanceOf(SafeRuntimeException.class)
                .hasMessageContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldThrowIfNoQuorum() {
        Callable<Void> delegate = () -> {
            throw new ServiceNotAvailableException(new SafeRuntimeException(TEST_MESSAGE));
        };

        assertThatThrownBy(() -> {
                    CountDownLatch delegateCallStarted = new CountDownLatch(1);
                    CountDownLatch leadershipLost = new CountDownLatch(1);

                    Callable<Void> proxy = proxyFor(() -> {
                        delegateCallStarted.countDown();
                        leadershipLost.await();

                        return delegate.call();
                    });

                    Future<Void> blockingCall = executor.submit(proxy);
                    executor.shutdown();
                    delegateCallStarted.await();

                    loseLeadershipOnLeaderElectionService(StillLeadingStatus.NO_QUORUM);
                    leadershipLost.countDown();

                    try {
                        blockingCall.get();
                        fail("call should have failed due to lost leadership");
                    } catch (Throwable e) {
                        throw e.getCause();
                    }
                })
                .isInstanceOf(ServiceNotAvailableException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)")
                .rootCause()
                .isInstanceOf(SafeRuntimeException.class)
                .hasMessageContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldMapInterruptedExceptionToNcleIfLeadingStatusChanges() {
        Callable<Void> delegate = () -> {
            throw new InterruptedException(TEST_MESSAGE);
        };

        assertThatLoggableExceptionThrownBy(() -> loseLeadershipDuringCallToProxyFor(delegate))
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasLogMessage("received an interrupt due to leader election.")
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

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        assertThatThrownBy(proxy::call).isInstanceOf(InterruptedException.class).hasMessage(TEST_MESSAGE);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    public void shouldPropagateError() throws InterruptedException {
        Callable<Void> proxy = proxyFor(() -> {
            throw new LinkageError(TEST_MESSAGE);
        });
        waitForLeadershipToBeGained();

        assertThatThrownBy(proxy::call).isInstanceOf(LinkageError.class).hasMessage(TEST_MESSAGE);
    }

    @Test
    public void shouldGainLeadershipImmediatelyIfAlreadyLeading() throws Exception {
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.of(leadershipToken));

        Callable<Void> proxy = proxyFor(() -> null);

        proxy.call();

        verify(leaderElectionService, never()).blockOnBecomingLeader();
    }

    @Test
    public void shouldBlockOnGainingLeadershipIfNotCurrentlyLeading() throws Exception {
        Callable<Void> proxy = proxyFor(() -> null);
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

        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(
                Runnable.class, delegateSupplier, LeadershipCoordinator.create(leaderElectionService));

        Thread.sleep(1000); // wait for retrying on gaining leadership

        proxy.run();
        verify(leaderElectionService, atLeast(2)).blockOnBecomingLeader();
    }

    @Test
    public void shouldClearDelegateUponLosingLeadership() throws Exception {
        MyCloseable mock = mock(MyCloseable.class);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock, LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        proxy.val();
        Callable<Void> callable = () -> {
            proxy.val();
            return null;
        };
        loseLeadership(callable);
        assertThatLoggableExceptionThrownBy(callable::call)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasLogMessage("method invoked on a non-leader");
        verify(mock).close();
    }

    @Test
    public void shouldClearDelegateIfLeadershipLossIsRealizedByAnotherProxy() throws Exception {
        MyCloseable mockA = mock(MyCloseable.class);
        LeadershipCoordinator leadershipCoordinator = LeadershipCoordinator.create(leaderElectionService);
        MyCloseable proxyA =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, () -> mockA, leadershipCoordinator);

        MyCloseable proxyB = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock(MyCloseable.class), leadershipCoordinator);

        waitForLeadershipToBeGained();

        proxyA.val();
        proxyB.val();

        loseLeadership(() -> {
            proxyB.val();
            return null;
        });

        assertThatLoggableExceptionThrownBy(proxyA::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasLogMessage("method invoked on a non-leader");
        verify(mockA).close();
    }

    @Test
    public void shouldClearDelegateIfLeadershipTokenIsRefreshed() throws Exception {
        MyCloseable mock = mock(MyCloseable.class);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock, LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        proxy.val();
        refreshLeadershipToken(proxy);

        proxy.val();
        verify(mock).close();
    }

    @Test
    public void shouldClearDelegateIfLeadershipTokenIsRefreshedByAnotherProxy() throws Exception {
        MyCloseable mockA = mock(MyCloseable.class);
        LeadershipCoordinator leadershipCoordinator = LeadershipCoordinator.create(leaderElectionService);
        MyCloseable proxyA =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, () -> mockA, leadershipCoordinator);

        MyCloseable proxyB = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock(MyCloseable.class), leadershipCoordinator);

        waitForLeadershipToBeGained();

        proxyA.val();
        proxyB.val();

        refreshLeadershipToken(proxyB);

        proxyA.val();
        verify(mockA).close();
    }

    @Test
    public void clearsOnlyRelevantDelegateOnSuspectedNotCurrentLeaderFalseAlarm()
            throws IOException, InterruptedException {
        MyCloseable mock = mock(MyCloseable.class);
        doThrow(new SuspectedNotCurrentLeaderException("There is one imposter among us"))
                .doNothing()
                .when(mock)
                .val();
        Supplier<MyCloseable> factory = createSpyableSupplier(mock);
        LeadershipCoordinator coordinator = LeadershipCoordinator.create(leaderElectionService);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, factory, coordinator);

        MyCloseable bystander = mock(MyCloseable.class);
        Supplier<MyCloseable> bystanderFactory = createSpyableSupplier(bystander);
        MyCloseable bystanderProxy =
                AwaitingLeadershipProxy.newProxyInstance(MyCloseable.class, bystanderFactory, coordinator);

        waitForLeadershipToBeGained();

        assertThatCode(bystanderProxy::val).as("leadership could be gained").doesNotThrowAnyException();

        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(SuspectedNotCurrentLeaderException.class)
                .hasMessage("There is one imposter among us");

        assertThatCode(proxy::val)
                .as("the underlying mock can be called again, if we actually maintained leadership")
                .doesNotThrowAnyException();

        assertThatCode(() -> {
                    proxy.val();
                    bystanderProxy.val();
                    proxy.val();
                    bystanderProxy.val();
                    proxy.val();
                })
                .as("the proxies are still able to forward requests")
                .doesNotThrowAnyException();

        // We create once at the beginning, before we discover we will get a SuspectedNotCurrentLeaderException,
        // and another time after we checked that we were still leading.
        verify(factory, times(2)).get();
        verify(bystanderFactory).get();
        verify(mock).close();
        verify(bystander, never()).close();
    }

    // TODO (jkong): Collapse this test and the next into a single Parameterised one when we move to JUnit 5.
    @Test
    public void shouldLoseLeadershipOnSuspectedNotCurrentLeaderWhenLeadershipGenuinelyLost()
            throws InterruptedException, IOException {
        MyCloseable mock = mock(MyCloseable.class);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock, LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        doAnswer(_invocation -> {
                    loseLeadershipOnLeaderElectionService(StillLeadingStatus.NOT_LEADING);
                    throw new SuspectedNotCurrentLeaderException("There is one imposter among us");
                })
                .when(mock)
                .val();

        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)");

        // Another call is required before we clear existing resources.
        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader");
        verify(mock).close();
    }

    @Test
    public void shouldLoseLeadershipOnSuspectedNotCurrentLeaderWhenNoQuorum() throws InterruptedException, IOException {
        MyCloseable mock = mock(MyCloseable.class);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock, LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        doAnswer(_invocation -> {
                    loseLeadershipOnLeaderElectionService(StillLeadingStatus.NO_QUORUM);
                    throw new SuspectedNotCurrentLeaderException("There is one imposter among us");
                })
                .when(mock)
                .val();

        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)");

        // Another call is required before we clear existing resources.
        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader");
        verify(mock).close();
    }

    // TODO (jkong): Collapse this test and the next into a single Parameterised one when we move to JUnit 5.
    @Test
    public void shouldLoseLeadershipOnSuspectedNotCurrentLeaderWhenFailingToCheckLeadershipState()
            throws InterruptedException, IOException {
        MyCloseable mock = mock(MyCloseable.class);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock, LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        doAnswer(_invocation -> {
                    throw new SuspectedNotCurrentLeaderException("There is one imposter among us");
                })
                .when(mock)
                .val();

        RuntimeException underlyingError = new RuntimeException("Something went wrong");
        // Need to eventually return a status, so we can check that we did lose leadership, and clear resources.
        when(leaderElectionService.isStillLeading(any()))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.LEADING))
                .thenReturn(Futures.immediateFailedFuture(underlyingError))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.NOT_LEADING));

        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)")
                .cause()
                .isInstanceOf(SuspectedNotCurrentLeaderException.class)
                .hasMessage("There is one imposter among us")
                .hasSuppressedException(underlyingError);

        // Another call is required before we clear existing resources.
        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader");

        verify(mock).close();
    }

    @Test
    public void shouldLoseLeadershipOnSuspectedNotCurrentLeaderWhenLeadershipStateCheckCancelled()
            throws InterruptedException, IOException {
        MyCloseable mock = mock(MyCloseable.class);
        MyCloseable proxy = AwaitingLeadershipProxy.newProxyInstance(
                MyCloseable.class, () -> mock, LeadershipCoordinator.create(leaderElectionService));
        waitForLeadershipToBeGained();

        doAnswer(_invocation -> {
                    throw new SuspectedNotCurrentLeaderException("There is one imposter among us");
                })
                .when(mock)
                .val();

        RuntimeException underlyingError = new CancellationException("I tried so hard, and got so far");
        // Need to eventually return a status, so we can check that we did lose leadership, and clear resources.
        when(leaderElectionService.isStillLeading(any()))
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.LEADING))
                .thenThrow(underlyingError)
                .thenReturn(Futures.immediateFuture(StillLeadingStatus.NOT_LEADING));

        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader (leadership lost)")
                .cause()
                .isInstanceOf(SuspectedNotCurrentLeaderException.class)
                .hasMessage("There is one imposter among us")
                .hasSuppressedException(underlyingError);

        // Another call is required before we clear existing resources.
        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("method invoked on a non-leader");

        verify(mock).close();
    }

    private static Supplier<MyCloseable> createSpyableSupplier(MyCloseable mock) {
        // Mockito is not able to spy lambdas, so this anonymous class must remain as such.
        //noinspection Convert2Lambda
        return spy(new Supplier<>() {
            @Override
            public MyCloseable get() {
                return mock;
            }
        });
    }

    @SuppressWarnings("IllegalThrows")
    private void loseLeadershipDuringCallToProxyFor(Callable<Void> delegate) throws Throwable {
        CountDownLatch delegateCallStarted = new CountDownLatch(1);
        CountDownLatch leadershipLost = new CountDownLatch(1);

        Callable<Void> proxy = proxyFor(() -> {
            delegateCallStarted.countDown();
            leadershipLost.await();

            return delegate.call();
        });

        waitForLeadershipToBeGained();

        Future<Void> blockingCall = executor.submit(proxy);
        executor.shutdown();
        delegateCallStarted.await();

        loseLeadership(proxy);
        leadershipLost.countDown();

        try {
            blockingCall.get();
            fail("call should have failed due to lost leadership");
        } catch (Throwable e) {
            throw e.getCause();
        }
    }

    private void callOnNonLeader(Callable<Void> delegate) throws Throwable {
        CountDownLatch delegateCallStarted = new CountDownLatch(1);
        CountDownLatch leadershipLost = new CountDownLatch(1);

        Callable<Void> proxy = proxyFor(() -> {
            delegateCallStarted.countDown();
            leadershipLost.await();

            return delegate.call();
        });

        waitForLeadershipToBeGained();

        Future<Void> blockingCall = executor.submit(proxy);
        executor.shutdown();
        delegateCallStarted.await();

        loseLeadership(proxy);
        leadershipLost.countDown();

        try {
            blockingCall.get();
            fail("call should have failed due to lost leadership");
        } catch (Throwable e) {
            throw e.getCause();
        }
    }

    private void loseLeadership(Callable<?> proxy) throws InterruptedException {
        loseLeadershipOnLeaderElectionService(StillLeadingStatus.NOT_LEADING);

        // make a call so the proxy will realize that it has lost leadership
        assertThatLoggableExceptionThrownBy(proxy::call)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasLogMessage("method invoked on a non-leader (leadership lost)");
    }

    private void loseLeadershipOnLeaderElectionService(StillLeadingStatus leadingStatus) throws InterruptedException {
        Preconditions.checkState(
                leadingStatus != StillLeadingStatus.LEADING,
                "Cannot lose leadership and think that we are still leading");

        when(leaderElectionService.isStillLeading(any())).thenReturn(Futures.immediateFuture(leadingStatus));
        when(leaderElectionService.blockOnBecomingLeader()).thenAnswer(invocation -> {
            // never return
            LockSupport.park();
            return null;
        });
    }

    private void refreshLeadershipToken(MyCloseable proxy) throws InterruptedException {
        LeadershipToken newLeadershipToken = mock(LeadershipToken.class);
        when(leaderElectionService.isStillLeading(any())).thenAnswer(invocation -> {
            Object arg = invocation.getArgument(0);
            if (arg.equals(newLeadershipToken)) {
                return Futures.immediateFuture(StillLeadingStatus.LEADING);
            } else {
                return Futures.immediateFuture(StillLeadingStatus.NOT_LEADING);
            }
        });
        when(leaderElectionService.blockOnBecomingLeader()).thenAnswer(invocation -> newLeadershipToken);

        // make a call so the proxy will realize that it has lost leadership
        assertThatLoggableExceptionThrownBy(proxy::val)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasLogMessage("method invoked on a non-leader (leadership lost)")
                .hasExactlyArgs(SafeArg.of("serviceHint", Optional.empty()));

        // Wait to gain leadership
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));
    }

    @SuppressWarnings("unchecked")
    private <T> Callable<T> proxyFor(Callable<T> fn) {
        return (Callable<T>) AwaitingLeadershipProxy.newProxyInstance(
                Callable.class, () -> fn, LeadershipCoordinator.create(leaderElectionService));
    }

    private void waitForLeadershipToBeGained() throws InterruptedException {
        verify(leaderElectionService, timeout(5_000)).blockOnBecomingLeader();
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100L));
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
}
