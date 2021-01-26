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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Tracers;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwaitingLeadershipProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadershipProxy.class);

    private static final int MAX_NO_QUORUM_RETRIES = 10;
    private static final ListeningScheduledExecutorService schedulingExecutor =
            MoreExecutors.listeningDecorator(PTExecutors.newScheduledThreadPoolExecutor(1));
    private static final ListeningExecutorService executionExecutor = MoreExecutors.listeningDecorator(
            PTExecutors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
    private static final AsyncRetrier<StillLeadingStatus> statusRetrier = new AsyncRetrier<>(
            MAX_NO_QUORUM_RETRIES,
            Duration.ofMillis(700),
            schedulingExecutor,
            executionExecutor,
            status -> status != StillLeadingStatus.NO_QUORUM);

    private final LeadershipCoordinator leadershipCoordinator;
    private final Supplier<T> delegateSupplier;
    private final AtomicReference<T> delegateRef;
    private final AtomicReference<LeadershipToken> maybeValidLeadershipTokenRef;
    private final Class<T> interfaceClass;
    private final CoalescingSupplier<LeadershipToken> leadershipTokenCoalescingSupplier;
    private volatile boolean isClosed;

    private AwaitingLeadershipProxy(
            LeadershipCoordinator leadershipCoordinator, Supplier<T> delegateSupplier, Class<T> interfaceClass) {
        this.leadershipCoordinator = leadershipCoordinator;
        this.delegateSupplier = delegateSupplier;
        this.delegateRef = new AtomicReference<>();
        this.maybeValidLeadershipTokenRef = new AtomicReference<>();
        this.interfaceClass = interfaceClass;
        this.leadershipTokenCoalescingSupplier = new CoalescingSupplier<>(this::getLeadershipToken);
        this.isClosed = false;
    }

    public static <U> U newProxyInstance(
            Class<U> interfaceClass, Supplier<U> delegateSupplier, LeadershipCoordinator awaitingLeadership) {
        AwaitingLeadershipProxy<U> proxy =
                new AwaitingLeadershipProxy<>(awaitingLeadership, delegateSupplier, interfaceClass);
        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass, Closeable.class}, proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("close") && args.length == 0) {
            log.debug("Closing leadership proxy");
            isClosed = true;
            clearDelegate();
            return null;
        }

        // This calls `getLeadershipToken` which can refresh the delegateRef and leadershipTokenRef if
        // maybeValidLeadershipTokenRef is not the current leader token.
        // Do not modify order, hide, or remove following two lines without considering impact on correctness.
        final LeadershipToken leadershipToken = leadershipTokenCoalescingSupplier.get();
        T maybeValidDelegate = delegateRef.get();

        ListenableFuture<StillLeadingStatus> leadingFuture = Tracers.wrapListenableFuture(
                "validate-leadership",
                () -> statusRetrier.execute(() -> Tracers.wrapListenableFuture(
                        "validate-leadership-attempt", () -> leadershipCoordinator.getStillLeading(leadershipToken))));

        ListenableFuture<T> delegateFuture = Futures.transformAsync(
                leadingFuture,
                leading -> {
                    // treat a repeated NO_QUORUM as NOT_LEADING; likely we've been cut off from the other nodes
                    // and should assume we're not the leader
                    if (leading == StillLeadingStatus.NOT_LEADING || leading == StillLeadingStatus.NO_QUORUM) {
                        return Futures.submitAsync(
                                () -> {
                                    handleNotLeading(leadershipToken, null /* cause */);
                                    throw new AssertionError("should not reach here");
                                },
                                executionExecutor);
                    }

                    if (isClosed) {
                        throw new IllegalStateException("already closed proxy for " + interfaceClass.getName());
                    }

                    Preconditions.checkNotNull(maybeValidDelegate, "%s backing is null", interfaceClass.getName());
                    return Futures.immediateFuture(maybeValidDelegate);
                },
                MoreExecutors.directExecutor());

        if (!method.getReturnType().equals(ListenableFuture.class)) {
            T delegate = AtlasFutures.getUnchecked(delegateFuture);
            try (CloseableTracer ignored = CloseableTracer.startSpan("execute-on-delegate")) {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw handleDelegateThrewException(leadershipToken, e);
            }
        } else {
            return FluentFuture.from(delegateFuture)
                    .transformAsync(
                            delegate -> Tracers.wrapListenableFuture("execute-on-delegate-async", () -> {
                                try {
                                    return (ListenableFuture<Object>) method.invoke(delegate, args);
                                } catch (IllegalAccessException | InvocationTargetException e) {
                                    return Futures.immediateFailedFuture(e);
                                }
                            }),
                            executionExecutor)
                    .catchingAsync(
                            InvocationTargetException.class,
                            e -> {
                                throw handleDelegateThrewException(leadershipToken, e);
                            },
                            executionExecutor);
        }
    }

    /**
     * Checks the local ref of leadership token. We need to refresh the delegateRef in case the token has
     * changed.
     * This code can be accessed by multiple threads. In order to save all of the requests from locking-unlocking
     * `this` while trying to update maybeValidLeadershipTokenRef, the invocations are batched using CoalescingSupplier.
     */
    private LeadershipToken getLeadershipToken() {
        if (!leadershipCoordinator.isStillCurrentToken(maybeValidLeadershipTokenRef.get())) {
            // we need to clear out existing resources if leadership token has been updated
            claimResourcesOnLeadershipUpdate();
            tryToUpdateLeadershipToken();
        }

        LeadershipToken leadershipToken = maybeValidLeadershipTokenRef.get();
        if (leadershipToken == null) {
            // reclaim delegate resources if leadership cannot be gained
            claimResourcesOnLeadershipUpdate();
            throw leadershipCoordinator.notCurrentLeaderException("method invoked on a non-leader");
        }
        return leadershipToken;
    }

    /**
     * This method refreshes the delegateRef which can be a very expensive operation. This should be executed exactly
     * once for one leadershipToken update.
     * @throws NotCurrentLeaderException if we do not have leadership anymore.
     */
    private synchronized void tryToUpdateLeadershipToken() {
        if (leadershipCoordinator.isStillCurrentToken(maybeValidLeadershipTokenRef.get())) {
            return;
        }
        LeadershipToken leadershipToken = leadershipCoordinator.getLeadershipToken();

        T delegate = null;
        try {
            delegate = delegateSupplier.get();
        } catch (Throwable t) {
            log.error("problem creating delegate", t);
        }

        if (delegate != null) {
            // Do not modify, hide, or remove this line without considering impact on correctness.
            delegateRef.set(delegate);
            if (isClosed) {
                clearDelegate();
            } else {
                maybeValidLeadershipTokenRef.set(leadershipToken);
                log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
            }
        }
    }

    private RuntimeException handleDelegateThrewException(
            LeadershipToken leadershipToken, InvocationTargetException exception) throws Exception {
        if (exception.getCause() instanceof ServiceNotAvailableException
                || exception.getCause() instanceof NotCurrentLeaderException) {
            handleNotLeading(leadershipToken, exception.getCause());
        }
        // Prevent blocked lock requests from receiving a non-retryable 500 on interrupts
        // in case of a leader election.
        if (exception.getCause() instanceof InterruptedException
                && !leadershipCoordinator.isStillCurrentToken(leadershipToken)) {
            throw leadershipCoordinator.notCurrentLeaderException(
                    "received an interrupt due to leader election.", exception.getCause());
        }
        Throwables.propagateIfPossible(exception.getCause(), Exception.class);
        throw new RuntimeException(exception.getCause());
    }

    private void clearDelegate() {
        Object delegate = delegateRef.getAndSet(null);
        if (delegate instanceof Closeable) {
            try {
                ((Closeable) delegate).close();
            } catch (IOException ex) {
                log.warn("problem closing delegate", ex);
            }
        }
    }

    private void claimResourcesOnLeadershipUpdate() {
        maybeValidLeadershipTokenRef.set(null);
        clearDelegate();
    }

    /**
     * Right now there is no way to release resources quickly. In the event of loss of leadership, we wait till the
     * LeadershipCoordinator has updated its state. Then, the next request can release the delegateRef in
     * `getLeadershipToken`.
     */
    private void handleNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        if (maybeValidLeadershipTokenRef.compareAndSet(leadershipToken, null)) {
            // We are not clearing delegateTokenRef here. This is fine as we are relying on `getLeadershipToken` to
            // claim the resources for the next request if this node loses leadership.
            // If this node gains leadership again (i.e. with a different leadership token),
            // `tryToUpdateLeadershipToken` guarantees that the delegate will be refreshed *before* we get a new
            // leadershipToken.
            // If we were to clearDelegateRef here or outside the CAS, we could race with
            // `tryToUpdateLeadershipToken` and end up clearing `delegateRef`.

            leadershipCoordinator.markAsNotLeading(leadershipToken, cause);
        }
        throw leadershipCoordinator.notCurrentLeaderException(
                "method invoked on a non-leader (leadership lost)", cause);
    }
}
