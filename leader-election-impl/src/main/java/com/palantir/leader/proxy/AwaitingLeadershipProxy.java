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

import com.google.common.base.Throwables;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.SuspectedNotCurrentLeaderException;
import com.palantir.leader.proxy.LeadershipStateManager.LeadershipState;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Tracers;
import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@SuppressWarnings("ProxyNonConstantType")
public final class AwaitingLeadershipProxy<T> extends AbstractInvocationHandler {
    private static final SafeLogger log = SafeLoggerFactory.get(AwaitingLeadershipProxy.class);

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
    private final Class<T> interfaceClass;

    private final LeadershipStateManager<T> leadershipStateManager;
    private volatile boolean isClosed;

    private AwaitingLeadershipProxy(
            LeadershipCoordinator leadershipCoordinator, Supplier<T> delegateSupplier, Class<T> interfaceClass) {
        Preconditions.checkNotNull(delegateSupplier, "Unable to create an AwaitingLeadershipProxy with no supplier");
        this.leadershipCoordinator = leadershipCoordinator;
        this.interfaceClass = interfaceClass;
        this.leadershipStateManager = new LeadershipStateManager<>(leadershipCoordinator, delegateSupplier);
        this.isClosed = false;
    }

    public static <U> U newProxyInstance(
            Class<U> interfaceClass, Supplier<U> delegateSupplier, LeadershipCoordinator awaitingLeadership) {
        AwaitingLeadershipProxy<U> proxy =
                new AwaitingLeadershipProxy<>(awaitingLeadership, delegateSupplier, interfaceClass);
        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass, Closeable.class}, proxy);
    }

    @SuppressWarnings("ThrowError") // Possible legacy API
    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("close") && args.length == 0) {
            log.debug("Closing leadership proxy");
            isClosed = true;
            leadershipStateManager.close();
            return null;
        }

        // The state must NEVER be cached, each request must fetch latest leadership state from the
        // leadershipStateManager
        LeadershipState<T> leadershipState = leadershipStateManager.getLeadershipState();

        final LeadershipToken leadershipToken = leadershipState.leadershipToken();
        T maybeValidDelegate = leadershipState.delegate();

        ListenableFuture<StillLeadingStatus> leadingFuture = Tracers.wrapListenableFuture(
                "validate-leadership",
                () -> statusRetrier.execute(() -> Tracers.wrapListenableFuture(
                        "validate-leadership-attempt", () -> leadershipCoordinator.isStillLeading(leadershipToken))));

        ListenableFuture<T> delegateFuture = Futures.transformAsync(
                leadingFuture,
                leading -> {
                    // treat a repeated NO_QUORUM as NOT_LEADING; likely we've been cut off from the other nodes
                    // and should assume we're not the leader
                    if (leading == StillLeadingStatus.NOT_LEADING || leading == StillLeadingStatus.NO_QUORUM) {
                        return Futures.immediateFailedFuture(leadershipStateManager.invalidateStateOnLostLeadership(
                                leadershipToken, null /* cause */));
                    }

                    if (isClosed) {
                        throw new IllegalStateException("already closed proxy for " + interfaceClass.getName());
                    }

                    Preconditions.checkNotNull(
                            maybeValidDelegate,
                            "no valid delegate present",
                            SafeArg.of("InterfaceClass", interfaceClass.getName()));
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

    private RuntimeException handleDelegateThrewException(
            LeadershipToken leadershipToken, InvocationTargetException exception) throws Exception {
        Throwable cause = exception.getCause();
        if (cause instanceof SuspectedNotCurrentLeaderException) {
            handleSuspectedNotCurrentLeader(leadershipToken, (SuspectedNotCurrentLeaderException) cause);
        } else if (cause instanceof ServiceNotAvailableException) { // implicitly covers NotCurrentLeaderException
            throw leadershipStateManager.invalidateStateOnLostLeadership(leadershipToken, cause);
        } else if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            if (!leadershipCoordinator.isStillCurrentToken(leadershipToken)) {
                // Prevent blocked lock requests from receiving a non-retryable 500 on interrupts
                // in case of a leader election.
                throw leadershipCoordinator.notCurrentLeaderException(
                        "received an interrupt due to leader election.", cause);
            }
        }
        Throwables.propagateIfPossible(cause, Exception.class);
        throw new RuntimeException(cause);
    }

    private void handleSuspectedNotCurrentLeader(
            LeadershipToken leadershipToken, SuspectedNotCurrentLeaderException cause) throws InterruptedException {
        StillLeadingStatus status = determineCurrentLeadershipStatus(leadershipToken, cause);

        switch (status) {
            case LEADING:
                // We were still the leader. Invalidate the relevant delegates, but don't generally get rid of our
                // leadership (so this doesn't need to propagate to other delegates).
                leadershipStateManager.requestDelegateInvalidation();
                break;
            case NO_QUORUM:
                // Treating as not leading is consistent with uses of ServiceNotAvailableException elsewhere.
            case NOT_LEADING:
                throw leadershipStateManager.invalidateStateOnLostLeadership(leadershipToken, cause);
            default:
                throw new SafeIllegalStateException("Unexpected StillLeadingStatus", SafeArg.of("status", status));
        }
    }

    // Chosen to match past behaviour, where interruptions are propagated to the caller.
    private StillLeadingStatus determineCurrentLeadershipStatus(
            LeadershipToken leadershipToken, SuspectedNotCurrentLeaderException cause) throws InterruptedException {
        try {
            ListenableFuture<StillLeadingStatus> stillLeading = leadershipCoordinator.isStillLeading(leadershipToken);
            return stillLeading.get();
        } catch (ExecutionException e) {
            log.info("Failed to check if we were still leading. Being defensive, and saying we are not leading.", e);
            cause.addSuppressed(e.getCause());
            return StillLeadingStatus.NOT_LEADING;
        } catch (CancellationException e) {
            log.info(
                    "Attempt to check if we were still leading was cancelled. Being defensive, and saying we are not"
                            + " leading.",
                    e);
            cause.addSuppressed(e);
            return StillLeadingStatus.NOT_LEADING;
        }
    }
}
