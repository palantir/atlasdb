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
import com.palantir.leader.proxy.LeadershipStateManager.LeadershipState;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Tracers;
import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
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
        return interfaceClass.cast(Proxy.newProxyInstance(
                interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass, Closeable.class}, proxy));
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

        // The state must NEVER be cached, each request must fetch the latest state from leadershipStateManager
        LeadershipState<T> leadershipState = leadershipStateManager.getLeadershipState();
        LeadershipToken leadershipToken = leadershipState.leadershipToken();

        ListenableFuture<StillLeadingStatus> leadingFuture = Tracers.wrapListenableFuture(
                "validate-leadership",
                () -> statusRetrier.execute(() -> Tracers.wrapListenableFuture(
                        "validate-leadership-attempt", () -> leadershipCoordinator.isStillLeading(leadershipToken))));

        ListenableFuture<T> delegateFuture = Futures.transformAsync(
                leadingFuture, leading -> delegate(leadershipState, leading), MoreExecutors.directExecutor());

        if (ListenableFuture.class.equals(method.getReturnType())) {
            return executeOnDelegateAsync(method, args, leadershipToken, delegateFuture);
        } else {
            return executeOnDelegate(method, args, leadershipToken, delegateFuture);
        }
    }

    private Object executeOnDelegate(
            Method method, Object[] args, LeadershipToken leadershipToken, ListenableFuture<T> delegateFuture)
            throws Exception {
        T delegate = AtlasFutures.getUnchecked(delegateFuture);
        try (CloseableTracer ignored = CloseableTracer.startSpan("execute-on-delegate")) {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw handleDelegateThrewException(leadershipToken, causeFrom(e));
        }
    }

    private FluentFuture<Object> executeOnDelegateAsync(
            Method method, Object[] args, LeadershipToken leadershipToken, ListenableFuture<T> delegateFuture) {
        Preconditions.checkArgument(
                ListenableFuture.class.equals(method.getReturnType()), "Async delegate must return ListenableFuture");
        return FluentFuture.from(delegateFuture)
                .transformAsync(
                        delegate -> Tracers.wrapListenableFuture("execute-on-delegate-async", () -> {
                            try {
                                return (ListenableFuture<Object>) method.invoke(delegate, args);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                return Futures.immediateFailedFuture(causeFrom(e));
                            }
                        }),
                        executionExecutor)
                .catchingAsync(
                        Throwable.class,
                        t -> {
                            throw handleDelegateThrewException(leadershipToken, causeFrom(t));
                        },
                        executionExecutor);
    }

    private ListenableFuture<T> delegate(LeadershipState<T> leadershipState, StillLeadingStatus leading) {
        // treat a repeated NO_QUORUM as NOT_LEADING; likely we've been cut off from the other nodes
        // and should assume we're not the leader
        if (leading == StillLeadingStatus.NOT_LEADING || leading == StillLeadingStatus.NO_QUORUM) {
            throw leadershipStateManager.handleNotLeading(leadershipState.leadershipToken(), null /* cause */);
        } else if (isClosed) {
            throw new SafeIllegalStateException("Already closed proxy", SafeArg.of("interfaceClass", interfaceClass));
        }
        return Futures.immediateFuture(Preconditions.checkNotNull(
                leadershipState.delegate(),
                "no valid delegate present",
                SafeArg.of("interfaceClass", interfaceClass.getName())));
    }

    private SafeRuntimeException handleDelegateThrewException(LeadershipToken leadershipToken, Throwable throwable)
            throws Exception {
        Throwable cause = causeFrom(throwable);
        if (cause instanceof ServiceNotAvailableException) {
            throw leadershipStateManager.handleNotLeading(leadershipToken, cause);
        }
        // Prevent blocked lock requests from receiving a non-retryable 500 on interrupts
        // in case of a leader election.
        if (cause instanceof InterruptedException && !leadershipCoordinator.isStillCurrentToken(leadershipToken)) {
            throw leadershipCoordinator.notCurrentLeaderException(
                    "received an interrupt due to leader election.", cause);
        }
        Throwables.propagateIfPossible(cause, Exception.class);
        throw new SafeRuntimeException("Delegate threw", throwable);
    }

    private static Throwable causeFrom(Throwable throwable) {
        if (throwable instanceof InvocationTargetException) {
            return throwable.getCause();
        }
        return throwable;
    }
}
