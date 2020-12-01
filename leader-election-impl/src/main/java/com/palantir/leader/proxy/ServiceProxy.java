/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
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

public class ServiceProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ServiceProxy.class);

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

    private final AwaitingLeadership awaitingLeadership;
    private final Supplier<T> delegateSupplier;
    private final AtomicReference<T> delegateRef;
    private final Class<T> interfaceClass;
    private volatile boolean isClosed;

    private ServiceProxy(
            AwaitingLeadership awaitingLeadership,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        this.awaitingLeadership = awaitingLeadership;
        this.delegateSupplier = delegateSupplier;
        this.delegateRef = new AtomicReference<>();
        this.interfaceClass = interfaceClass;
        this.isClosed = false;
    }

    public static <U> U newProxyInstance(AwaitingLeadership awaitingLeadership,
            Class<U> interfaceClass, Supplier<U> delegateSupplier) {
        ServiceProxy<U> proxy =
                new ServiceProxy<>(awaitingLeadership, delegateSupplier, interfaceClass);

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

        final LeadershipToken leadershipToken = awaitingLeadership.getLeadershipToken();

        T maybeValidDelegate = getMaybeValidDelegate();

        ListenableFuture<StillLeadingStatus> leadingFuture = Tracers.wrapListenableFuture(
                "validate-leadership",
                () -> statusRetrier.execute(() -> Tracers.wrapListenableFuture(
                        "validate-leadership-attempt", () -> awaitingLeadership.getStillLeading(leadershipToken))));

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

    private T getMaybeValidDelegate() {
        T delegate = delegateRef.get();
        if (delegate != null) {
            return delegate;
        }

        if (awaitingLeadership.canServiceRequest()) {
            // We are now the leader, we should create a delegate so we can service calls
            while (delegate == null) {
                try {
                    delegate = delegateSupplier.get();
                } catch (Throwable t) {
                    log.error("problem creating delegate", t);
                    if (isClosed) {
                        break; // todo
                    }
                }
            }

            // Do not modify, hide, or remove this line without considering impact on correctness.
            delegateRef.set(delegate);
        }

        return delegateRef.get();
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
                && !awaitingLeadership.isStillCurrentToken(leadershipToken)) {
            throw awaitingLeadership.notCurrentLeaderException("received an interrupt due to leader election.",
                    exception.getCause());
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
                // we don't want to rethrow here; we're likely on a background thread
                log.warn("problem closing delegate", ex);
            }
        }
    }

    private void handleNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        try {
            clearDelegate();
        } catch (Throwable t) {
            // If close fails we should still try to gain leadership
        }
        awaitingLeadership.markAsNotLeading(leadershipToken, cause);
    }
}
