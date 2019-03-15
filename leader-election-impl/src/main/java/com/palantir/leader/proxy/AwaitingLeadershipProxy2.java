/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;

public final class AwaitingLeadershipProxy2<T> extends AbstractInvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadershipProxy2.class);

    private static final long MAX_NO_QUORUM_RETRIES = 10;

    @SuppressWarnings("unchecked")
    public static <U> U newProxyInstance(Class<U> interfaceClass,
                                         Supplier<U> delegateSupplier,
                                         AwaitingLeadershipService awaitingLeadershipService) {
        AwaitingLeadershipProxy2<U> proxy = new AwaitingLeadershipProxy2<>(
                delegateSupplier,
                awaitingLeadershipService,
                interfaceClass);
        awaitingLeadershipService.registerGainLeadershipTask(proxy::onGainedLeadership);
        awaitingLeadershipService.registerLosingLeadershipTask(proxy::markAsNotLeading);

        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @VisibleForTesting
    static <T> AwaitingLeadershipProxy2<T> proxyForTest(Supplier<T> delegateSupplier,
            AwaitingLeadershipService awaitingLeadershipService,
            Class<T> interfaceClass) {
        return new AwaitingLeadershipProxy2<>(delegateSupplier, awaitingLeadershipService, interfaceClass);
    }

    final Supplier<T> delegateSupplier;
    final AwaitingLeadershipService awaitingLeadershipService;
    final ExecutorService executor;
    /**
     * This is used as the handoff point between the executor doing the blocking
     * and the invocation calls.  It is set by the executor after the delegateRef is set.
     * It is cleared out by invoke which will close the delegate and spawn a new blocking task.
     */
    final AtomicReference<T> delegateRef;
    final Class<T> interfaceClass;
    volatile boolean isClosed;

    private AwaitingLeadershipProxy2(Supplier<T> delegateSupplier, AwaitingLeadershipService awaitingLeadershipService,
            Class<T> interfaceClass) {
        Preconditions.checkNotNull(delegateSupplier,
                "Unable to create an AwaitingLeadershipProxy with no supplier");
        this.delegateSupplier = delegateSupplier;
        this.awaitingLeadershipService = awaitingLeadershipService;
        this.executor = PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true));
        this.delegateRef = new AtomicReference<>();
        this.interfaceClass = interfaceClass;
        this.isClosed = false;
    }

    private void onGainedLeadership(LeadershipToken leadershipToken)  {
        log.debug("Gained leadership, getting delegates to start serving calls");
        // We are now the leader, we should create a delegate so we can service calls
        T delegate = null;
        while (delegate == null) {
            try {
                delegate = delegateSupplier.get();
            } catch (Throwable t) {
                log.error("problem creating delegate", t);
                if (isClosed) {
                    return;
                }
            }
        }

        // Do not modify, hide, or remove this line without considering impact on correctness.
        delegateRef.set(delegate);

        if (isClosed) {
            clearDelegate();
        } else {
            log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
        }
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

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("close") && args.length == 0) {
            log.debug("Closing leadership proxy");
            isClosed = true;
            executor.shutdownNow();
            clearDelegate();
            return null;
        }

        final LeadershipToken leadershipToken = awaitingLeadershipService.getLeadershipToken();

        Object delegate = delegateRef.get();
        StillLeadingStatus leading = null;
        for (int i = 0; i < MAX_NO_QUORUM_RETRIES; i++) {
            leading = awaitingLeadershipService.isStillLeading(leadershipToken);
            if (leading != StillLeadingStatus.NO_QUORUM) {
                break;
            }
        }

        // treat a repeated NO_QUORUM as NOT_LEADING; likely we've been cut off from the other nodes
        // and should assume we're not the leader
        if (leading == StillLeadingStatus.NOT_LEADING || leading == StillLeadingStatus.NO_QUORUM) {
            awaitingLeadershipService.markAsNotLeading(leadershipToken, new RuntimeException(leading.toString()));
        }

        if (isClosed) {
            throw new IllegalStateException("already closed proxy for " + interfaceClass.getName());
        }

        Preconditions.checkNotNull(delegate, "%s backing is null", interfaceClass.getName());
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof ServiceNotAvailableException
                    || e.getTargetException() instanceof NotCurrentLeaderException) {
                awaitingLeadershipService.markAsNotLeading(leadershipToken, e.getCause());
            }
            // Prevent blocked lock requests from receiving a non-retryable 500 on interrupts
            // in case of a leader election.
            if (e.getTargetException() instanceof InterruptedException && !awaitingLeadershipService.isStillCurrentToken(leadershipToken)) {
                throw notCurrentLeaderException("received an interrupt due to leader election.",
                        e.getTargetException());
            }
            throw e.getTargetException();
        }
    }

    private void markAsNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        try {
            clearDelegate();
        } catch (Throwable t) {
            // If close fails we should still try to gain leadership
        }
    }

    private NotCurrentLeaderException notCurrentLeaderException(String message, @Nullable Throwable cause) {
        Optional<HostAndPort> maybeLeader = awaitingLeadershipService.getSuspectedLeaderInMemory();
        if (maybeLeader.isPresent()) {
            HostAndPort leaderHint = maybeLeader.get();
            return new NotCurrentLeaderException(message + "; hinting suspected leader host " + leaderHint,
                    cause, leaderHint);
        } else {
            return new NotCurrentLeaderException(message, cause);
        }
    }

}
