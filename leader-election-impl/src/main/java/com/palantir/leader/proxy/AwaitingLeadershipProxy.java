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
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;

public final class AwaitingLeadershipProxy<T> extends AbstractInvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadershipProxy.class);

    private static final long MAX_NO_QUORUM_RETRIES = 10;
    private static final Duration GAIN_LEADERSHIP_BACKOFF = Duration.ofMillis(500);

    public static <U> U newProxyInstance(Class<U> interfaceClass,
                                         Supplier<U> delegateSupplier,
                                         LeaderElectionService leaderElectionService) {
        AwaitingLeadershipProxy<U> proxy = new AwaitingLeadershipProxy<>(
                delegateSupplier,
                leaderElectionService,
                interfaceClass);
        proxy.tryToGainLeadership();

        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @VisibleForTesting
    static <T> AwaitingLeadershipProxy<T> proxyForTest(Supplier<T> delegateSupplier,
            LeaderElectionService leaderElectionService,
            Class<T> interfaceClass,
            AtomicReference<LeadershipToken> leadershipTokenRef) {
        return new AwaitingLeadershipProxy<>(delegateSupplier, leaderElectionService, interfaceClass,
                leadershipTokenRef);
    }

    final Supplier<T> delegateSupplier;
    final LeaderElectionService leaderElectionService;
    final ExecutorService executor;
    /**
     * This is used as the handoff point between the executor doing the blocking
     * and the invocation calls.  It is set by the executor after the delegateRef is set.
     * It is cleared out by invoke which will close the delegate and spawn a new blocking task.
     */
    final AtomicReference<LeadershipToken> leadershipTokenRef;
    final AtomicReference<T> delegateRef;
    final Class<T> interfaceClass;
    volatile boolean isClosed;

    private AwaitingLeadershipProxy(Supplier<T> delegateSupplier,
                                    LeaderElectionService leaderElectionService,
                                    Class<T> interfaceClass) {
        this(delegateSupplier, leaderElectionService, interfaceClass, new AtomicReference<>());
    }

    private AwaitingLeadershipProxy(Supplier<T> delegateSupplier, LeaderElectionService leaderElectionService,
            Class<T> interfaceClass, AtomicReference<LeadershipToken> leadershipTokenRef) {
        Preconditions.checkNotNull(delegateSupplier,
                "Unable to create an AwaitingLeadershipProxy with no supplier");
        this.delegateSupplier = delegateSupplier;
        this.leaderElectionService = leaderElectionService;

        this.executor = Tracers.wrapWithNewTrace("AwaitingLeadershipProxy.executor",
                PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true)));
        this.leadershipTokenRef = leadershipTokenRef;
        this.delegateRef = new AtomicReference<>();
        this.interfaceClass = interfaceClass;
        this.isClosed = false;
    }

    private void tryToGainLeadership() {
        Optional<LeadershipToken> currentToken = leaderElectionService.getCurrentTokenIfLeading();
        if (currentToken.isPresent()) {
            log.info("we've already been elected leader, calling on gained leadership directly, coming from mark as not leading",
                    SafeArg.of("thread", Thread.currentThread().getId()));
            onGainedLeadership(currentToken.get());
        } else {
            log.info("we've not been elected leader, we will try to get in the background",
                    SafeArg.of("thread", Thread.currentThread().getId()));
            tryToGainLeadershipAsync();
        }
    }

    private void tryToGainLeadershipAsync() {
        try {
            executor.submit(this::gainLeadershipWithRetry);
        } catch (RejectedExecutionException e) {
            if (!isClosed) {
                throw new IllegalStateException("failed to submit task but proxy not closed", e);
            }
        }
    }

    private void gainLeadershipWithRetry() {
        log.info("running with trace id", SafeArg.of("traceId", Tracer.getTraceId()));
        while (!gainLeadershipBlocking()) {
            try {
                log.info("Failed to gain leadership, sleeping",
                        SafeArg.of("thread", Thread.currentThread().getId()),
                        SafeArg.of("duration", GAIN_LEADERSHIP_BACKOFF.toMillis()));
                Thread.sleep(GAIN_LEADERSHIP_BACKOFF.toMillis());
            } catch (InterruptedException e) {
                log.warn("gain leadership backoff interrupted");
            }
        }
    }

    private boolean gainLeadershipBlocking() {
        log.info("Block until gained leadership", SafeArg.of("thread", Thread.currentThread().getId()));
        try {
            LeadershipToken leadershipToken = leaderElectionService.blockOnBecomingLeader();
            onGainedLeadership(leadershipToken);
            return true;
        } catch (InterruptedException e) {
            log.warn("attempt to gain leadership interrupted", e);
        } catch (Throwable e) {
            log.error("problem blocking on leadership", e);
        }
        return false;
    }

    private void onGainedLeadership(LeadershipToken leadershipToken)  {
        try (CloseableTrace ignored = startLocalTrace("onGainedLeadership")) {
            onGainedLeadershipUntraced(leadershipToken);
        }
    }

    private void onGainedLeadershipUntraced(LeadershipToken leadershipToken) {
        log.info("Gained leadership, getting delegate to start serving calls",
                SafeArg.of("thread", Thread.currentThread().getId()));
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
        log.info("Finished creating delegate",
                SafeArg.of("thread", Thread.currentThread().getId()));
        // Do not modify, hide, or remove this line without considering impact on correctness.
        delegateRef.set(delegate);

        if (isClosed) {
            clearDelegate();
        } else {
            leadershipTokenRef.set(leadershipToken);
            log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken),
                    SafeArg.of("thread", Thread.currentThread().getId()));
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

    private static CloseableTrace startLocalTrace(String operation) {
        log.info("tracing operation " + operation, SafeArg.of("thread", Thread.currentThread().getId()));
        return CloseableTrace.startLocalTrace("AtlasDB:AwaitingLeadershipProxy", operation);
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

        final LeadershipToken leadershipToken = getLeadershipToken();

        Object delegate = delegateRef.get();
        StillLeadingStatus leading = null;
        for (int i = 0; i < MAX_NO_QUORUM_RETRIES; i++) {
            // TODO(nziebart): check if leadershipTokenRef has been nulled out between iterations?
            leading = leaderElectionService.isStillLeading(leadershipToken);
            if (leading != StillLeadingStatus.NO_QUORUM) {
                break;
            }
        }

        // treat a repeated NO_QUORUM as NOT_LEADING; likely we've been cut off from the other nodes
        // and should assume we're not the leader
        if (leading == StillLeadingStatus.NOT_LEADING || leading == StillLeadingStatus.NO_QUORUM) {
            markAsNotLeading(leadershipToken, null /* cause */);
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
                markAsNotLeading(leadershipToken, e.getCause());
            }
            // Prevent blocked lock requests from receiving a non-retryable 500 on interrupts
            // in case of a leader election.
            if (e.getTargetException() instanceof InterruptedException && !isStillCurrentToken(leadershipToken)) {
                throw notCurrentLeaderException("received an interrupt due to leader election.",
                        e.getTargetException());
            }
            throw e.getTargetException();
        }
    }

    @VisibleForTesting
    LeadershipToken getLeadershipToken() {
        LeadershipToken leadershipToken = leadershipTokenRef.get();

        if (leadershipToken == null) {
            NotCurrentLeaderException notCurrentLeaderException = notCurrentLeaderException(
                    "method invoked on a non-leader");

            if (notCurrentLeaderException.getServiceHint().isPresent()) {
                // There's a chance that we can gain leadership while generating this exception.
                // In this case, we should be able to get a leadership token after all
                leadershipToken = leadershipTokenRef.get();
                // If leadershipToken is still null, then someone's the leader, but it isn't us.
            }

            if (leadershipToken == null) {
                throw notCurrentLeaderException;
            }
        }

        return leadershipToken;
    }

    private boolean isStillCurrentToken(LeadershipToken leadershipToken) {
        return leadershipTokenRef.get() == leadershipToken;
    }

    private NotCurrentLeaderException notCurrentLeaderException(String message, @Nullable Throwable cause) {
        Optional<HostAndPort> maybeLeader = leaderElectionService.getSuspectedLeaderInMemory();
        if (maybeLeader.isPresent()) {
            HostAndPort leaderHint = maybeLeader.get();
            return new NotCurrentLeaderException(message + "; hinting suspected leader host " + leaderHint,
                    cause, leaderHint);
        } else {
            return new NotCurrentLeaderException(message, cause);
        }
    }

    private NotCurrentLeaderException notCurrentLeaderException(String message) {
        return notCurrentLeaderException(message, null /* cause */);
    }

    private void markAsNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        log.warn("Lost leadership", cause);
        if (leadershipTokenRef.compareAndSet(leadershipToken, null)) {
            // this is fine in the case that this node has been elected leader again (i.e. with a different leadership
            // token). `onGainedLeadership` guarantees that the delegate will be refreshed *before* we get a new
            // leadershipToken. We're closing here instead of relying on the close in `onGainedLeadership` to reclaim
            // resources and does not affect correctness.

            // if we were to move this above or below the CAS, we could race with `onGainedLeadership` and end up
            // clearing `delegateRef`.
            try {
                clearDelegate();
            } catch (Throwable t) {
                // If close fails we should still try to gain leadership
            }
            tryToGainLeadership();
        }
        throw notCurrentLeaderException("method invoked on a non-leader (leadership lost)", cause);
    }

}
