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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.palantir.remoting2.tracing.Tracers;

public final class AwaitingLeadershipProxy<T> extends AbstractInvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadershipProxy.class);

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
        Preconditions.checkNotNull(delegateSupplier, "Unable to create an AwaitingLeadershipProxy with no supplier");
        this.delegateSupplier = delegateSupplier;
        this.leaderElectionService = leaderElectionService;
        this.executor = Tracers.wrap(PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true)));
        this.leadershipTokenRef = new AtomicReference<>();
        this.delegateRef = new AtomicReference<>();
        this.interfaceClass = interfaceClass;
        this.isClosed = false;
    }

    private void tryToGainLeadership() {
        try {
            executor.submit(this::gainLeadership);
        } catch (RejectedExecutionException e) {
            if (!isClosed) {
                throw new IllegalStateException("failed to submit task but proxy not closed", e);
            }
        }
    }

    private void gainLeadership() {
        try {
            LeadershipToken leadershipToken = leaderElectionService.blockOnBecomingLeader();
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
                leadershipTokenRef.set(leadershipToken);
                log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
            }
        } catch (InterruptedException e) {
            log.warn("attempt to gain leadership interrupted", e);
        } catch (Throwable e) {
            log.error("problem blocking on leadership", e);
        }
    }

    private void clearDelegate() throws IOException {
        Object delegate = delegateRef.getAndSet(null);
        if (delegate instanceof Closeable) {
            ((Closeable) delegate).close();
        }
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        final LeadershipToken leadershipToken = leadershipTokenRef.get();

        if (leadershipToken == null) {
            throw notCurrentLeaderException("method invoked on a non-leader");
        }

        if (method.getName().equals("close") && args.length == 0) {
            isClosed = true;
            executor.shutdownNow();
            clearDelegate();
            return null;
        }

        Object delegate = delegateRef.get();
        StillLeadingStatus leading;
        do {
            leading = leaderElectionService.isStillLeading(leadershipToken);
        } while (leading == StillLeadingStatus.NO_QUORUM);

        if (leading == StillLeadingStatus.NOT_LEADING) {
            markAsNotLeading(leadershipToken, null /* cause */);
        }

        if (isClosed) {
            throw new IllegalStateException("already closed proxy for " + interfaceClass.getName());
        }

        Preconditions.checkNotNull(delegate, interfaceClass.getName() + " backing is null");
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof ServiceNotAvailableException
                    || e.getCause() instanceof NotCurrentLeaderException) {
                markAsNotLeading(leadershipToken, e.getCause());
            }
            throw e.getCause();
        }
    }

    private NotCurrentLeaderException notCurrentLeaderException(String message, @Nullable Throwable cause) {
        Optional<HostAndPort> maybeLeader = leaderElectionService.getSuspectedLeaderInMemory();
        if (maybeLeader.isPresent()) {
            HostAndPort leaderHint = maybeLeader.get();
            return new NotCurrentLeaderException(message + "; hinting suspected leader host " + leaderHint, cause, leaderHint);
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
