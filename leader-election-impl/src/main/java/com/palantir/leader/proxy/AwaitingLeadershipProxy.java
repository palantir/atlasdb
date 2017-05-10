/**
 * Copyright 2015 Palantir Technologies
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
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

public final class AwaitingLeadershipProxy extends AbstractInvocationHandler {

    private static final AtomicInteger numProxies = new AtomicInteger();

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass,
                                         Supplier<T> delegateSupplier,
                                         LeaderElectionService leaderElectionService) {
        AwaitingLeadershipProxy proxy = new AwaitingLeadershipProxy(
                delegateSupplier,
                leaderElectionService,
                interfaceClass);
        proxy.tryToGainLeadership();
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[] {
                interfaceClass, Closeable.class }, proxy);
    }

    private final TaggedLogger log;
    private final TaggedLogger leaderLog;

    final Supplier<?> delegateSupplier;
    final LeaderElectionService leaderElectionService;
    final ExecutorService executor;
    /**
     * This is used as the handoff point between the executor doing the blocking
     * and the invocation calls.  It is set by the executor after the delegateRef is set.
     * It is cleared out by invoke which will close the delegate and spawn a new blocking task.
     */
    final AtomicReference<LeadershipToken> leadershipTokenRef;
    final AtomicReference<Object> delegateRef;
    final Class<?> interfaceClass;
    volatile boolean isClosed;

    private AwaitingLeadershipProxy(Supplier<?> delegateSupplier,
                                    LeaderElectionService leaderElectionService,
                                    Class<?> interfaceClass) {
        Validate.notNull(delegateSupplier);
        this.delegateSupplier = delegateSupplier;
        this.leaderElectionService = leaderElectionService;
        this.executor = PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true));
        this.leadershipTokenRef = new AtomicReference<LeadershipToken>();
        this.delegateRef = new AtomicReference<Object>();
        this.interfaceClass = interfaceClass;
        this.isClosed = false;

        String logTag = "Proxy #" + numProxies.getAndIncrement() + " - " + interfaceClass.getSimpleName();
        log = new TaggedLogger(AwaitingLeadershipProxy.class, logTag);
        leaderLog = new TaggedLogger("leadership", logTag);
    }

    private void tryToGainLeadership() {
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    gainLeadership();
                }
            });
        } catch (RejectedExecutionException e) {
            if (!isClosed) {
                throw new IllegalStateException("failed to submit task but proxy not closed", e);
            }
        }
    }

    private void gainLeadership() {
        try {
            leaderLog.info("Not the leader. Going to sleep.");
            LeadershipToken leadershipToken = leaderElectionService.blockOnBecomingLeader();
            // We are now the leader, we should create a delegate so we can service calls
            Object delegate = null;
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
                leaderLog.warn("Gained leadership for {}", leadershipToken);
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
            log.warn("Shutting down now");
            leaderLog.warn("Shutting down now");
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
        leaderLog.warn("Noticed that we lost leadership", cause);
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

    /**
     * Wraps a delegate Logger, prepending the specified string to the start of each log message.
     */
    private static class TaggedLogger {
        private final Logger delegate;
        private final String tag;

        public TaggedLogger(Class clazz, String tag) {
            this.delegate = LoggerFactory.getLogger(clazz);
            this.tag = "[" + tag + "] ";
        }

        public TaggedLogger(String name, String tag) {
            this.delegate = LoggerFactory.getLogger(name);
            this.tag = "[" + tag + "] ";
        }

        public void trace(String s, Throwable t) {
            delegate.trace(tag + s, t);
        }

        public void debug(String s, Throwable t) {
            delegate.debug(tag + s, t);
        }

        public void info(String s, Throwable t) {
            delegate.info(tag + s, t);
        }

        public void warn(String s, Throwable t) {
            delegate.warn(tag + s, t);
        }

        public void error(String s, Throwable t) {
            delegate.error(tag + s, t);
        }

        public void trace(String s, Object... args) {
            delegate.trace(tag + s, args);
        }

        public void debug(String s, Object... args) {
            delegate.debug(tag + s, args);
        }

        public void info(String s, Object... args) {
            delegate.info(tag + s, args);
        }

        public void warn(String s, Object... args) {
            delegate.warn(tag + s, args);
        }

        public void error(String s, Object... args) {
            delegate.error(tag + s, args);
        }
    }
}
