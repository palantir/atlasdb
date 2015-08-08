package com.palantir.leader.proxy;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;

public final class AwaitingLeadershipProxy extends AbstractInvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadershipProxy.class);

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
            delegateRef.set(delegate);
            if (isClosed) {
                clearDelegate();
            } else {
                leadershipTokenRef.set(leadershipToken);
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
            throw new NotCurrentLeaderException("method invoked on a non-leader");
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
            markAsNotLeading(leadershipToken, null);
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

    private void markAsNotLeading(final LeadershipToken leadershipToken, Throwable cause) {
        if (leadershipTokenRef.compareAndSet(leadershipToken, null)) {
            try {
                clearDelegate();
            } catch (Throwable t) {
                // If close fails we should still try to gain leadership
            }
            tryToGainLeadership();
        }
        throw new NotCurrentLeaderException("method invoked on a non-leader (leadership lost)", cause);
    }

    @Override
    public String toString() {
        LeadershipToken leadershipToken = leadershipTokenRef.get();
        Object delegate = delegateRef.get();
        boolean isLeading = leadershipToken != null;
        return "AwaitingLeadershipProxy [delegateSupplier=" + delegateSupplier
                + ", leaderElectionService=" + leaderElectionService
                + ", executor=" + executor
                + ", leadershipTokenRef=" + leadershipToken
                + ", delegateRef=" + delegate
                + ", interfaceClass=" + interfaceClass
                + ", isClosed=" + isClosed
                + ", isLeading=" + isLeading
                + "]";
    }

}
