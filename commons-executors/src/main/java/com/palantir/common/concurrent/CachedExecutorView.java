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

package com.palantir.common.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.IntUnaryOperator;

import javax.annotation.Nullable;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * Efficient view of a cached executor service which avoids lock contention in the common path. This allows us to
 * provide references to the same underlying pool of threads to different consumers. We take advantage of the fact that
 * cached executors never queue, and always submit work immediately to avoid implementing queueing.
 *
 * <p>This implementation is based on the design of the Undertow GracefulShutdownHandler.
 */
final class CachedExecutorView extends AbstractExecutorService {
    private static final AtomicIntegerFieldUpdater<CachedExecutorView> stateUpdater =
            AtomicIntegerFieldUpdater.newUpdater(CachedExecutorView.class, "state");

    private static final int SHUTDOWN_MASK = 1 << 31;
    private static final int TERMINATED_MASK = 1 << 30;
    private static final int ACTIVE_COUNT_MASK = (1 << 30) - 1;

    // Hoist expensive lambda allocations
    private static final IntUnaryOperator incrementActiveStateUpdater = current -> {
        int activeCount = getActiveCount(current);
        // Defensive check. If there are a million active tasks something has gone terribly wrong.
        if (activeCount == ACTIVE_COUNT_MASK) {
            throw new SafeIllegalStateException("There are already " + ACTIVE_COUNT_MASK + " active tasks");
        }
        int updatedActiveCount = activeCount + 1;
        return updatedActiveCount | (current & ~ACTIVE_COUNT_MASK);
    };

    private static final IntUnaryOperator incrementActiveAndShutdownStateUpdater =
            incrementActiveStateUpdater.andThen(current -> current | SHUTDOWN_MASK);

    private static final IntUnaryOperator decrementActiveStateUpdater = current -> {
        int updatedActiveCount = getActiveCount(current) - 1;
        int result = updatedActiveCount | (current & ~ACTIVE_COUNT_MASK);
        if (updatedActiveCount == 0 && isShutdown(current)) {
            result |= TERMINATED_MASK;
        }
        return result;
    };

    private final Executor delegate;
    private final Object shutdownLock = new Object();
    private final Set<CachedExecutorViewRunnable> activeRunnables = ConcurrentHashMap.newKeySet();

    /**
     * State structure.
     * <ul>
     *     <li>Bit 00..29: Number of active tasks (unsigned)
     *     <li>Bit 30: executor termination state; 0 = not terminated
     *     <li>Bit 31: executor shutdown state; 0 = shutdown has not been requested
     * </ul>
     */
    @SuppressWarnings("unused") // Used by stateUpdater
    private volatile int state = 0;

    private CachedExecutorView(Executor delegate) {
        this.delegate = Preconditions.checkNotNull(delegate, "Delegate Executor is required");
    }

    static ExecutorService of(Executor delegate) {
        return new CachedExecutorView(delegate);
    }

    @Override
    public void shutdown() {
        // The active task count is never zero while the shutdown flag is set.
        stateUpdater.updateAndGet(this, incrementActiveAndShutdownStateUpdater);
        decrementActive();
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        activeRunnables.forEach(CachedExecutorViewRunnable::interrupt);
        // This implementation is built for cached executors which do not queue so it's impossible
        // to have pending runnables.
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return isShutdown(stateUpdater.get(this));
    }

    private static boolean isShutdown(int state) {
        return (state & SHUTDOWN_MASK) != 0;
    }

    @Override
    public boolean isTerminated() {
        return isTerminated(stateUpdater.get(this));
    }

    private static boolean isTerminated(int state) {
        return (state & TERMINATED_MASK) != 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (!isShutdown()) {
            throw new SafeIllegalStateException("Executor has not been shut down");
        }
        synchronized (shutdownLock) {
            long end = System.currentTimeMillis() + unit.toMillis(timeout);
            while (!isTerminated()) {
                long remaining = end - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                shutdownLock.wait(remaining);
            }
            return true;
        }
    }

    @Override
    public void execute(Runnable task) {
        boolean submittedTask = false;
        int snapshot = stateUpdater.updateAndGet(this, incrementActiveStateUpdater);
        try {
            if (isShutdown(snapshot)) {
                throw new RejectedExecutionException("Executor has been shut down");
            }
            delegate.execute(new CachedExecutorViewRunnable(task));
            submittedTask = true;
        } finally {
            if (!submittedTask) {
                decrementActive();
            }
        }
    }

    private void decrementActive() {
        int stateSnapshot = stateUpdater.updateAndGet(this, decrementActiveStateUpdater);
        if (isTerminated(stateSnapshot)) {
            synchronized (shutdownLock) {
                shutdownLock.notifyAll();
            }
        }
    }

    private static int getActiveCount(int state) {
        return state & ACTIVE_COUNT_MASK;
    }

    private final class CachedExecutorViewRunnable implements Runnable {

        private final Runnable delegate;

        @Nullable
        private volatile Thread thread;

        CachedExecutorViewRunnable(Runnable delegate) {
            this.delegate = Preconditions.checkNotNull(delegate, "Runnable");
        }

        @Override
        public void run() {
            this.thread = Thread.currentThread();
            activeRunnables.add(this);
            try {
                delegate.run();
            } finally {
                activeRunnables.remove(this);
                // Synchronization is important to avoid racily reading the current thread and interrupting
                // it after this task completes and a task from another view has begun execution.
                synchronized (this) {
                    this.thread = null;
                }
                decrementActive();
            }
        }

        synchronized void interrupt() {
            Thread taskThread = this.thread;
            if (taskThread != null) {
                taskThread.interrupt();
            }
        }

        @Override
        public String toString() {
            return "CachedExecutorViewRunnable{delegate=" + delegate + '}';
        }
    }
}
