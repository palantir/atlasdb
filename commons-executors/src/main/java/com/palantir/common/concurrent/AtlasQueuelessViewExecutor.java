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

import com.palantir.logsafe.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nullable;

/**
 * Borrowed from jboss-threads. http://www.apache.org/licenses/LICENSE-2.0
 * https://github.com/jbossas/jboss-threads/blob/master/src/main/java/org/jboss/threads/QueuelessViewExecutor.java
 * Changes have been contributed and merged, this may be replaced by the upstream ViewExecutor pending a release
 * including https://github.com/jbossas/jboss-threads/pull/85.
 *
 * <p>Licensed under http://www.apache.org/licenses/LICENSE-2.0.
 * https://github.com/jbossas/jboss-threads/blob/5df767f325214acf3f7b80fa5354411c4453e073/LICENSE.txt
 *
 * <p>A View Executor implementation which avoids lock contention in the common path. This allows us to provide
 * references to the same underlying pool of threads to different consumers and utilize distinct instrumentation without
 * duplicating resources. This implementation is optimized to avoid locking in cases where the view is not required
 * queue work beyond a fixed number of permits, useful for cached executors for example.
 *
 * @author <a href="mailto:ckozak@ckozak.net">Carter Kozak</a>
 */
@SuppressWarnings({"checkstyle:InnerAssignment", "checkstyle:HiddenField", "NullAway"})
final class AtlasQueuelessViewExecutor extends AtlasViewExecutor {
    private static final AtomicIntegerFieldUpdater<AtlasQueuelessViewExecutor> stateUpdater =
            AtomicIntegerFieldUpdater.newUpdater(AtlasQueuelessViewExecutor.class, "state");

    private static final int SHUTDOWN_MASK = 1 << 31;
    private static final int ACTIVE_COUNT_MASK = (1 << 31) - 1;

    private final Executor delegate;
    private final int maxCount;

    private final Object shutdownLock = new Object();
    private final Set<AtlasQueuelessViewExecutorRunnable> activeRunnables = ConcurrentHashMap.newKeySet();

    /**
     * State structure.
     *
     * <ul>
     *   <li>Bit 00..30: Number of active tasks (unsigned)
     *   <li>Bit 31: executor shutdown state; 0 = shutdown has not been requested
     * </ul>
     */
    @SuppressWarnings("unused")
    private volatile int state = 0;

    private volatile boolean interrupted = false;

    AtlasQueuelessViewExecutor(
            final Executor delegate,
            final int maxCount,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
        this.maxCount = maxCount;
        this.setExceptionHandler(uncaughtExceptionHandler);
    }

    @Override
    public void shutdown(boolean interrupt) {
        for (;;) {
            int stateSnapshot = state;
            if (isShutdown(stateSnapshot)) {
                break; // nothing to do
            }
            int newState = stateSnapshot | SHUTDOWN_MASK;
            if (compareAndSwapState(stateSnapshot, newState)) {
                notifyWaitersIfTerminated(newState);
                break;
            }
        }
        if (interrupt) {
            interrupted = true;
            activeRunnables.forEach(AtlasQueuelessViewExecutorRunnable::interrupt);
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown(true);
        // This implementation is built for cached executors which do not queue so it's impossible
        // to have queued runnables.
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return isShutdown(state);
    }

    private static boolean isShutdown(int state) {
        return (state & SHUTDOWN_MASK) != 0;
    }

    @Override
    public boolean isTerminated() {
        return isTerminated(state);
    }

    private static boolean isTerminated(int state) {
        return state == SHUTDOWN_MASK;
    }

    private void notifyWaitersIfTerminated(int stateSnapshot) {
        if (isTerminated(stateSnapshot)) {
            synchronized (shutdownLock) {
                shutdownLock.notifyAll();
            }
            runTermination();
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        // Use the system precise clock to avoid issues resulting from time changes.
        long now = System.nanoTime();
        synchronized (shutdownLock) {
            while (!isTerminated()) {
                remainingNanos -= Math.max(-now + (now = System.nanoTime()), 0L);
                long remainingMillis = TimeUnit.MILLISECONDS.convert(remainingNanos, TimeUnit.NANOSECONDS);
                if (remainingMillis <= 0) {
                    return false;
                }
                shutdownLock.wait(remainingMillis);
            }
            return true;
        }
    }

    @Override
    public void execute(Runnable task) {
        Preconditions.checkNotNull(task, "task");
        incrementActiveOrReject();
        boolean submittedTask = false;
        try {
            // When CachedExecutorViewRunnable allocation fails the active count must be reduced.
            delegate.execute(new AtlasQueuelessViewExecutorRunnable(task));
            submittedTask = true;
        } finally {
            if (!submittedTask) {
                decrementActive();
            }
        }
    }

    /** Increments the active task count, otherwise throws a {@link RejectedExecutionException}. */
    private void incrementActiveOrReject() {
        int maxCount = this.maxCount;
        for (;;) {
            int stateSnapshot = state;
            if (isShutdown(stateSnapshot)) {
                throw new RejectedExecutionException("Executor has been shut down");
            }

            int activeCount = getActiveCount(stateSnapshot);
            if (activeCount >= maxCount) {
                throw new RejectedExecutionException("No executor queue space remaining");
            }
            int updatedActiveCount = activeCount + 1;
            if (compareAndSwapState(stateSnapshot, updatedActiveCount | (stateSnapshot & ~ACTIVE_COUNT_MASK))) {
                return;
            }
        }
    }

    private void decrementActive() {
        for (;;) {
            int stateSnapshot = state;
            int updatedActiveCount = getActiveCount(stateSnapshot) - 1;
            int newState = updatedActiveCount | (stateSnapshot & ~ACTIVE_COUNT_MASK);
            if (compareAndSwapState(stateSnapshot, newState)) {
                notifyWaitersIfTerminated(newState);
                return;
            }
        }
    }

    private static int getActiveCount(int state) {
        return state & ACTIVE_COUNT_MASK;
    }

    private boolean compareAndSwapState(int expected, int update) {
        return stateUpdater.compareAndSet(this, expected, update);
    }

    @Override
    public String toString() {
        return "AtlasQueuelessViewExecutor{delegate=" + delegate + ", state=" + state + '}';
    }

    private final class AtlasQueuelessViewExecutorRunnable implements Runnable {

        private final Runnable delegate;

        @Nullable
        private volatile Thread thread;

        AtlasQueuelessViewExecutorRunnable(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            Thread currentThread = Thread.currentThread();
            Set<AtlasQueuelessViewExecutorRunnable> runnables = activeRunnables;
            this.thread = currentThread;
            try {
                runnables.add(this);
                if (interrupted) {
                    // shutdownNow may have been invoked after this task was submitted
                    // but prior to activeRunnables.add(this).
                    currentThread.interrupt();
                }
                delegate.run();
            } catch (Throwable t) {
                // The uncaught exception handler should be called on the current thread in order to log
                // using the updated thread name based on nameFunction.
                uncaughtExceptionHandler().uncaughtException(thread, t);
            } finally {
                runnables.remove(this);
                // Synchronization is important to avoid racily reading the current thread and interrupting
                // it after this task completes and a task from another view has begun execution.
                synchronized (this) {
                    this.thread = null;
                }
                decrementActive();
            }
        }

        private Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
            Thread.UncaughtExceptionHandler handler = getExceptionHandler();
            if (handler != null) {
                return handler;
            }
            // If not uncaught exception handler is set, use the current threads existing handler if present.
            // Otherwise use the default handler.
            Thread.UncaughtExceptionHandler threadHandler =
                    Thread.currentThread().getUncaughtExceptionHandler();
            return threadHandler != null ? threadHandler : AtlasUncaughtExceptionHandler.INSTANCE;
        }

        synchronized void interrupt() {
            Thread taskThread = this.thread;
            if (taskThread != null) {
                taskThread.interrupt();
            }
        }

        @Override
        public String toString() {
            return "AtlasQueuelessViewExecutorRunnable{" + delegate + '}';
        }
    }
}
