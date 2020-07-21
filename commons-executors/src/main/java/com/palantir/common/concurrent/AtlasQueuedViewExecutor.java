/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.common.concurrent;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Borrowed from jboss-threads. http://www.apache.org/licenses/LICENSE-2.0
 * https://github.com/jbossas/jboss-threads/blob/master/src/main/java/org/jboss/threads/QueuedViewExecutor.java Changes
 * have been contributed and merged, this may be replaced by the upstream ViewExecutor pending a release including
 * https://github.com/jbossas/jboss-threads/pull/85.
 *
 * <p>Licensed under http://www.apache.org/licenses/LICENSE-2.0.
 * https://github.com/jbossas/jboss-threads/blob/5df767f325214acf3f7b80fa5354411c4453e073/LICENSE.txt
 *
 * <p>An executorservice that is actually a "view" over another executor service.
 */
@SuppressWarnings({
        "checkstyle:InnerAssignment",
        "checkstyle:HiddenField",
        "checkstyle:NestedTryDepth",
        "checkstyle:CyclomaticComplexity",
        "NullAway"
        })
final class AtlasQueuedViewExecutor extends AtlasViewExecutor {
    private static final Logger log = LoggerFactory.getLogger(AtlasQueuedViewExecutor.class);
    private static final Runnable[] NO_RUNNABLES = new Runnable[0];

    private final Executor delegate;

    private final Lock lock;
    private final Condition shutDownCondition;
    private final ArrayDeque<Runnable> queue;
    private final Set<TaskWrapper> allWrappers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final int queueLimit;
    private final short maxCount;
    private short submittedCount;
    private short runningCount;
    private int state = ST_RUNNING;

    private static final int ST_RUNNING = 0;
    private static final int ST_SHUTDOWN_REQ = 1;
    private static final int ST_SHUTDOWN_INT_REQ = 2;
    private static final int ST_STOPPED = 3;

    AtlasQueuedViewExecutor(
            final Executor delegate,
            final short maxCount,
            final int queueLimit,
            final int queueInitialSize,
            final Thread.UncaughtExceptionHandler handler) {
        this.delegate = delegate;
        this.maxCount = maxCount;
        this.queueLimit = queueLimit;
        this.setExceptionHandler(handler);
        queue = new ArrayDeque<>(Math.min(queueLimit, queueInitialSize));
        lock = new ReentrantLock();
        shutDownCondition = lock.newCondition();
    }

    @Override
    public void execute(final Runnable command) {
        lock.lock();
        try {
            if (state != ST_RUNNING) {
                throw new RejectedExecutionException("Executor has been shut down");
            }
            final short submittedCount = this.submittedCount;
            if (runningCount + submittedCount < maxCount) {
                this.submittedCount = (short) (submittedCount + 1);
                final TaskWrapper tw = new TaskWrapper(command);
                allWrappers.add(tw);
                try {
                    /* this cannot be easily moved outside of the lock, otherwise queued tasks might never run
                     * under certain rare scenarios.
                     */
                    delegate.execute(tw);
                } catch (Throwable t) {
                    this.submittedCount--;
                    allWrappers.remove(tw);
                    throw t;
                }
            } else if (queue.size() < queueLimit) {
                queue.add(command);
            } else {
                throw new RejectedExecutionException("No executor queue space remaining");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("LockNotBeforeTry")
    public void shutdown(boolean interrupt) {
        lock.lock();
        int oldState = this.state;
        if (oldState < ST_SHUTDOWN_REQ) {
            // do shutdown
            final boolean emptyQueue;
            try {
                emptyQueue = queue.isEmpty();
            } catch (Throwable t) {
                lock.unlock();
                throw t;
            }
            if (runningCount == 0 && submittedCount == 0 && emptyQueue) {
                this.state = ST_STOPPED;
                try {
                    shutDownCondition.signalAll();
                } finally {
                    lock.unlock();
                }
                runTermination();
                return;
            }
        }
        // didn't exit
        this.state = interrupt ? ST_SHUTDOWN_INT_REQ : ST_SHUTDOWN_REQ;
        lock.unlock();
        if (interrupt && oldState < ST_SHUTDOWN_INT_REQ) {
            // interrupt all runners
            for (TaskWrapper wrapper : allWrappers) {
                wrapper.interrupt();
            }
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        lock.lock();
        int oldState = this.state;
        final Runnable[] tasks;
        try {
            tasks = queue.toArray(NO_RUNNABLES);
            queue.clear();
        } catch (Throwable t) {
            lock.unlock();
            throw t;
        }
        if (oldState < ST_SHUTDOWN_INT_REQ) {
            // do shutdown
            if (runningCount == 0 && submittedCount == 0) {
                this.state = ST_STOPPED;
                try {
                    shutDownCondition.signalAll();
                } finally {
                    lock.unlock();
                }
                runTermination();
            } else {
                this.state = ST_SHUTDOWN_INT_REQ;
                lock.unlock();
                // interrupt all runners
                for (TaskWrapper wrapper : allWrappers) {
                    wrapper.interrupt();
                }
            }
        } else {
            lock.unlock();
        }
        return Arrays.asList(tasks);
    }

    @Override
    public boolean isShutdown() {
        lock.lock();
        try {
            return state >= ST_SHUTDOWN_REQ;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isTerminated() {
        lock.lock();
        try {
            return state == ST_STOPPED;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            if (state == ST_STOPPED) {
                return true;
            }
            final long nanos = unit.toNanos(timeout);
            if (nanos <= 0) {
                return false;
            }
            long elapsed = 0;
            final long start = System.nanoTime();
            for (;;) {
                shutDownCondition.awaitNanos(nanos - elapsed);
                if (state == ST_STOPPED) {
                    return true;
                }
                elapsed = System.nanoTime() - start;
                if (elapsed >= nanos) {
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "view of " + delegate;
    }

    class TaskWrapper implements Runnable {
        private volatile Thread thread;
        private Runnable command;

        TaskWrapper(final Runnable command) {
            this.command = command;
        }

        synchronized void interrupt() {
            final Thread thread = this.thread;
            if (thread != null) {
                thread.interrupt();
            }
        }

        @Override
        public void run() {
            boolean resetStateOnCompletion = true;
            thread = Thread.currentThread();
            try {
                for (;;) {
                    lock.lock();
                    try {
                        submittedCount--;
                        runningCount++;
                        // Interruption may be missed between when a TaskWrapper is submitted
                        // to the delegate executor, and when the task begins to execute.
                        // This must execute after thread is set.
                        if (state == ST_SHUTDOWN_INT_REQ) {
                            Thread.currentThread().interrupt();
                        }
                    } finally {
                        lock.unlock();
                    }
                    try {
                        command.run();
                    } catch (Throwable t) {
                        try {
                            getExceptionHandler().uncaughtException(Thread.currentThread(), t);
                        } catch (Throwable tt) {
                            log.debug("failed to call the uncaught exception handler", tt);
                        }
                    }
                    lock.lock();
                    runningCount--;
                    try {
                        command = queue.pollFirst();
                    } catch (Throwable t) {
                        lock.unlock();
                        throw t;
                    }
                    if (runningCount + submittedCount < maxCount && command != null) {
                        // execute next
                        submittedCount++;
                        lock.unlock();
                    } else if (command == null && runningCount == 0 && submittedCount == 0 && state != ST_RUNNING) {
                        // we're the last task
                        state = ST_STOPPED;
                        try {
                            shutDownCondition.signalAll();
                        } finally {
                            lock.unlock();
                        }
                        runTermination();
                        return;
                    } else {
                        lock.unlock();
                        return;
                    }
                    try {
                        // Unset the current thread prior to resubmitting this task to avoid clobbering the value
                        // if the delegate executes in parallel to the finally block.
                        unsetThread();
                        delegate.execute(this);
                        resetStateOnCompletion = false;
                        // resubmitted this task for execution, so return
                        return;
                    } catch (Throwable t) {
                        log.warn(
                                "Failed to resubmit executor task to delegate executor"
                                        + " (executing task immediately instead)",
                                t);
                        // resubmit failed, so continue execution in this thread after resetting state
                        thread = Thread.currentThread();
                        // resubmit failed, so continue execution in this thread
                    }
                }
            } finally {
                if (resetStateOnCompletion) {
                    allWrappers.remove(this);
                    unsetThread();
                }
            }
        }

        // Must be synchronized with interrupt() to avoid acquiring the thread reference as work completes
        // and interrupting the next task run by this thread which may not originate from this view.
        private synchronized void unsetThread() {
            thread = null;
        }
    }
}
