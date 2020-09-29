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
package com.palantir.common.concurrent;

import com.palantir.common.base.Throwables;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * A {@link RunnableFuture} whose {@code get} methods will never throw
 * {@link CancellationException} <em>unless</em> the task was never run. If
 * {@link CancellationException} is thrown, then it is guaranteed that the
 * {@link #call()} method was never invoked and never will be invoked.
 * <p>
 * NOTE: The interface for future is arguably broken for the {@link Future#isCancelled()} method.
 * This class chooses {@link #isCancelled()} to be implemented in a more friendly way that breaks
 * the traditional {@link Future} interface.
 *
 * @param <V> the return type of {@link #call()}.
 * @author jtamer
 */
public abstract class InterruptibleFuture<V> implements RunnableFuture<V> {

    private static enum State { WAITING_TO_RUN, RUNNING, COMPLETED }

    private final Lock lock = new ReentrantLock(false);
    private final Condition condition = lock.newCondition();

    @GuardedBy(value = "lock") private V returnValue;
    @GuardedBy(value = "lock") private Throwable executionException;
    @GuardedBy(value = "lock") private volatile CancellationException cancellationException;
    @GuardedBy(value = "lock") private volatile State state = State.WAITING_TO_RUN;

    private final FutureTask<?> futureTask = new FutureTask<Void>(new Runnable() {
        @Override
        public void run() {
            lock.lock();
            try {
                if (state != State.WAITING_TO_RUN) return;
                state = State.RUNNING;
            } finally {
                lock.unlock();
            }
            V value = null;
            Throwable throwable = null;
            try {
                value = call();
            } catch (Throwable e) {
                throwable = e;
            }
            lock.lock();
            try {
                returnValue = value;
                executionException = throwable;
                noteFinished();
            } finally {
                lock.unlock();
            }
        }
    }, null);

    protected abstract V call() throws Exception;

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            if (state == State.WAITING_TO_RUN) {
                cancellationException = new CancellationException("Cancel was successful.");
                noteFinished();
            }
        } finally {
            lock.unlock();
        }
        return futureTask.cancel(mayInterruptIfRunning);
    }

    @SuppressWarnings("GuardedByChecker")
    protected void noteFinished() {
        state = State.COMPLETED;
        condition.signalAll();
    }

    /**
     * {@inheritDoc}
     * @throws CancellationException if and only if {@link #isCancelled()} returns true
     */
    @Override
    public final V get() throws InterruptedException, ExecutionException, CancellationException {
        lock.lock();
        try {
            while (state != State.COMPLETED) {
                condition.await();
            }
            return getReturnValue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * @throws CancellationException if and only if {@link #isCancelled()} returns true
     */
    @Override
    public final V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException, CancellationException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        lock.lock();
        try {
            while (state != State.COMPLETED) {
                if (condition.awaitNanos(deadline - System.nanoTime()) <= 0) {
                    throw new TimeoutException();
                }
            }
            return getReturnValue();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("GuardedByChecker")
    private V getReturnValue() throws ExecutionException, CancellationException {
        if (cancellationException != null) {
            throw Throwables.chain(new CancellationException("This task was canceled before it ever ran."), cancellationException);
        }
        if (executionException != null) {
            throw new ExecutionException(executionException);
        }
        return returnValue;
    }

    /**
     * @return true if and only if the task was canceled before it ever executed
     */
    @Override
    @SuppressWarnings("GuardedByChecker")
    public final boolean isCancelled() {
        return cancellationException != null;
    }

    @Override
    @SuppressWarnings("GuardedByChecker")
    public final boolean isDone() {
        return state == State.COMPLETED;
    }

    @Override
    public final void run() {
        futureTask.run();
    }

    public static <V> InterruptibleFuture<V> of(final Callable<V> callable) {
        return new InterruptibleFuture<V>() {
            @Override
            protected V call() throws Exception {
                return callable.call();
            }
        };
    }
}
