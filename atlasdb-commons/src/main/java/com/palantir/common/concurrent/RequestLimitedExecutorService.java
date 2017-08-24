/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RequestLimitedExecutorService implements ExecutorService {

    private final ExecutorService delegate;
    private final Semaphore semaphore;
    private final long blockingTimeoutMillis;

    private RequestLimitedExecutorService(ExecutorService delegate, int maxConcurrency, Duration blockingTimeout) {
        this.delegate = delegate;
        this.semaphore = new Semaphore(maxConcurrency);
        this.blockingTimeoutMillis = blockingTimeout.toMillis();
    }

    public static RequestLimitedExecutorService fromDelegate(
            ExecutorService delegate, int maxConcurrency, Duration blockingTimeout) {
        return new RequestLimitedExecutorService(delegate, maxConcurrency, blockingTimeout);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return runBlockingOnSemaphore(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return runBlockingOnSemaphore(() -> {
            task.run();
            return result;
        });
    }

    @Override
    public Future<?> submit(Runnable task) {
        return runBlockingOnSemaphore(() -> {
            task.run();
            return null;
        });
    }

    @Override
    public void execute(Runnable command) {
        submit(command);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException("Not Implemented");
    }

    private <T> Future<T> runBlockingOnSemaphore(Callable<T> callable) {
        try {
            boolean acquired = semaphore.tryAcquire(blockingTimeoutMillis, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new RuntimeException(String.format(
                        "Timed out after %s seconds waiting to run operation", blockingTimeoutMillis));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return delegate.submit(() -> {
            try {
                return callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                semaphore.release();
            }
        });
    }
}
