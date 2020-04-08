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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.google.common.collect.Collections2;
import com.palantir.logsafe.Preconditions;

/** An ExecutorService decorator which updates names of threads while tasks run. */
final class ThreadNamingExecutorService implements ExecutorService {
    private final ExecutorService delegate;
    private final ThreadNameFunction nameFunction;

    private ThreadNamingExecutorService(ExecutorService delegate, ThreadNameFunction nameFunction) {
        this.delegate = Preconditions.checkNotNull(delegate, "ExecutorService is required");
        this.nameFunction = Preconditions.checkNotNull(nameFunction, "Name function is required");
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
        return delegate.submit(wrap(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(wrap(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate.submit(wrap(task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(Collections2.transform(tasks, this::wrap));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return delegate.invokeAll(Collections2.transform(tasks, this::wrap), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(Collections2.transform(tasks, this::wrap));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(Collections2.transform(tasks, this::wrap), timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(wrap(command));
    }

    private Runnable wrap(Runnable in) {
        return new NamingRunnable(in, nameFunction);
    }

    private <T> Callable<T> wrap(Callable<T> in) {
        return new NamingCallable<>(in, nameFunction);
    }

    private static final class NamingRunnable implements Runnable {

        private final Runnable delegate;
        private final ThreadNameFunction nameFunction;

        NamingRunnable(Runnable delegate, ThreadNameFunction nameFunction) {
            this.delegate = delegate;
            this.nameFunction = nameFunction;
        }

        @Override
        public void run() {
            Thread current = Thread.currentThread();
            String originalName = current.getName();
            current.setName(nameFunction.rename(originalName));
            try {
                delegate.run();
            } finally {
                current.setName(originalName);
            }
        }
    }

    private static final class NamingCallable<T> implements Callable<T> {

        private final Callable<T> delegate;
        private final ThreadNameFunction nameFunction;

        NamingCallable(Callable<T> delegate, ThreadNameFunction nameFunction) {
            this.delegate = delegate;
            this.nameFunction = nameFunction;
        }

        @Override
        public T call() throws Exception {
            Thread current = Thread.currentThread();
            String originalName = current.getName();
            current.setName(nameFunction.rename(originalName));
            try {
                return delegate.call();
            } finally {
                current.setName(originalName);
            }
        }
    }

    interface ThreadNameFunction {
        String rename(String original);
    }

    static Builder builder() {
        return new Builder();
    }

    static final class Builder {

        @Nullable
        private ExecutorService executor;

        @Nullable
        private ThreadNameFunction nameFunction;

        private Builder() {}

        Builder executor(ExecutorService value) {
            this.executor = Preconditions.checkNotNull(value, "ExecutorService is required");
            return this;
        }

        Builder threadNameFunction(ThreadNameFunction value) {
            this.nameFunction = Preconditions.checkNotNull(value, "ThreadNameFunction is required");
            return this;
        }

        ExecutorService build() {
            return new ThreadNamingExecutorService(
                    Preconditions.checkNotNull(executor, "ExecutorService is required"),
                    Preconditions.checkNotNull(nameFunction, "ThreadNameFunction is required"));
        }
    }
}
