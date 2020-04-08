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

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.Preconditions;

/** An ExecutorService decorator which updates names of threads while tasks run. */
final class ThreadNamingExecutorService extends AbstractExecutorService {
    private static final Logger log = LoggerFactory.getLogger(ThreadNamingExecutorService.class);
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
    public void execute(Runnable command) {
        delegate.execute(new NamingRunnable(command, nameFunction));
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
            } catch (Throwable t) {
                // The uncaught exception handler must be called while the thread has an updated name
                // The throwable must not be rethrown otherwise the uncaught exception handler would
                // be invoked twice.
                invokeUncaughtExceptionHandler(t);
            } finally {
                current.setName(originalName);
            }
        }
    }

    private static void invokeUncaughtExceptionHandler(Throwable throwable) {
        Thread current = Thread.currentThread();
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler = current.getUncaughtExceptionHandler();
        if (uncaughtExceptionHandler != null) {
            uncaughtExceptionHandler.uncaughtException(current, throwable);
        } else {
            log.warn("Caught and unhandled exception", throwable);
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
