/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link AbstractForwardingScheduledExecutorService} except this implementation allows
 * implementors to wrap one-shot and recurring tasks differently. Consider tracing, for example,
 * where we want to propagate trace-id to executors, we generally do not want recurring tasks
 * to retain information from the originating thread.
 *
 * Intentionally package private, not meant for consumption outside of PTExecutors.
 */
abstract class InternalForwardingScheduledExecutorService extends AbstractExecutorService
        implements ScheduledExecutorService {

    protected abstract ScheduledExecutorService delegate();

    protected abstract Runnable wrap(Runnable runnable);

    protected abstract <T> Callable<T> wrap(Callable<T> callable);

    protected abstract Runnable wrapRecurring(Runnable runnable);

    @Override
    public void shutdown() {
        delegate().shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate().shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate().isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate().isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate().awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate().execute(wrap(command));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate().schedule(wrap(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return delegate().schedule(wrap(callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return delegate().scheduleAtFixedRate(wrapRecurring(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return delegate().scheduleWithFixedDelay(wrapRecurring(command), initialDelay, delay, unit);
    }
}
