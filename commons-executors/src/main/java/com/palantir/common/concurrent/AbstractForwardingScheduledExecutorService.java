/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class AbstractForwardingScheduledExecutorService extends AbstractExecutorService
        implements ScheduledExecutorService {

    protected abstract ScheduledExecutorService delegate();

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
        delegate().execute(PTExecutors.wrap(command));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate().schedule(PTExecutors.wrap(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return delegate().schedule(PTExecutors.wrap(callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
            TimeUnit unit) {
        return delegate().scheduleAtFixedRate(PTExecutors.wrap(command), initialDelay, period,
                unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
            long delay, TimeUnit unit) {
        return delegate().scheduleWithFixedDelay(PTExecutors.wrap(command), initialDelay, delay,
                unit);
    }
}
