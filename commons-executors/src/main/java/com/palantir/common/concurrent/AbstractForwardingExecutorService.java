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

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This delegates all the submit calls to the {@link #execute(Runnable)} call.  If you wish to
 * decorate submitted tasks, you only need to decorate the execute method.
 */
public abstract class AbstractForwardingExecutorService extends AbstractExecutorService {

    protected abstract ExecutorService delegate();

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate().awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate().execute(wrap(command));
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
    public void shutdown() {
        delegate().shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate().shutdownNow();
    }

    protected Runnable wrap(Runnable runnable) {
        return PTExecutors.wrap(runnable);
    }
}
