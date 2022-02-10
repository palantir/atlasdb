/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A supplier of ScheduledExecutorService that can be initialized with the desired number of threads at runtime. Must
 * be initialized before it is first used, and once initialized, further calls to the {@link #initialize(int)} method
 * are ignored.
 */
public final class InitializeableScheduledExecutorServiceSupplier implements Supplier<ScheduledExecutorService> {
    private final ThreadFactory threadFactory;
    private final AtomicReference<ScheduledExecutorService> delegate = new AtomicReference<>();

    public InitializeableScheduledExecutorServiceSupplier(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    private InitializeableScheduledExecutorServiceSupplier(ScheduledExecutorService executor) {
        threadFactory = null;
        delegate.set(executor);
    }

    public static InitializeableScheduledExecutorServiceSupplier createForTests(ScheduledExecutorService executor) {
        return new InitializeableScheduledExecutorServiceSupplier(executor);
    }

    public void initialize(int numThreads) {
        delegate.compareAndSet(null, PTExecutors.newScheduledThreadPool(numThreads, threadFactory));
    }

    @Override
    public ScheduledExecutorService get() {
        Preconditions.checkState(delegate.get() != null, "Executor must be initialized");
        return delegate.get();
    }
}
