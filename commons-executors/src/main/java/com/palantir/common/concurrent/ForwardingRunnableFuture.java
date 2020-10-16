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

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

public class ForwardingRunnableFuture<V> extends ForwardingFuture<V> implements RunnableFuture<V> {
    private final Runnable runnable;
    private final Future<V> future;

    public ForwardingRunnableFuture(RunnableFuture<V> runnableFuture) {
        runnable = runnableFuture;
        future = runnableFuture;
    }

    public ForwardingRunnableFuture(FutureTask<V> futureTask) {
        runnable = futureTask;
        future = futureTask;
    }

    public ForwardingRunnableFuture(Runnable runnable, Future<V> future) {
        this.runnable = runnable;
        this.future = future;
    }

    public ForwardingRunnableFuture(Future<V> future) {
        if (!(future instanceof Runnable)) {
            throw new IllegalArgumentException();
        }
        runnable = (Runnable) future;
        this.future = future;
    }

    public static ForwardingRunnableFuture<?> of(Runnable runnable) {
        if (!(runnable instanceof Future<?>)) {
            throw new IllegalArgumentException();
        }
        @SuppressWarnings("unchecked")
        Future<Object> unsafeFuture = (Future<Object>) runnable;
        return new ForwardingRunnableFuture<Object>(unsafeFuture);
    }

    @Override
    public void run() {
        runnable.run();
    }

    @Override
    protected Future<V> delegate() {
        return future;
    }
}
