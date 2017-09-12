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

package com.palantir.paxos;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

/**
 * A supplier that coalesces computation requests, such that only one computation is ever running at a time, and
 * concurrent requests will result in a single computation. Computations are guaranteed to execute after being
 * requested; requests will not receive results for computations that started prior to the request.
 */
class CoalescingSupplier<T> implements Supplier<T> {

    private final Supplier<T> delegate;
    private volatile CompletableFuture<T> nextResult = new CompletableFuture<T>();
    private final Lock fairLock = new ReentrantLock(true);

    public CoalescingSupplier(Supplier<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T get() {
        CompletableFuture<T> future = nextResult;

        completeOrWaitForCompletion(future);

        return getResult(future);
    }

    private void completeOrWaitForCompletion(CompletableFuture<T> future) {
        fairLock.lock();
        try {
            resetAndCompleteIfNotCompleted(future);
        } finally {
            fairLock.unlock();
        }
    }

    private void resetAndCompleteIfNotCompleted(CompletableFuture<T> future) {
        if (future.isDone()) {
            return;
        }

        nextResult = new CompletableFuture<T>();
        try {
            future.complete(delegate.get());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    private T getResult(CompletableFuture<T> future) {
        try {
            return future.getNow(null);
        } catch (CompletionException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

}
