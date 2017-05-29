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

package com.palantir.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

/**
 * An executor that combines computation requests, such that only one computation is ever running at a time.
 * Computations are guaranteed to execute after being requested; requests will not receive results for computations that started
 * prior to the request.
 */
public class CoalescingExecutor<T> {

    private final Supplier<T> delegate;
    private final AtomicReference<CompletableFuture<T>> nextResult = new AtomicReference<>(new CompletableFuture<T>());
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public CoalescingExecutor(Supplier<T> delegate) {
        this.delegate = delegate;
    }

    public T getNextResult() {
        CompletableFuture<T> result = nextResult.get();

        executor.submit(() -> maybeComputeResult(result));

        return getResult(result);
    }

    private void maybeComputeResult(CompletableFuture<T> result) {
        if (tryStartNextComputation(result)) {
            computeResult(result);
        }
    }

    private boolean tryStartNextComputation(CompletableFuture<T> result) {
        return nextResult.compareAndSet(result, new CompletableFuture<T>());
    }

    private void computeResult(CompletableFuture<T> result) {
        try {
            result.complete(delegate.get());
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }

    private T getResult(CompletableFuture<T> result) {
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

}
