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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

/**
 * A supplier that batches computation requests, such that only N computations are ever running at a time. Computations
 * are guaranteed to execute after being requested; requests will not receive results for computations that started
 * prior to the request.
 *
 * N is determined by the number of threads in the provided executor.
 */
class BatchingSupplier<T> implements Supplier<Future<T>> {

    private final Supplier<T> delegate;
    private final AtomicReference<CompletableFuture<T>> nextResult = new AtomicReference<>(new CompletableFuture<T>());
    private final ExecutorService executor;

    public BatchingSupplier(Supplier<T> delegate, ExecutorService executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @Override
    public Future<T> get() {
        CompletableFuture<T> future = nextResult.get();

        executor.submit(() -> maybeStartNextComputationFor(future));

        return future;
    }

    private void maybeStartNextComputationFor(CompletableFuture<T> future) {
        if (tryTakeForCompleting(future)) {
            complete(future);
        }
    }

    private boolean tryTakeForCompleting(CompletableFuture<T> future) {
        return nextResult.compareAndSet(future, new CompletableFuture<>());
    }

    private void complete(CompletableFuture<T> future) {
        try {
            future.complete(delegate.get());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    private T getUnchecked(CompletableFuture<T> result) {
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
