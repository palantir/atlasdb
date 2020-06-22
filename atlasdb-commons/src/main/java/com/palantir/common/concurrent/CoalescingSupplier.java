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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A supplier that coalesces computation requests, such that only one computation is ever running at a time, and
 * concurrent requests will result in a single computation. Computations are guaranteed to execute after being
 * requested; requests will not receive results for computations that started prior to the request.
 */
public class CoalescingSupplier<T> implements Supplier<T> {
    private final Supplier<T> delegate;
    private static final ListeningExecutorService executor =
            MoreExecutors.listeningDecorator(PTExecutors.newCachedThreadPool());

    private volatile Round round = new Round();

    public CoalescingSupplier(Supplier<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T get() {
        Round present = round;
        if (present.isFirstToArrive()) {
            present.execute();
            return present.getResult();
        }
        Round next = present.awaitDone();
        if (next.isFirstToArrive()) {
            next.execute();
        }
        return next.getResult();
    }

    public ListenableFuture<T> getAsync() {
        return Futures.nonCancellationPropagating(getAsyncNotHandlingCancellation());
    }

    @SuppressWarnings("CheckReturnValue")
    private ListenableFuture<T> getAsyncNotHandlingCancellation() {
        Round present = round;
        if (present.isFirstToArrive()) {
            return Futures.submitAsync(() -> {
                present.execute();
                return present.getResultAsync();
            }, executor);
        }
        return Futures.transformAsync(present.done(), next -> {
            if (next.isFirstToArrive()) {
                executor.submit(next::execute);
            }
            return next.getResultAsync();
        }, MoreExecutors.directExecutor());
    }

    private final class Round {
        private final AtomicBoolean hasStarted = new AtomicBoolean(false);
        private final SettableFuture<T> future = SettableFuture.create();
        private volatile Round next;

        boolean isFirstToArrive() {
            // adding the get benchmarks as faster, expected because compareAndSet forces an exclusive cache line
            return !hasStarted.get() && hasStarted.compareAndSet(false, true);
        }

        ListenableFuture<Round> done() {
            return FluentFuture.from(future)
                    .catching(Throwable.class, thrown -> null, MoreExecutors.directExecutor())
                    .transform(x -> next, MoreExecutors.directExecutor());
        }

        Round awaitDone() {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                // ignore
            }
            return next;
        }

        // Why is this safe? There are two phases of computation
        void execute() {
            next = new Round();
            ListenableFuture<T> result = compute();
            round = next;
            future.setFuture(result);
        }

        private ListenableFuture<T> compute() {
            try {
                return Futures.immediateFuture(delegate.get());
            } catch (Throwable t) {
                return Futures.immediateFailedFuture(t);
            }
        }

        ListenableFuture<T> getResultAsync() {
            return future;
        }

        T getResult() {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
    }
}
