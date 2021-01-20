/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.logsafe.SafeArg;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterizedCoalescingSupplier<T> implements Supplier<T> {
    private static final Logger log = LoggerFactory.getLogger(ParameterizedCoalescingSupplier.class);

    private final Supplier<T> delegate;
    private final String namespace;
    private static final ListeningExecutorService executor =
            MoreExecutors.listeningDecorator(PTExecutors.newCachedThreadPool());

    private volatile NewRound round = new NewRound();

    public ParameterizedCoalescingSupplier(String namespace, Supplier<T> delegate) {
        this.namespace = namespace;
        this.delegate = delegate;
    }

    @Override
    public T get() {
        long startTime = System.nanoTime();
        NewRound present = round;
        if (present.isFirstToArrive()) {
            present.execute();
            T result = present.getResult();
            log.info(
                    "The start - {} and end times - {} of first to arrive request for namespace - {}",
                    SafeArg.of("startTime", startTime),
                    SafeArg.of("endTime", System.nanoTime()),
                    SafeArg.of("firstToArriveReqNamespace", namespace));
            return result;
        }
        NewRound next = present.awaitDone();
        if (next.isFirstToArrive()) {
            next.execute();
        }
        T result = next.getResult();
        log.info(
                "The start - {} and end times - {} of first to arrive request for namespace - {}",
                SafeArg.of("startTime", startTime),
                SafeArg.of("endTime", System.nanoTime()),
                SafeArg.of("nextArrivedReqNamespace", namespace));
        return result;
    }

    public ListenableFuture<T> getAsync() {
        return Futures.nonCancellationPropagating(getAsyncNotHandlingCancellation());
    }

    @SuppressWarnings("CheckReturnValue")
    private ListenableFuture<T> getAsyncNotHandlingCancellation() {
        NewRound present = round;
        if (present.isFirstToArrive()) {
            return Futures.submitAsync(
                    () -> {
                        present.execute();
                        return present.getResultAsync();
                    },
                    executor);
        }
        return Futures.transformAsync(
                present.done(),
                next -> {
                    if (next.isFirstToArrive()) {
                        executor.execute(next::execute);
                    }
                    return next.getResultAsync();
                },
                MoreExecutors.directExecutor());
    }

    private final class NewRound {
        private final AtomicBoolean hasStarted = new AtomicBoolean(false);
        private final SettableFuture<T> future = SettableFuture.create();
        private volatile NewRound next;

        boolean isFirstToArrive() {
            // adding the get benchmarks as faster, expected because compareAndSet forces an exclusive cache line
            return !hasStarted.get() && hasStarted.compareAndSet(false, true);
        }

        ListenableFuture<NewRound> done() {
            return FluentFuture.from(future)
                    .catching(Throwable.class, thrown -> null, MoreExecutors.directExecutor())
                    .transform(x -> next, MoreExecutors.directExecutor());
        }

        NewRound awaitDone() {
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
            next = new NewRound();
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
