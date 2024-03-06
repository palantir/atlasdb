/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.futures;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.base.Throwables;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tracing.DeferredTracer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AtlasFutures {
    private AtlasFutures() {}

    /**
     * Constructs a {@link FuturesCombiner} implementation which takes ownership of the {@code executorService} and
     * calls {@link ExecutorService#shutdown()} when close is called on it.
     *
     * @param executorService to be used to combine the futures
     * @return implementation of {@link FuturesCombiner}
     */
    public static FuturesCombiner futuresCombiner(ExecutorService executorService) {
        return new FuturesCombiner() {
            @Override
            public <T, R> ListenableFuture<Map<T, R>> allAsMap(
                    Map<T, ListenableFuture<Optional<R>>> inputToListenableFutureMap) {
                return AtlasFutures.allOptionalsAsMap(inputToListenableFutureMap, executorService);
            }

            @Override
            public void close() {
                executorService.shutdown();
            }
        };
    }

    /**
     * Creates a new {@code ListenableFuture} whose value is a map containing the values of all its
     * input futures, if all succeed. Input key-value pairs for which the input futures resolve to
     * {@link Optional#empty()} are filtered out.
     *
     * @param inputToListenableFutureMap query input to {@link ListenableFuture} of the query result
     * @param <T> type of query input
     * @param <R> type of query result
     * @return {@link ListenableFuture} of the combined map
     */
    public static <T, R> ListenableFuture<Map<T, R>> allOptionalsAsMap(
            Map<T, ListenableFuture<Optional<R>>> inputToListenableFutureMap, Executor executor) {
        Executor tracingExecutor = traceRestoringExecutor(executor, "AtlasFutures: allOptionalsAsMap");

        return Futures.whenAllSucceed(inputToListenableFutureMap.values())
                .call(
                        () -> KeyedStream.stream(inputToListenableFutureMap)
                                .map(AtlasFutures::getDone)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collectToMap(),
                        tracingExecutor);
    }

    public static <T, R> ListenableFuture<Map<T, R>> allAsMap(
            Map<T, ListenableFuture<R>> inputToListenableFutureMap, Executor executor) {
        Executor tracingExecutor = traceRestoringExecutor(executor, "AtlasFutures: allAsMap");

        return Futures.whenAllSucceed(inputToListenableFutureMap.values())
                .call(
                        () -> KeyedStream.stream(inputToListenableFutureMap)
                                .map(AtlasFutures::getDone)
                                .filter(Objects::nonNull)
                                .collectToMap(),
                        tracingExecutor);
    }

    public static <R> R getDone(ListenableFuture<R> resultFuture) {
        try {
            return Futures.getDone(resultFuture);
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    public static <R> R getUnchecked(Future<R> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        } catch (Exception e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public static <R> CompletableFuture<R> toCompletableFuture(ListenableFuture<R> listenableFuture) {
        CompletableFuture<R> completableFuture = new CompletableFuture<R>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean result = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };
        Futures.addCallback(
                listenableFuture,
                new FutureCallback<R>() {
                    @Override
                    public void onSuccess(R result) {
                        completableFuture.complete(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        completableFuture.completeExceptionally(t);
                    }
                },
                MoreExecutors.directExecutor());
        return completableFuture;
    }

    public static <R> ListenableFuture<R> toListenableFuture(CompletableFuture<R> completableFuture) {
        return new ListenableFuture<R>() {
            @Override
            public void addListener(Runnable listener, Executor executor) {
                completableFuture.thenRunAsync(listener, executor);
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return completableFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return completableFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return completableFuture.isDone();
            }

            @Override
            public R get() throws InterruptedException, ExecutionException {
                return completableFuture.get();
            }

            @Override
            public R get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                return completableFuture.get(timeout, unit);
            }
        };
    }

    private static Executor traceRestoringExecutor(Executor executor, String operation) {
        DeferredTracer deferredTracer = new DeferredTracer(operation);
        return command -> executor.execute(() -> deferredTracer.withTrace(() -> {
            command.run();
            return null;
        }));
    }
}
