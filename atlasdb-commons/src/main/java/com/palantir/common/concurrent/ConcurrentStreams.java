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

package com.palantir.common.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.palantir.common.base.Throwables;

public class ConcurrentStreams {

    private ConcurrentStreams() {}

    private static final TaskCanceledException TASK_CANCELED_EXCEPTION = new TaskCanceledException();

    /**
     * Runs a map function over all elements in a list with a provided executor and concurrency level.
     *
     * @param values The elements to be mapped.
     * @param mapper The function that maps the elements.
     * @param executor The executor that runs the concurrent operations.
     * @param concurrency The max number of operations to be run in parallel. Note that this will
     *        not check the size of the underlying executor, so ideally the executor should have at least
     *        as many threads as this value.
     * @return a stream of mapped elements from the provided list.
     */
    public static <T, S> Stream<S> map(
            List<T> values, Function<T, S> mapper, Executor executor, int concurrency) {

        int size = values.size();
        if (size <= 1 || concurrency == 1) {
            return values.stream().map(mapper);
        }

        Map<T, CompletableFuture<S>> futuresByValue = values.stream()
                .collect(Collectors.toMap(Function.identity(), value -> new CompletableFuture<>()));
        Queue<T> valueQueue = new ConcurrentLinkedQueue<T>(values);

        if (size < concurrency) {
            concurrency = size;
        }
        for (int i = 0; i < concurrency; i++) {
            executor.execute(() -> runOperationsAndUpdateFutures(futuresByValue, valueQueue, mapper));
        }
        return streamAllUnchecked(futuresByValue.values());
    }

    private static <T, S> void runOperationsAndUpdateFutures(
            Map<T, CompletableFuture<S>> futuresByValue, Queue<T> valueQueue, Function<T, S> operation) {

        runUntilEmpty(valueQueue, value -> {
            CompletableFuture<S> future = futuresByValue.get(value);
            try {
                future.complete(operation.apply(value));
            } catch (Exception e) {
                future.completeExceptionally(e);
                completeAllExceptionally(futuresByValue, valueQueue);
            }
        });
    }

    private static <T, S> void completeAllExceptionally(
            Map<T, CompletableFuture<S>> futuresByValue, Queue<T> valueQueue) {
        runUntilEmpty(valueQueue, value -> futuresByValue.get(value).completeExceptionally(TASK_CANCELED_EXCEPTION));
    }

    private static <T> void runUntilEmpty(Queue<T> queue, Consumer<T> consumer) {
        for (T element = queue.poll(); element != null; element = queue.poll()) {
            consumer.accept(element);
        }
    }

    private static <S> Stream<S> streamAllUnchecked(Collection<CompletableFuture<S>> futures) {
        return futures.stream().map(future -> {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwables.throwIfUncheckedException(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        });
    }

    private static class TaskCanceledException extends RuntimeException {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    };

}
