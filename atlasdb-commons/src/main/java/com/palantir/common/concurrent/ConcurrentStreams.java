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

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;

public class ConcurrentStreams {

    private ConcurrentStreams() {}

    /**
     * Runs a map function over all elements in an iterable with a provided executor and concurrency level.
     *
     * @param values The elements to be mapped.
     * @param mapper The function that maps the elements.
     * @param executor The executor that runs the concurrent operations.
     * @param concurrency The max number of operations to be submitted to the executor at a time. Note that this will
     *        not check the size of the underlying executor, so ideally the executor should have at least
     *        as many threads as this value.
     * @param timeout The max time to wait on submitting an element to the executor when {@param concurrency}
     *        operations are already running.
     * @return a stream of mapped elements from the provided iterable.
     */
    public static <T, S> Stream<S> map(
            Iterable<T> values, Function<T, S> mapper, ExecutorService executor, int concurrency, Duration timeout) {

        int size = Iterables.size(values);
        if (size == 1 || concurrency == 1) {
            return StreamSupport.stream(values.spliterator(), false).map(mapper);
        }
        if (size > concurrency) {
            return map(values, mapper, RequestLimitedExecutorService.fromDelegate(executor, concurrency, timeout));
        } else {
            return map(values, mapper, executor);
        }
    }

    private static <T, S> Stream<S> map(Iterable<T> values, Function<T, S> mapper, final ExecutorService executor) {
        return StreamSupport.stream(values.spliterator(), false)
                .map(value -> executor.submit(() -> mapper.apply(value)))
                // Collect first to ensure the maps aren't chained since we want to submit all futures before blocking.
                .collect(Collectors.toList())
                .stream()
                .map(ConcurrentStreams::getFromFuture);
    }

    private static <T> T getFromFuture(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
