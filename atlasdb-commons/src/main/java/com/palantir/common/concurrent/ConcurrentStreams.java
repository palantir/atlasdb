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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ConcurrentStreams {

    private ConcurrentStreams() {}

    public static <T, S> Stream<S> map(
            Iterable<T> values, Function<T, S> mapper, ExecutorService executor, int concurrency, Duration timeout) {

        int size = Iterables.size(values);
        if (size == 1 || concurrency == 1) {
            return StreamSupport.stream(values.spliterator(), false).map(mapper);
        }
        if (size > concurrency) {
            executor = RequestLimitedExecutorService.fromDelegate(executor, concurrency, timeout);
        }

        List<Future<S>> futures = Lists.newArrayListWithCapacity(size);
        for (T value : values) {
            futures.add(executor.submit(() -> mapper.apply(value)));
        }

        return futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
