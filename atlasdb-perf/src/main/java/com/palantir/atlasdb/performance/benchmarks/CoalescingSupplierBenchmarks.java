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

package com.palantir.atlasdb.performance.benchmarks;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.concurrent.CoalescingSupplier;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class CoalescingSupplierBenchmarks {
    private static final Map<Integer, CoalescingSupplier<Integer>> map = Maps.newConcurrentMap();
    // Atomic integer containing the next thread ID to be assigned
    private static final AtomicInteger nextId = new AtomicInteger(0);

    // Thread local variable containing each thread's ID
    private static final ThreadLocal<Integer> threadId =
            new ThreadLocal<Integer>() {
                @Override protected Integer initialValue() {
                    return nextId.getAndIncrement();
                }
            };

    @Benchmark
    @Threads(512)
    @Warmup(time = 5)
    @Measurement(time = 10)
    public int coalescing() {
        return AtlasFutures.getUnchecked(Futures.immediateFuture(getTimer(threadId.get()).get()));
    }

    private CoalescingSupplier<Integer> getTimer(Integer id) {
        return map.computeIfAbsent(id, u -> new CoalescingSupplier(() -> {
            Uninterruptibles.sleepUninterruptibly(Duration.ofNanos(1500000));
            return id;
        }));
    }
}
