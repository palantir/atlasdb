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

package com.palantir.atlasdb.performance.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.palantir.common.concurrent.CoalescingSupplier;

@State(Scope.Benchmark)
public class CoalescingSupplierBenchmark {
    private final Supplier<String> supplier = new CoalescingSupplier<>(() -> {
        try {
            Thread.sleep(2);
            return "result";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    });

    @Benchmark
    @Group("parallel")
    @GroupThreads(16)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS, iterations = 4)
    @Measurement(time = 1, timeUnit = TimeUnit.SECONDS, iterations = 10)
    @Fork(1)
    public String benchmark() throws InterruptedException {
        return supplier.get();
    }

    @Benchmark
    @Group("parallel")
    @GroupThreads(6)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS, iterations = 4)
    @Measurement(time = 1, timeUnit = TimeUnit.SECONDS, iterations = 10)
    @Fork(1)
    public int useCpu() {
        return 58 * 102;
    }

}
