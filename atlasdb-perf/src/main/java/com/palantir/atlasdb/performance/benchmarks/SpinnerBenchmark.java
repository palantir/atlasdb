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

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.Autobatchers.SupplierKey;
import com.palantir.atlasdb.autobatch.CoalescingRequestSupplier;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.concurrent.CoalescingSupplier;

@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.Throughput)
@Threads(256)
@Fork(value = 1)
public class SpinnerBenchmark {
    private final Supplier<Long> delegate = () -> {
        try {
            Thread.sleep(1);
            return ThreadLocalRandom.current().nextLong();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    };
    private final Spinner<Long> spinner = new Spinner<>(delegate);
    private final CoalescingSupplier<Long> supplier = new CoalescingSupplier<>(delegate);
    private final DisruptorAutobatcher<SupplierKey, Long> disruptor =
            Autobatchers.coalescing(new CoalescingRequestSupplier<>(delegate)).safeLoggablePurpose("").build();
    private final Supplier<Long> disruptorSupplier = () -> Futures.getUnchecked(disruptor.apply(SupplierKey.INSTANCE));
    private long state = 0;

    @TearDown
    public void tearDown() {
        spinner.close();
        disruptor.close();
    }

    @Benchmark
    public long testDisruptor() {
        return disruptorSupplier.get();
    }

    @Benchmark
    public long testSpinner() {
        return spinner.get();
    }

    @Benchmark
    public long testSupplier() {
        return supplier.get();
    }

    @Benchmark
    public synchronized long testLock() throws InterruptedException {
        this.notifyAll();
        this.wait(10);
        return state++;
    }

    public static void main(String[] args) throws RunnerException {
        Runner runner = new Runner(new OptionsBuilder().include(SpinnerBenchmark.class.getName()).build());
        runner.run();
    }
}
