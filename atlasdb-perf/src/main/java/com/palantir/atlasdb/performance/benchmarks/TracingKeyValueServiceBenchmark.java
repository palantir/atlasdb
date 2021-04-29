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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tracing.AlwaysSampler;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.RandomSampler;
import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import java.util.Collection;
import java.util.Collections;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
public class TracingKeyValueServiceBenchmark {

    @State(Scope.Thread)
    public static class MyState {
        public TableReference tableReference = TableReference.createFromFullyQualifiedName("hello.table");
        public Collection<String> cells = Collections.singletonList("hello");
    }

    @SuppressWarnings("ImmutableEnumChecker")
    public enum BenchmarkObservability {
        SAMPLE(AlwaysSampler.INSTANCE),
        DO_NOT_SAMPLE(() -> false),
        UNDECIDED(RandomSampler.create(0.01f));

        private final TraceSampler traceSampler;

        BenchmarkObservability(TraceSampler traceSampler) {
            this.traceSampler = traceSampler;
        }

        public TraceSampler getTraceSampler() {
            return traceSampler;
        }
    }

    @Param({"UNDECIDED"})
    public BenchmarkObservability observability;

    @Setup
    public final void before(Blackhole blackhole) {
        Tracer.setSampler(observability.getTraceSampler());
        Tracer.subscribe("jmh", blackhole::consume);
        // clear any existing trace to make sure this sampler is used
        Tracer.getAndClearTrace();
    }

    @TearDown
    public final void after() {
        Tracer.unsubscribe("jmh");
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 180)
    public int benchmarkTracing(MyState myState) {
        try (CloseableTracer trace =
                TracingKeyValueService.startLocalTrace("addGarbageCollectionSentinelValues", sink -> {
                    sink.accept(
                            "table",
                            LoggingArgs.safeTableOrPlaceholder(myState.tableReference)
                                    .toString());
                    sink.accept("numCells", Integer.toString(Iterables.size(myState.cells)));
                })) {
            return myState.cells.size();
        }
    }

    public static void main(String[] _args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(TracingKeyValueServiceBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .jvmArgs(
                        "-server",
                        "-XX:+UnlockDiagnosticVMOptions",
                        "-XX:PrintAssemblyOptions=intel",
                        "-XX:CompileCommand=print,*TracingKeyValueServiceBenchmark.benchmark*")
                .forks(1)
                .threads(4)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(3))
                .build();
        new Runner(opt).run();
    }
}
