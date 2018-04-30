/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.performance.benchmarks;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.palantir.atlasdb.performance.benchmarks.endpoint.TimestampServiceEndpoint;
import com.palantir.timestamp.TimestampRange;

public class TimestampServiceBenchmarks {
    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    public long fewThreadsGetFreshTimestamp(TimestampServiceEndpoint timestampService) {
        return timestampService.getFreshTimestamp();
    }

    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(64)
    public long manyThreadsGetFreshTimestamp(TimestampServiceEndpoint timestampService) {
        return timestampService.getFreshTimestamp();
    }

    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 120, timeUnit = TimeUnit.SECONDS)
    @Threads(64)
    public long manyThreadsGetFreshTimestampWithBackoff(TimestampServiceEndpoint timestampService) {
        Blackhole.consumeCPU(20000);
        return timestampService.getFreshTimestamp();
    }

    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    public TimestampRange fewThreadsGetBatchOfTimestamps(TimestampServiceEndpoint timestampService) {
        return timestampService.getFreshTimestamps(10000);
    }

    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(32)
    public TimestampRange manyThreadsGetBatchOfTimestamps(TimestampServiceEndpoint timestampService) {
        return timestampService.getFreshTimestamps(500);
    }
}
