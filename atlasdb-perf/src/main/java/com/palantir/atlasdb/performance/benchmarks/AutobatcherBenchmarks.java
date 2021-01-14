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
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class AutobatcherBenchmarks {
    private static final DisruptorAutobatcher<Integer, Integer> autobatcher =
            Autobatchers.coalescing(new Handler())
                    .bufferSize(OptionalInt.of(512))
                    .safeLoggablePurpose("test")
                    .build();

    @Benchmark
    @Threads(256)
    @Warmup(time = 15)
    @Measurement(time = 15)
    public int coalescingBatching() {
        return AtlasFutures.getUnchecked(autobatcher.apply(1));
    }

    @State(Scope.Benchmark)
    private static class Handler implements CoalescingRequestFunction<Integer, Integer> {
        @Override
        public Map<Integer, Integer> apply(Set<Integer> request) {
            Map<Integer, Integer> map = Maps.newHashMapWithExpectedSize(request.size());
            request.forEach(num -> map.put(num, num));
            return map;
        }
    }
}
