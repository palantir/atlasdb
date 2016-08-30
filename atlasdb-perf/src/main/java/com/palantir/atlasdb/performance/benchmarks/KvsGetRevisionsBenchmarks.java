/**
 * Copyright 2016 Palantir Technologies
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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class KvsGetRevisionsBenchmarks {

    @Benchmark
    public Multimap<Cell, Long> getAllTimetampsVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return table.getKvs().getAllTimestamps(table.TABLE_REF, Collections.singleton(table.getRandomCell()), Long.MAX_VALUE);
    }

    @Benchmark
    public Multimap<Cell, Long> getAllTimetampsClean(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return table.getKvs().getAllTimestamps(table.TABLE_REF, Collections.singleton(table.getRandomCell()), Long.MAX_VALUE);
    }
}
