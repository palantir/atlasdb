/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.palantir.atlasdb.performance.benchmarks;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.performance.benchmarks.table.RegeneratingTable;
import com.palantir.atlasdb.sweep.SweepTaskRunner;

@State(Scope.Benchmark)
public class SweepBenchmarks {

    private static final int BATCH_SIZE = 10;
    private static final long DELETED_COUNT = RegeneratingTable.SWEEP_DUPLICATES - 1L;

    private Object runSingleSweep(RegeneratingTable table, int batchSize) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = sweepTaskRunner.run(table.getTableRef(), batchSize, batchSize, null);
        assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT * batchSize));
        return sweepResults;
    }

    private Object runMultiSweep(RegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = null;
        byte[] nextStartRow = null;
        for (int i = 0; i < BATCH_SIZE; i++) {
            sweepResults = sweepTaskRunner.run(table.getTableRef(), 1, 1, nextStartRow);
            nextStartRow = sweepResults.getNextStartRow().get();
            assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT));
        }
        return sweepResults;
    }

    @Benchmark
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 100)
    public Object singleSweepRun(RegeneratingTable.SweepRegeneratingTable table) {
        return runSingleSweep(table, 1);
    }

    @Benchmark
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 100)
    public Object batchedUniformSingleSweepRun(RegeneratingTable.SweepBatchUniformMultipleRegeneratingTable table) {
        return runSingleSweep(table, BATCH_SIZE);
    }

    @Benchmark
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 100)
    public Object batchedSingleSweepRun(RegeneratingTable.SweepBatchNonUniformMultipleSeparateRegeneratingTable table) {
        return runSingleSweep(table, BATCH_SIZE);
    }

    @Benchmark
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 100)
    public Object multipleUniformSweepRun(RegeneratingTable.SweepBatchUniformMultipleRegeneratingTable table) {
        return runMultiSweep(table);
    }

    @Benchmark
    @Warmup(time = 15, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 100)
    public Object multipleSweepRun(RegeneratingTable.SweepBatchNonUniformMultipleSeparateRegeneratingTable table) {
        return runMultiSweep(table);
    }
}
