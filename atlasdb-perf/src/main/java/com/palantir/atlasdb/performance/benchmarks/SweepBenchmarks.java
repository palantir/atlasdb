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

    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object singleSweepRun(RegeneratingTable.SweepRegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = sweepTaskRunner.run(table.getTableRef(), 1, null);
        assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT));
        return sweepResults;
    }

    @Benchmark
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Object batchedUniformSingleSweepRun(RegeneratingTable.SweepBatchUniformMultipleRegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = sweepTaskRunner.run(table.getTableRef(), BATCH_SIZE, null);
        assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT * BATCH_SIZE));
        return sweepResults;
    }

    @Benchmark
    @Warmup(time = 6, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
    public Object batchedSingleSweepRun(RegeneratingTable.SweepBatchNonUniformMultipleSeparateRegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = sweepTaskRunner.run(table.getTableRef(), BATCH_SIZE, null);
        assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT * BATCH_SIZE));
        return sweepResults;
    }

    @Benchmark
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object multipleUniformSweepRun(RegeneratingTable.SweepBatchUniformMultipleRegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = null;
        byte[] nextStartRow = null;
        for (int i = 0; i < BATCH_SIZE; i++) {
            sweepResults = sweepTaskRunner.run(table.getTableRef(), 1, nextStartRow);
            nextStartRow = sweepResults.getNextStartRow().get();
            assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT));
        }
        return sweepResults;
    }

    @Benchmark
    @Warmup(time = 8, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 40, timeUnit = TimeUnit.SECONDS)
    public Object multipleSweepRun(RegeneratingTable.SweepBatchNonUniformMultipleSeparateRegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = null;
        byte[] nextStartRow = null;
        for (int i = 0; i < BATCH_SIZE; i++) {
            sweepResults = sweepTaskRunner.run(table.getTableRef(), 1, nextStartRow);
            nextStartRow = sweepResults.getNextStartRow().get();
            assertThat(sweepResults.getCellsDeleted(), is(DELETED_COUNT));
        }
        return sweepResults;
    }
}
