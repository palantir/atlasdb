/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.performance.benchmarks.table.RegeneratingTable;
import com.palantir.atlasdb.sweep.ImmutableSweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class SweepBenchmarks {

    private static final int BATCH_SIZE = 10;
    private static final int DELETED_COUNT = RegeneratingTable.SWEEP_DUPLICATES - 1;

    private Object runSingleSweep(RegeneratingTable table, int uniqueCellsToSweep) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepBatchConfig batchConfig = ImmutableSweepBatchConfig.builder()
                .deleteBatchSize(DELETED_COUNT * uniqueCellsToSweep)
                .candidateBatchSize(RegeneratingTable.SWEEP_DUPLICATES * uniqueCellsToSweep + 1)
                .maxCellTsPairsToExamine(RegeneratingTable.SWEEP_DUPLICATES * uniqueCellsToSweep)
                .build();
        SweepResults sweepResults = sweepTaskRunner.run(table.getTableRef(), batchConfig, PtBytes.EMPTY_BYTE_ARRAY);
        assertThat(sweepResults.getStaleValuesDeleted(), is((long) DELETED_COUNT * uniqueCellsToSweep));
        return sweepResults;
    }

    private Object runMultiSweep(RegeneratingTable table) {
        SweepTaskRunner sweepTaskRunner = table.getSweepTaskRunner();
        SweepResults sweepResults = null;
        byte[] nextStartRow = PtBytes.EMPTY_BYTE_ARRAY;
        for (int i = 0; i < BATCH_SIZE; i++) {
            SweepBatchConfig batchConfig = ImmutableSweepBatchConfig.builder()
                    .deleteBatchSize(DELETED_COUNT)
                    .candidateBatchSize(1)
                    .maxCellTsPairsToExamine(RegeneratingTable.SWEEP_DUPLICATES)
                    .build();
            sweepResults = sweepTaskRunner.run(table.getTableRef(), batchConfig, nextStartRow);
            nextStartRow = sweepResults.getNextStartRow().get();
            assertThat(sweepResults.getStaleValuesDeleted(), is((long) DELETED_COUNT));
        }
        return sweepResults;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object singleSweepRun(RegeneratingTable.SweepRegeneratingTable table) {
        return runSingleSweep(table, 1);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object batchedUniformSingleSweepRun(RegeneratingTable.SweepBatchUniformMultipleRegeneratingTable table) {
        return runSingleSweep(table, BATCH_SIZE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 75, timeUnit = TimeUnit.SECONDS)
    public Object batchedSingleSweepRun(RegeneratingTable.SweepBatchNonUniformMultipleSeparateRegeneratingTable table) {
        return runSingleSweep(table, BATCH_SIZE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 25, timeUnit = TimeUnit.SECONDS)
    public Object multipleUniformSweepRun(RegeneratingTable.SweepBatchUniformMultipleRegeneratingTable table) {
        return runMultiSweep(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 15, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 90, timeUnit = TimeUnit.SECONDS)
    public Object multipleSweepRun(RegeneratingTable.SweepBatchNonUniformMultipleSeparateRegeneratingTable table) {
        return runMultiSweep(table);
    }
}
