/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.Test;

public class SweepBatchAccumulatorTest {
    private static final long SWEEP_TIMESTAMP = 3141592L;
    private static final long PROGRESS_TIMESTAMP = 16180L;
    private static final TableReference TABLE_REFERENCE_1 = TableReference.createWithEmptyNamespace("table");
    private static final Cell CELL_1 = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("reihe"), PtBytes.toBytes("kolumne"));
    private static final WriteInfo WRITE_INFO_1 = WriteInfo.write(TABLE_REFERENCE_1, CELL_1, PROGRESS_TIMESTAMP + 100);
    private static final WriteInfo WRITE_INFO_2 = WriteInfo.write(TABLE_REFERENCE_1, CELL_2, PROGRESS_TIMESTAMP + 200);

    private static final DedicatedRows NO_DEDICATED_ROWS = DedicatedRows.of(ImmutableList.of());
    private static final SweepableCellsTable.SweepableCellsRow SWEEPABLE_CELLS_ROW_1
            = SweepableCellsTable.SweepableCellsRow.of(1, PtBytes.toBytes("metadata"));
    private static final SweepableCellsTable.SweepableCellsRow SWEEPABLE_CELLS_ROW_2
            = SweepableCellsTable.SweepableCellsRow.of(2, PtBytes.toBytes("more metadata"));
    private static final DedicatedRows DEDICATED_ROWS_1 = DedicatedRows.of(ImmutableList.of(SWEEPABLE_CELLS_ROW_1));
    private static final DedicatedRows DEDICATED_ROWS_2 = DedicatedRows.of(ImmutableList.of(SWEEPABLE_CELLS_ROW_2));

    private SweepBatchAccumulator accumulator;

    @Before
    public void setUp() {
        accumulator = new SweepBatchAccumulator(SWEEP_TIMESTAMP, PROGRESS_TIMESTAMP);
    }

    @Test
    public void noBatchesMeansEverythingSweptUpToTheSweepTimestamp() {
        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();

        assertThat(batchWithPartitionInfo.sweepBatch()).satisfies(batch -> {
            assertThat(batch.writes()).isEmpty();
            assertThat(batch.dedicatedRows()).isEqualTo(NO_DEDICATED_ROWS);
            assertThat(batch.lastSweptTimestamp()).isEqualTo(SWEEP_TIMESTAMP - 1);
        });
        assertThat(batchWithPartitionInfo.finePartitions()).isEmpty();
    }

    @Test
    public void oneBatchAccumulatesToItselfAndLogsPartition() {
        SweepBatch sweepBatch = SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                DEDICATED_ROWS_1,
                PROGRESS_TIMESTAMP + 200);
        accumulator.accumulateBatch(sweepBatch);

        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();
        assertThat(batchWithPartitionInfo.sweepBatch()).isEqualTo(sweepBatch);
        assertThat(batchWithPartitionInfo.finePartitions())
                .containsExactly(SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 200));
    }

    @Test
    public void mergesTwoBatchesWithDistinctWriteInfo() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                NO_DEDICATED_ROWS,
                PROGRESS_TIMESTAMP + 177));
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_2),
                NO_DEDICATED_ROWS,
                PROGRESS_TIMESTAMP + 288));

        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();
        assertThat(batchWithPartitionInfo.sweepBatch()).satisfies(batch -> {
            assertThat(batch.writes()).containsExactlyInAnyOrder(WRITE_INFO_1, WRITE_INFO_2);
            assertThat(batch.dedicatedRows()).isEqualTo(NO_DEDICATED_ROWS);
            assertThat(batch.lastSweptTimestamp()).isEqualTo(PROGRESS_TIMESTAMP + 288);
        });
        assertThat(batchWithPartitionInfo.finePartitions()).isEqualTo(ImmutableSet.of(
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 177),
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 288)));
    }

    @Test
    public void mergesDedicatedRows() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(),
                DEDICATED_ROWS_1,
                PROGRESS_TIMESTAMP + 177));
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(),
                DEDICATED_ROWS_2,
                PROGRESS_TIMESTAMP + 288));

        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();
        assertThat(batchWithPartitionInfo.sweepBatch()).satisfies(batch -> {
            assertThat(batch.dedicatedRows()).isEqualTo(DedicatedRows.of(
                    ImmutableList.of(SWEEPABLE_CELLS_ROW_1, SWEEPABLE_CELLS_ROW_2)));
            assertThat(batch.lastSweptTimestamp()).isEqualTo(PROGRESS_TIMESTAMP + 288);
        });
        assertThat(batchWithPartitionInfo.finePartitions()).isEqualTo(ImmutableSet.of(
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 177),
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 288)));
    }

    @Test
    public void mergesFinePartitionsFromWritesAndDedicatedRows() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                DedicatedRows.of(
                        ImmutableList.of(
                                SweepableCellsTable.SweepableCellsRow.of(
                                        SweepQueueUtils.minTsForFinePartition(1), PtBytes.toBytes("aaaaaaa")))),
                SweepQueueUtils.minTsForFinePartition(2)));

        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();
        assertThat(batchWithPartitionInfo.finePartitions())
                .hasSize(2)
                .isEqualTo(ImmutableSet.of(SweepQueueUtils.tsPartitionFine(WRITE_INFO_1.timestamp()), 1L));
    }

    @Test
    public void onlyKeepsNewestVersionOfWriteInfoWhenMergingMultipleBatches() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                NO_DEDICATED_ROWS,
                PROGRESS_TIMESTAMP + 177));
        WriteInfo writeInfo1AtNewerVersion = WriteInfo.write(TABLE_REFERENCE_1, CELL_1,
                PROGRESS_TIMESTAMP + 100 + SweepQueueUtils.TS_FINE_GRANULARITY);
        long newSweepTimestamp = PROGRESS_TIMESTAMP + 288 + SweepQueueUtils.minTsForFinePartition(1);
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(writeInfo1AtNewerVersion),
                NO_DEDICATED_ROWS,
                newSweepTimestamp));

        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();
        assertThat(batchWithPartitionInfo.sweepBatch()).satisfies(batch -> {
            assertThat(batch.writes()).containsExactlyInAnyOrder(writeInfo1AtNewerVersion);
            assertThat(batch.dedicatedRows()).isEqualTo(NO_DEDICATED_ROWS);
            assertThat(batch.lastSweptTimestamp()).isEqualTo(newSweepTimestamp);
        });

        // Both must still be present here!
        assertThat(batchWithPartitionInfo.finePartitions()).isEqualTo(ImmutableSet.of(
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 177),
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 100 + SweepQueueUtils.minTsForFinePartition(1))));
    }

    @Test
    public void correctlySkipsPartitionsWhenMergingMultipleBatches() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                NO_DEDICATED_ROWS,
                PROGRESS_TIMESTAMP + 177));
        WriteInfo writeInfo1AtNewerVersion = WriteInfo.write(TABLE_REFERENCE_1, CELL_1,
                PROGRESS_TIMESTAMP + 100 + SweepQueueUtils.minTsForFinePartition(9));
        long newSweepTimestamp = PROGRESS_TIMESTAMP + 288 + SweepQueueUtils.minTsForFinePartition(15);
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(writeInfo1AtNewerVersion),
                NO_DEDICATED_ROWS,
                newSweepTimestamp));

        SweepBatchWithPartitionInfo batchWithPartitionInfo = accumulator.toSweepBatch();
        assertThat(batchWithPartitionInfo.sweepBatch()).satisfies(batch -> {
            assertThat(batch.writes()).containsExactlyInAnyOrder(writeInfo1AtNewerVersion);
            assertThat(batch.dedicatedRows()).isEqualTo(NO_DEDICATED_ROWS);
            assertThat(batch.lastSweptTimestamp()).isEqualTo(newSweepTimestamp);
        });

        assertThat(batchWithPartitionInfo.finePartitions()).isEqualTo(ImmutableSet.of(
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 177),
                SweepQueueUtils.tsPartitionFine(PROGRESS_TIMESTAMP + 100 + SweepQueueUtils.minTsForFinePartition(9))));
    }

    @Test
    public void throwsWhenAttemptingToMergeWriteInfoAtTheSweepTimestamp() {
        assertThatThrownBy(() -> accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WriteInfo.write(TABLE_REFERENCE_1, CELL_1, PROGRESS_TIMESTAMP + 5)),
                NO_DEDICATED_ROWS,
                SWEEP_TIMESTAMP)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tried to accumulate a batch")
                .hasMessageContaining("went beyond the sweep timestamp");
    }

    @Test
    public void throwsWhenAttemptingToMergeWriteInfoAfterTheSweepTimestamp() {
        assertThatThrownBy(() -> accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WriteInfo.write(TABLE_REFERENCE_1, CELL_1, PROGRESS_TIMESTAMP + 5)),
                NO_DEDICATED_ROWS,
                SWEEP_TIMESTAMP + 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tried to accumulate a batch")
                .hasMessageContaining("went beyond the sweep timestamp");
    }

    @Test
    public void initiallyAcceptsAdditionalBatches() {
        assertThat(accumulator.shouldAcceptAdditionalBatch()).isTrue();
    }

    @Test
    public void acceptsAdditionalBatchesAfterOneOneElementBatch() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                NO_DEDICATED_ROWS,
                PROGRESS_TIMESTAMP + 177));
        assertThat(accumulator.shouldAcceptAdditionalBatch()).isTrue();
    }

    @Test
    public void rejectsBatchesOnceLimitReached() {
        List<WriteInfo> writeInfos = LongStream.range(0, SweepQueueUtils.SWEEP_BATCH_SIZE)
                .mapToObj(index -> WriteInfo.write(
                        TABLE_REFERENCE_1, Cell.create(PtBytes.toBytes(index), PtBytes.toBytes(index)), index))
                .collect(Collectors.toList());
        accumulator.accumulateBatch(SweepBatch.of(
                writeInfos,
                NO_DEDICATED_ROWS,
                PROGRESS_TIMESTAMP + 177));
        assertThat(accumulator.shouldAcceptAdditionalBatch()).isFalse();
    }

    @Test
    public void rejectsBatchesOnceSweepTimestampIsReached() {
        accumulator.accumulateBatch(SweepBatch.of(
                ImmutableList.of(WRITE_INFO_1),
                NO_DEDICATED_ROWS,
                SWEEP_TIMESTAMP - 1));
        assertThat(accumulator.shouldAcceptAdditionalBatch()).isFalse();
    }
}
