/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;

import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.common.time.SystemClock;

public class BackgroundSweeperFastTest extends SweeperTestSetup {

    @Test
    public void testWritePriorityAfterCompleteFreshRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(2)
                        .newCellTsPairsExamined(10)
                        .newMinimumSweptTimestamp(12345L)
                        .newLastSweepTimeMillis(currentTimeMillis)
                        .newWriteCount(0L)
                        .build()));
    }

    @Test
    public void testWritePriorityAfterSecondRunCompletesSweep() {
        setProgress(ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(0L)
                .startTimeInMillis(0L)
                .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(9999L)
                .previousStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(5)
                        .newCellTsPairsExamined(21)
                        .newMinimumSweptTimestamp(4567L)
                        .newLastSweepTimeMillis(currentTimeMillis)
                        .build()));
    }

    @Test
    public void testWriteProgressAfterIncompleteRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();

        ArgumentCaptor<SweepProgress> argumentCaptor = ArgumentCaptor.forClass(SweepProgress.class);
        Mockito.verify(progressStore).saveProgress(argumentCaptor.capture());

        SweepProgress sweepProgress = argumentCaptor.getValue();
        long startTime = sweepProgress.startTimeInMillis();

        Assert.assertThat(new SystemClock().getTimeMillis() - startTime, Matchers.lessThan(1000L));

        Mockito.verify(progressStore).saveProgress(
                Mockito.eq(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(2)
                        .cellTsPairsExamined(10)
                        .minimumSweptTimestamp(12345L)
                        .startRow(new byte[] {1, 2, 3})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(0L)
                        .startTimeInMillis(startTime)
                        .build()));
    }

    @Test
    public void testWriteProgressAfterIncompleteRunWithPreviousProgress() {
        setProgress(ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(10L)
                .startTimeInMillis(0L)
                .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {4, 5, 6}))
                .timeInMillis(20L)
                .build());
        backgroundSweeper.runOnce();

        Mockito.verify(progressStore).saveProgress(
                Mockito.eq(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(5)
                        .cellTsPairsExamined(21)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {4, 5, 6})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(30L)
                        .startTimeInMillis(0L)
                        .build()));
    }

    @Test
    public void testPutZeroWriteCountAfterFreshIncompleteRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newWriteCount(0L)
                        .build()));
    }

    @Test
    public void testMetricsRecordedAfterIncompleteRunForOneIterationOnly() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetrics).deletedCellsOneIteration(2);
        Mockito.verify(sweepMetrics).examinedCellsOneIteration(10);
        Mockito.verify(sweepMetrics, Mockito.never()).deletedCellsFullTable(anyLong(), any(TableReference.class));
        Mockito.verify(sweepMetrics, Mockito.never()).examinedCellsFullTable(anyLong(), any(TableReference.class));
    }

    @Test
    public void testRecordFinalBatchMetricsAfterCompleteRun() {
        setProgress(
                ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(3)
                        .cellTsPairsExamined(11)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {1, 2, 3})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(0L)
                        .startTimeInMillis(0L)
                        .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetrics).deletedCellsOneIteration(2);
        Mockito.verify(sweepMetrics).examinedCellsOneIteration(10);
        Mockito.verify(sweepMetrics).deletedCellsFullTable(5, TABLE_REF);
        Mockito.verify(sweepMetrics).examinedCellsFullTable(21, TABLE_REF);
    }

    @Test
    public void testRecordTimeMetricsCorrectlyAfterCompleteRunWithNoPrevious() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(10_000L)
                .timeElapsedSinceStart(100_000L)
                .build());
        backgroundSweeper.runOnce();

        Mockito.verify(sweepMetrics).timeSweepingFullTable(10_000L, TABLE_REF);
        Mockito.verify(sweepMetrics).sweepTimeElapsedFullTable(10_000L, TABLE_REF);
    }

    @Test
    public void testRecordTimeMetricsCorrectlyAfterCompleteRun() {
        setProgress(
                ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(3)
                        .cellTsPairsExamined(11)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {1, 2, 3})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(50L)
                        .startTimeInMillis(new SystemClock().getTimeMillis())
                        .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(10_000L)
                .timeElapsedSinceStart(100_000L)
                .build());
        backgroundSweeper.runOnce();

        Mockito.verify(sweepMetrics).timeSweepingFullTable(10_050L, TABLE_REF);
        Mockito.verify(sweepMetrics).sweepTimeElapsedFullTable(longThat(Matchers.lessThan(1000L)), eq(TABLE_REF));
    }

    // todo(gmaretic): test that per table metrics are getting recorded as well

    @Test
    public void testCompactInternallyAfterCompleteRunIfNonZeroDeletes() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(1)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(kvs).compactInternally(TABLE_REF);
    }

    @Test
    public void testDontCompactInternallyAfterCompleteRunIfZeroDeletes() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(0)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(kvs, Mockito.never()).compactInternally(TABLE_REF);
    }
}
