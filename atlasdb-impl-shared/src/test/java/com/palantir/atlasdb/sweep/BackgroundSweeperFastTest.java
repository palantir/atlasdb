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
package com.palantir.atlasdb.sweep;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.sweep.priority.ImmutableSweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class BackgroundSweeperFastTest extends SweeperTestSetup {

    @Test
    public void testWritePriorityAfterCompleteFreshRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .timeInMillis(10L)
                .timeSweepStarted(20L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                any(),
                eq(TABLE_REF),
                eq(ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(2)
                        .newCellTsPairsExamined(10)
                        .newMinimumSweptTimestamp(12345L)
                        .newLastSweepTimeMillis(currentTimeMillis)
                        .newWriteCount(0L)
                        .build()));
    }

    @Test
    public void testWritePriorityAfterSecondRunCompletesSweep() {
        setNextTableToSweep(TABLE_REF);
        setProgress(ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(10L)
                .startTimeInMillis(20L)
                .build());
        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(9999L)
                .previousStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(0L)
                .timeSweepStarted(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                any(),
                eq(TABLE_REF),
                eq(ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(5)
                        .newCellTsPairsExamined(21)
                        .newMinimumSweptTimestamp(4567L)
                        .newLastSweepTimeMillis(currentTimeMillis)
                        .build()));
    }

    @Test
    public void testSecondRunOnSameTable() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);

        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(10L)
                .timeSweepStarted(50L)
                .build());
        backgroundSweeper.runOnce();

        ImmutableSweepProgress progressAfterFirstIteration = ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minimumSweptTimestamp(12345L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(10L)
                .startTimeInMillis(50L)
                .build();
        Mockito.verify(progressStore).saveProgress(
                eq(progressAfterFirstIteration));
        setProgress(progressAfterFirstIteration);

        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minSweptTimestamp(4567L)
                .nextStartRow(Optional.of(new byte[] {4, 5, 6}))
                .timeInMillis(20L)
                .timeSweepStarted(50L)
                .build());
        backgroundSweeper.runOnce();

        Mockito.verify(progressStore).saveProgress(
                eq(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(5)
                        .cellTsPairsExamined(21)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {4, 5, 6})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(30L)
                        .startTimeInMillis(50L)
                        .build()));
    }

    @Test
    public void testSecondRunMaySweepDifferentTable() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);

        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(10L)
                .timeSweepStarted(50L)
                .build());
        backgroundSweeper.runOnce();

        ImmutableSweepProgress progressAfterFirstIteration = ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minimumSweptTimestamp(12345L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(10L)
                .startTimeInMillis(50L)
                .build();
        Mockito.verify(progressStore).saveProgress(
                eq(progressAfterFirstIteration));
        setProgress(progressAfterFirstIteration);

        // Between iterations 1 and 2, OTHER_TABLE gets added to the priority list.
        setNoProgress(OTHER_TABLE);
        setupTaskRunner(OTHER_TABLE, SweepResults.builder()
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minSweptTimestamp(4567L)
                .nextStartRow(Optional.of(new byte[] {4, 5, 6}))
                .timeInMillis(20L)
                .timeSweepStarted(50L)
                .build());
        overrideConfig = ImmutableSweepPriorityOverrideConfig.builder()
                .addPriorityTables(OTHER_TABLE.getQualifiedName())
                .build();
        setNextTableToSweep(OTHER_TABLE);
        backgroundSweeper.runOnce();

        ImmutableSweepProgress progressAfterSecondIteration = ImmutableSweepProgress.builder()
                .tableRef(OTHER_TABLE)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {4, 5, 6})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(20L)
                .startTimeInMillis(50L)
                .build();
        Mockito.verify(progressStore).saveProgress(
                eq(progressAfterSecondIteration));
    }

    @Test
    public void testWriteProgressAfterIncompleteRunUsesSystemTimeForStart() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(10L)
                .timeSweepStarted(Long.MAX_VALUE)
                .build());
        backgroundSweeper.runOnce();

        ArgumentCaptor<SweepProgress> argumentCaptor = ArgumentCaptor.forClass(SweepProgress.class);
        Mockito.verify(progressStore).saveProgress(argumentCaptor.capture());

        SweepProgress savedProgress = argumentCaptor.getValue();
        long timeSweepStarted = savedProgress.startTimeInMillis();

        Assertions.assertThat(timeSweepStarted).isLessThanOrEqualTo(System.currentTimeMillis());
        Assertions.assertThat(timeSweepStarted).isGreaterThan(System.currentTimeMillis() - 1000L);

        Assertions.assertThat(savedProgress)
                .isEqualTo(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(2)
                        .cellTsPairsExamined(10)
                        .minimumSweptTimestamp(12345L)
                        .startRow(new byte[] {1, 2, 3})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(10L)
                        .startTimeInMillis(timeSweepStarted)
                        .build());
    }

    @Test
    public void testWriteProgressAfterIncompleteRunWithPreviousProgress() {
        setNextTableToSweep(TABLE_REF);
        setProgress(ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(10L)
                .startTimeInMillis(20L)
                .build());
        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {4, 5, 6}))
                .timeInMillis(20L)
                .timeSweepStarted(50L)
                .build());
        backgroundSweeper.runOnce();

        Mockito.verify(progressStore).saveProgress(
                eq(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(5)
                        .cellTsPairsExamined(21)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {4, 5, 6})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(30L)
                        .startTimeInMillis(20L)
                        .build()));
    }

    @Test
    public void testNewThreadSweepsNewTable() {
        int otherThreadIndex = THREAD_INDEX + 1;
        setProgress(ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {1, 2, 3})
                .startColumn(PtBytes.toBytes("unused"))
                .timeInMillis(10L)
                .startTimeInMillis(20L)
                .build());
        setNoProgress(OTHER_TABLE);
        setupTaskRunner(OTHER_TABLE, SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {4, 5, 6}))
                .timeInMillis(20L)
                .timeSweepStarted(50L)
                .build());
        setNextTableToSweep(OTHER_TABLE);

        BackgroundSweepThread otherThread = getBackgroundSweepThread(otherThreadIndex);
        otherThread.runOnce();

        Mockito.verify(progressStore).saveProgress(
                eq(ImmutableSweepProgress.builder()
                        .tableRef(OTHER_TABLE)
                        .staleValuesDeleted(2)
                        .cellTsPairsExamined(10)
                        .minimumSweptTimestamp(12345L)
                        .startRow(new byte[] {4, 5, 6})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(20L)
                        .startTimeInMillis(50L)
                        .build()));
    }

    @Test
    public void testPutZeroWriteCountAfterFreshIncompleteRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(10L)
                .timeSweepStarted(20L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                any(),
                eq(TABLE_REF),
                eq(ImmutableUpdateSweepPriority.builder()
                        .newWriteCount(0L)
                        .build()));
    }

    @Test
    public void testMetricsRecordedAfterIncompleteRunForOneIterationOnly() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);

        SweepResults intermediateResults = SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(10L)
                .timeSweepStarted(20L)
                .build();

        setupTaskRunner(intermediateResults);
        backgroundSweeper.runOnce();

        ArgumentCaptor<Long> sweepTime = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> totalTimeElapsed = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(sweepMetrics).updateSweepTime(
                sweepTime.capture(),
                totalTimeElapsed.capture());

        Assertions.assertThat(intermediateResults.getTimeInMillis()).isEqualTo(sweepTime.getValue());
        Assertions.assertThat(intermediateResults.getTimeElapsedSinceStartedSweeping())
                .isCloseTo(totalTimeElapsed.getValue(), Percentage.withPercentage(5d));
    }

    @Test
    public void testMetricsUseIntermediateResultsPerIteration() {
        setNextTableToSweep(TABLE_REF);
        setProgress(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(3)
                        .cellTsPairsExamined(11)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {1, 2, 3})
                        .startColumn(PtBytes.toBytes("unused"))
                        .timeInMillis(10L)
                        .startTimeInMillis(20L)
                        .build());

        SweepResults intermediateResults = SweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .minSweptTimestamp(12345L)
                .timeInMillis(20L)
                .timeSweepStarted(50L)
                .build();

        setupTaskRunner(intermediateResults);
        backgroundSweeper.runOnce();

        ArgumentCaptor<Long> sweepTime = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> totalTimeElapsed = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(sweepMetrics).updateSweepTime(
                sweepTime.capture(),
                totalTimeElapsed.capture());

        Assertions.assertThat(intermediateResults.getTimeInMillis()).isEqualTo(sweepTime.getValue());
        Assertions.assertThat(intermediateResults.getTimeElapsedSinceStartedSweeping())
                .isCloseTo(totalTimeElapsed.getValue(), Percentage.withPercentage(5d));
    }
}
