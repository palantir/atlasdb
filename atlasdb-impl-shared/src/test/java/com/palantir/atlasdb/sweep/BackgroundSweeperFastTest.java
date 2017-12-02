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
import static org.mockito.Matchers.eq;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.metrics.SweepMetricsManager;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;

public class BackgroundSweeperFastTest extends SweeperTestSetup {

    @Test
    public void testWritePriorityAfterCompleteFreshRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
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
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(9999L)
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
    public void testWriteProgressAfterIncompleteRunUsesSystemTimeForStart() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
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
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
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
    public void testPutZeroWriteCountAfterFreshIncompleteRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
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

        SweepResults intermediateResults = ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(Optional.of(new byte[] {1, 2, 3}))
                .timeInMillis(10L)
                .timeSweepStarted(20L)
                .build();

        setupTaskRunner(intermediateResults);
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetricsManager).updateMetrics(intermediateResults, TABLE_REF, SweepMetricsManager.LogEvent.ONE_ITERATION);
        Mockito.verify(sweepMetricsManager, Mockito.never()).updateMetrics(
                any(SweepResults.class), any(TableReference.class), eq(SweepMetricsManager.LogEvent.FULL_TABLE));
    }

    @Test
    public void testRecordFinalBatchMetricsAfterCompleteRun() {
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

        SweepResults intermediateResults = ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(20L)
                .timeSweepStarted(50L)
                .build();
        SweepResults fullResults = ImmutableSweepResults.builder()
                .staleValuesDeleted(5)
                .cellTsPairsExamined(21)
                .sweptTimestamp(4567L)
                .timeInMillis(30L)
                .timeSweepStarted(20L)
                .build();

        setupTaskRunner(intermediateResults);
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetricsManager).updateMetrics(intermediateResults, TABLE_REF, SweepMetricsManager.LogEvent.ONE_ITERATION);
        Mockito.verify(sweepMetricsManager).updateMetrics(fullResults, TABLE_REF, SweepMetricsManager.LogEvent.FULL_TABLE);
    }

    @Test
    public void testCompactInternallyAfterCompleteRunIfNonZeroDeletes() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(1)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .timeInMillis(0L)
                .timeSweepStarted(0L)
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
                .timeSweepStarted(0L)
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(kvs, Mockito.never()).compactInternally(TABLE_REF);
    }
}
