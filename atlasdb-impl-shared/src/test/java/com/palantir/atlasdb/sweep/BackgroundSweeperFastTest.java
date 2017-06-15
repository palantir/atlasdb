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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.lock.RemoteLockService;

public class BackgroundSweeperFastTest extends SweeperTestSetup {
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName(
            "backgroundsweeper.fasttest");

    protected SpecificTableSweeper specificTableSweeper;
    protected BackgroundSweeperImpl backgroundSweeper;
    private KeyValueService kvs = Mockito.mock(KeyValueService.class);
    private SweepProgressStore progressStore = Mockito.mock(SweepProgressStore.class);
    private SweepPriorityStore priorityStore = Mockito.mock(SweepPriorityStore.class);
    private NextTableToSweepProvider nextTableToSweepProvider = Mockito.mock(NextTableToSweepProvider.class);
    private SweepTaskRunner sweepTaskRunner = Mockito.mock(SweepTaskRunner.class);
    private boolean sweepEnabled = true;
    private SweepMetrics sweepMetrics = Mockito.mock(SweepMetrics.class);
    private long currentTimeMillis = 1000200300L;

    @Before
    public void setup() {
        ImmutableSweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
                .deleteBatchSize(100)
                .candidateBatchSize(200)
                .maxCellTsPairsToExamine(1000)
                .build();

        specificTableSweeper = new SpecificTableSweeper(
                SweeperTestSetup.mockTxManager(),
                kvs,
                sweepTaskRunner,
                () -> sweepBatchConfig,
                priorityStore,
                progressStore,
                Mockito.mock(BackgroundSweeperPerformanceLogger.class),
                sweepMetrics,
                () -> currentTimeMillis);
        backgroundSweeper = new BackgroundSweeperImpl(
                Mockito.mock(RemoteLockService.class),
                nextTableToSweepProvider,
                () -> sweepEnabled,
                () -> 0L, // pauseMillis
                Mockito.mock(PersistentLockManager.class),
                specificTableSweeper);
    }

    @Test
    public void testWritePriorityAfterCompleteFreshRun() {
        setNoProgress(progressStore);
        setNextTableToSweep(TABLE_REF, nextTableToSweepProvider);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                Mockito.any(),
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
        setProgress(progressStore, ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(new byte[] {1, 2, 3})
                .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(9999L)
                .previousStartRow(new byte[] {1, 2, 3})
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                Mockito.any(),
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
        setNoProgress(progressStore);
        setNextTableToSweep(TABLE_REF, nextTableToSweepProvider);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(new byte[] {1, 2, 3})
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(progressStore).saveProgress(
                Mockito.any(),
                Mockito.eq(ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(2)
                        .cellTsPairsExamined(10)
                        .minimumSweptTimestamp(12345L)
                        .startRow(new byte[] {1, 2, 3})
                        .build()));
    }

    @Test
    public void testPutZeroWriteCountAfterFreshIncompleteRun() {
        setNoProgress(progressStore);
        setNextTableToSweep(TABLE_REF, nextTableToSweepProvider);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(new byte[] {1, 2, 3})
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(priorityStore).update(
                Mockito.any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newWriteCount(0L)
                        .build()));
    }

    @Test
    public void testMetricsNotRecordedAfterIncompleteRun() {
        setNoProgress(progressStore);
        setNextTableToSweep(TABLE_REF, nextTableToSweepProvider);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(new byte[] {1, 2, 3})
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verifyZeroInteractions(sweepMetrics);
    }

    @Test
    public void testRecordCumulativeMetricsAfterCompleteRun() {
        setProgress(progressStore,
                ImmutableSweepProgress.builder()
                        .tableRef(TABLE_REF)
                        .staleValuesDeleted(3)
                        .cellTsPairsExamined(11)
                        .minimumSweptTimestamp(4567L)
                        .startRow(new byte[] {1, 2, 3})
                        .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetrics).examinedCells(TABLE_REF, 21);
        Mockito.verify(sweepMetrics).deletedCells(TABLE_REF, 5);
    }

    @Test
    public void testCompactInternallyAfterCompleteRunIfNonZeroDeletes() {
        setNoProgress(progressStore);
        setNextTableToSweep(TABLE_REF, nextTableToSweepProvider);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(1)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(kvs).compactInternally(TABLE_REF);
    }

    @Test
    public void testDontCompactInternallyAfterCompleteRunIfZeroDeletes() {
        setNoProgress(progressStore);
        setNextTableToSweep(TABLE_REF, nextTableToSweepProvider);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(0)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .build(), sweepTaskRunner, TABLE_REF);
        backgroundSweeper.runOnce();
        Mockito.verify(kvs, Mockito.never()).compactInternally(TABLE_REF);
    }
}
