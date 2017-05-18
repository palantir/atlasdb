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

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.RemoteLockService;

public class BackgroundSweeperFastTest {
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName(
            "backgroundsweeper.fasttest");

    private BackgroundSweeperImpl backgroundSweeper;
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
        backgroundSweeper = new BackgroundSweeperImpl(
                mockTxManager(),
                Mockito.mock(RemoteLockService.class),
                kvs,
                progressStore,
                priorityStore,
                nextTableToSweepProvider,
                sweepTaskRunner,
                () -> sweepEnabled,
                () -> 0L, // pauseMillis
                () -> ImmutableSweepBatchConfig.builder()
                        .deleteBatchSize(100)
                        .candidateBatchSize(200)
                        .maxCellTsPairsToExamine(1000)
                        .build(),
                Mockito.mock(BackgroundSweeperPerformanceLogger.class),
                sweepMetrics,
                Mockito.mock(PersistentLockManager.class),
                () -> currentTimeMillis);
    }

    @Test
    public void testWritePriorityAfterCompleteFreshRun() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .build());
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
        setProgress(ImmutableSweepProgress.builder()
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
                .build());
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
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(new byte[] {1, 2, 3})
                .build());
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
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(new byte[] {1, 2, 3})
                .build());
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
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(new byte[] {1, 2, 3})
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetrics, Mockito.never()).recordMetrics(Mockito.any(), Mockito.any());
    }

    @Test
    public void testRecordCumulativeMetricsAfterCompleteRun() {
        setProgress(ImmutableSweepProgress.builder()
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
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(sweepMetrics).recordMetrics(
                TABLE_REF,
                ImmutableSweepResults.builder()
                        .staleValuesDeleted(5)
                        .cellTsPairsExamined(21)
                        .sweptTimestamp(4567L)
                        .build());
    }

    @Test
    public void testCompactInternallyAfterCompleteRunIfNonZeroDeletes() {
        setNoProgress();
        setNextTableToSweep(TABLE_REF);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(1)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
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
                .build());
        backgroundSweeper.runOnce();
        Mockito.verify(kvs, Mockito.never()).compactInternally(TABLE_REF);
    }

    private void setNoProgress() {
        Mockito.doReturn(Optional.empty()).when(progressStore).loadProgress(Mockito.any());
    }

    private void setProgress(SweepProgress progress) {
        Mockito.doReturn(Optional.of(progress)).when(progressStore).loadProgress(Mockito.any());
    }

    private void setNextTableToSweep(TableReference tableRef) {
        Mockito.doReturn(Optional.of(tableRef)).when(nextTableToSweepProvider)
                .chooseNextTableToSweep(Mockito.any(), Mockito.anyLong());
    }

    private void setupTaskRunner(SweepResults results) {
        Mockito.doReturn(results).when(sweepTaskRunner).run(Mockito.eq(TABLE_REF), Mockito.any(), Mockito.any());
    }

    private static TransactionManager mockTxManager() {
        TransactionManager txManager = Mockito.mock(TransactionManager.class);
        Answer runTaskAnswer = inv -> {
            Object[] args = inv.getArguments();
            TransactionTask<?, ?> task = (TransactionTask<?, ?>) args[0];
            return task.execute(Mockito.mock(Transaction.class));
        };
        Mockito.doAnswer(runTaskAnswer).when(txManager).runTaskReadOnly(Mockito.any());
        Mockito.doAnswer(runTaskAnswer).when(txManager).runTaskWithRetry(Mockito.any());
        return txManager;
    }

}
