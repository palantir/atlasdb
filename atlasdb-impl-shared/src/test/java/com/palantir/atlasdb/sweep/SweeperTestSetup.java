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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.metrics.LegacySweepMetrics;
import com.palantir.atlasdb.sweep.metrics.SweepOutcomeMetrics;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.stubbing.Answer;

public class SweeperTestSetup {
    protected static final TableReference TABLE_REF =
            TableReference.createFromFullyQualifiedName("backgroundsweeper.fasttest");
    protected static final TableReference OTHER_TABLE =
            TableReference.createFromFullyQualifiedName("backgroundsweeper.fasttest_other");
    protected static final int THREAD_INDEX = 0;

    protected static AdjustableSweepBatchConfigSource sweepBatchConfigSource;

    private static final MetricsManager metricsManager = MetricsManagers.createForTests();
    protected SpecificTableSweeper specificTableSweeper;
    protected BackgroundSweepThread backgroundSweeper;
    protected KeyValueService kvs = mock(KeyValueService.class);
    protected SweepProgressStore progressStore = mock(SweepProgressStore.class);
    protected SweepPriorityStore priorityStore = mock(SweepPriorityStore.class);
    private NextTableToSweepProvider nextTableToSweepProvider = mock(NextTableToSweepProvider.class);
    protected SweepTaskRunner sweepTaskRunner = mock(SweepTaskRunner.class);
    private boolean sweepEnabled = true;
    protected LegacySweepMetrics sweepMetrics = mock(LegacySweepMetrics.class);
    protected long currentTimeMillis = 1000200300L;
    protected SweepPriorityOverrideConfig overrideConfig;

    @BeforeClass
    public static void initialiseConfig() {
        ImmutableSweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
                .deleteBatchSize(100)
                .candidateBatchSize(200)
                .maxCellTsPairsToExamine(1000)
                .build();

        sweepBatchConfigSource = AdjustableSweepBatchConfigSource.create(metricsManager, () -> sweepBatchConfig);
    }

    @Before
    public void setup() {
        specificTableSweeper = getSpecificTableSweeperService();
        backgroundSweeper = getBackgroundSweepThread(THREAD_INDEX);
        overrideConfig = SweepPriorityOverrideConfig.defaultConfig();
    }

    protected BackgroundSweepThread getBackgroundSweepThread(int threadIndex) {
        return new BackgroundSweepThread(
                mock(LockService.class),
                nextTableToSweepProvider,
                sweepBatchConfigSource,
                () -> sweepEnabled,
                () -> 0L, // pauseMillis
                () -> overrideConfig,
                specificTableSweeper,
                SweepOutcomeMetrics.registerLegacy(metricsManager),
                new CountDownLatch(1),
                threadIndex);
    }

    protected SpecificTableSweeper getSpecificTableSweeperService() {
        return new SpecificTableSweeper(
                SweeperTestSetup.mockTxManager(),
                kvs,
                sweepTaskRunner,
                priorityStore,
                progressStore,
                mock(BackgroundSweeperPerformanceLogger.class),
                sweepMetrics,
                () -> currentTimeMillis);
    }

    public static TransactionManager mockTxManager() {
        TransactionManager txManager = mock(TransactionManager.class);
        Answer<?> runTaskAnswer = inv -> {
            Object[] args = inv.getArguments();
            TransactionTask<?, ?> task = (TransactionTask<?, ?>) args[0];
            return task.execute(mock(Transaction.class));
        };
        doAnswer(runTaskAnswer).when(txManager).runTaskReadOnly(any());
        doAnswer(runTaskAnswer).when(txManager).runTaskWithRetry(any());
        return txManager;
    }

    protected void setNoProgress() {
        setNoProgress(TABLE_REF);
    }

    protected void setNoProgress(TableReference tableRef) {
        doReturn(Optional.empty()).when(progressStore).loadProgress(tableRef);
    }

    protected void setProgress(SweepProgress progress) {
        doReturn(Optional.of(progress)).when(progressStore).loadProgress(progress.tableRef());
    }

    protected void setNextTableToSweep(TableReference tableRef) {
        doReturn(Optional.of(getTableToSweep(tableRef)))
                .when(nextTableToSweepProvider)
                .getNextTableToSweep(any(), anyLong());
        doReturn(Optional.of(getTableToSweep(tableRef)))
                .when(nextTableToSweepProvider)
                .getNextTableToSweep(any(), anyLong(), any());
    }

    private TableToSweep getTableToSweep(TableReference tableRef) {
        return TableToSweep.newTable(tableRef, mock(SingleLockService.class));
    }

    protected void setupTaskRunner(SweepResults results) {
        setupTaskRunner(TABLE_REF, results);
    }

    protected void setupTaskRunner(TableReference tableRef, SweepResults results) {
        doReturn(results).when(sweepTaskRunner).run(eq(tableRef), any(), any(), any());
    }
}
