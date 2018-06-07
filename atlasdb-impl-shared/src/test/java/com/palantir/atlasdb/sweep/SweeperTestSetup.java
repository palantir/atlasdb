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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.stubbing.Answer;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.metrics.SweepMetricsManager;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.LockService;

public class SweeperTestSetup {

    protected static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName(
            "backgroundsweeper.fasttest");

    protected static AdjustableSweepBatchConfigSource sweepBatchConfigSource;

    protected SpecificTableSweeper specificTableSweeper;
    protected BackgroundSweepThread backgroundSweeper;
    protected KeyValueService kvs = mock(KeyValueService.class);
    protected SweepProgressStore progressStore = mock(SweepProgressStore.class);
    protected SweepPriorityStore priorityStore = mock(SweepPriorityStore.class);
    private NextTableToSweepProvider nextTableToSweepProvider = mock(NextTableToSweepProvider.class);
    protected SweepTaskRunner sweepTaskRunner = mock(SweepTaskRunner.class);
    private boolean sweepEnabled = true;
    protected SweepMetricsManager sweepMetricsManager = mock(SweepMetricsManager.class);
    protected long currentTimeMillis = 1000200300L;

    @BeforeClass
    public static void initialiseConfig() {
        ImmutableSweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
                .deleteBatchSize(100)
                .candidateBatchSize(200)
                .maxCellTsPairsToExamine(1000)
                .build();

        sweepBatchConfigSource = AdjustableSweepBatchConfigSource.create(() -> sweepBatchConfig);
    }

    @Before
    public void setup() {
        specificTableSweeper = getSpecificTableSweeperService();

        backgroundSweeper = new BackgroundSweepThread(
                mock(LockService.class),
                nextTableToSweepProvider,
                sweepBatchConfigSource,
                () -> sweepEnabled,
                () -> 0L, // pauseMillis
                SweepPriorityOverrideConfig::defaultConfig,
                specificTableSweeper,
                new SweepOutcomeMetrics(),
                new CountDownLatch(1));
    }

    protected SpecificTableSweeper getSpecificTableSweeperService() {
        return new SpecificTableSweeper(
                SweeperTestSetup.mockTxManager(),
                kvs,
                sweepTaskRunner,
                priorityStore,
                progressStore,
                mock(BackgroundSweeperPerformanceLogger.class),
                sweepMetricsManager,
                () -> currentTimeMillis);
    }

    public static LockAwareTransactionManager mockTxManager() {
        LockAwareTransactionManager txManager = mock(LockAwareTransactionManager.class);
        Answer runTaskAnswer = inv -> {
            Object[] args = inv.getArguments();
            TransactionTask<?, ?> task = (TransactionTask<?, ?>) args[0];
            return task.execute(mock(Transaction.class));
        };
        doAnswer(runTaskAnswer).when(txManager).runTaskReadOnly(any());
        doAnswer(runTaskAnswer).when(txManager).runTaskWithRetry(any());
        return txManager;
    }

    protected void setNoProgress() {
        doReturn(Optional.empty()).when(progressStore).loadProgress();
    }

    protected void setProgress(SweepProgress progress) {
        doReturn(Optional.of(progress)).when(progressStore).loadProgress();
    }

    protected void setNextTableToSweep(TableReference tableRef) {
        doReturn(Optional.of(tableRef)).when(nextTableToSweepProvider).getNextTableToSweep(any(), anyLong());
        doReturn(Optional.of(tableRef)).when(nextTableToSweepProvider).getNextTableToSweep(any(), anyLong(), any());
    }

    protected void setupTaskRunner(SweepResults results) {
        doReturn(results).when(sweepTaskRunner).run(eq(TABLE_REF), any(), any());
    }

}
