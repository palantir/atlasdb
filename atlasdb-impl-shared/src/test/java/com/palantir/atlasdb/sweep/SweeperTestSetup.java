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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class SweeperTestSetup {

    protected static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName(
            "backgroundsweeper.fasttest");

    protected SpecificTableSweeper specificTableSweeper;
    protected BackgroundSweeperImpl backgroundSweeper;
    protected KeyValueService kvs = Mockito.mock(KeyValueService.class);
    protected SweepProgressStore progressStore = Mockito.mock(SweepProgressStore.class);
    protected SweepPriorityStore priorityStore = Mockito.mock(SweepPriorityStore.class);
    private NextTableToSweepProvider nextTableToSweepProvider = Mockito.mock(NextTableToSweepProvider.class);
    private SweepTaskRunner sweepTaskRunner = Mockito.mock(SweepTaskRunner.class);
    private boolean sweepEnabled = true;
    protected SweepMetrics sweepMetrics = Mockito.mock(SweepMetrics.class);
    protected long currentTimeMillis = 1000200300L;

    @Before
    public void setup() throws InterruptedException {
        specificTableSweeper = getSpecificTableSweeperService();
        SweepLocks sweepLocks = Mockito.mock(SweepLocks.class);
        Mockito.doReturn(true).when(sweepLocks).lockOrRefreshSweepLease();
        Mockito.doReturn(true).when(sweepLocks).lockTableToSweep(Mockito.any());

        backgroundSweeper = new BackgroundSweeperImpl(
                sweepLocks,
                nextTableToSweepProvider,
                () -> sweepEnabled,
                specificTableSweeper,
                new AtomicInteger(0),
                new AtomicInteger(0));
    }

    protected SpecificTableSweeper getSpecificTableSweeperService() {
        ImmutableSweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
                .deleteBatchSize(100)
                .candidateBatchSize(200)
                .maxCellTsPairsToExamine(1000)
                .build();

        return new SpecificTableSweeper(
                SweeperTestSetup.mockTxManager(),
                kvs,
                sweepTaskRunner,
                () -> sweepBatchConfig,
                priorityStore,
                progressStore,
                Mockito.mock(BackgroundSweeperPerformanceLogger.class),
                sweepMetrics,
                () -> currentTimeMillis);
    }

    public static LockAwareTransactionManager mockTxManager() {
        LockAwareTransactionManager txManager = Mockito.mock(LockAwareTransactionManager.class);
        Answer runTaskAnswer = inv -> {
            Object[] args = inv.getArguments();
            TransactionTask<?, ?> task = (TransactionTask<?, ?>) args[0];
            return task.execute(Mockito.mock(Transaction.class));
        };
        Mockito.doAnswer(runTaskAnswer).when(txManager).runTaskReadOnly(Mockito.any());
        Mockito.doAnswer(runTaskAnswer).when(txManager).runTaskWithRetry(Mockito.any());
        return txManager;
    }

    protected void setNoProgress() {
        Mockito.doReturn(Optional.empty()).when(progressStore).loadProgress(Mockito.any());
    }

    protected void setProgress(SweepProgress progress) {
        Mockito.doReturn(Optional.of(progress)).when(progressStore).loadProgress(Mockito.any());
    }

    protected void setNextTableToSweep(TableReference tableRef) {
        Mockito.doReturn(Optional.of(tableRef)).when(nextTableToSweepProvider)
                .chooseNextTableToSweep(Mockito.any(), Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(Sets.newHashSet(tableRef)).when(kvs)
                .getAllTableNames();
    }

    protected void setupTaskRunner(SweepResults results) {
        Mockito.doReturn(results).when(sweepTaskRunner).run(Mockito.eq(TABLE_REF), Mockito.any(), Mockito.any());
    }

}
