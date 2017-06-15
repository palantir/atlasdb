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

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class SweeperTestSetup {
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

    protected void setNoProgress(SweepProgressStore progressStore) {
        Mockito.doReturn(Optional.empty()).when(progressStore).loadProgress(Mockito.any());
    }

    protected void setProgress(SweepProgressStore progressStore, SweepProgress progress) {
        Mockito.doReturn(Optional.of(progress)).when(progressStore).loadProgress(Mockito.any());
    }

    protected void setNextTableToSweep(TableReference tableRef, NextTableToSweepProvider nextTableToSweepProvider) {
        Mockito.doReturn(Optional.of(tableRef)).when(nextTableToSweepProvider)
                .chooseNextTableToSweep(Mockito.any(), Mockito.anyLong());
    }

    protected void setupTaskRunner(SweepResults results, SweepTaskRunner sweepTaskRunner,
            TableReference tableRef) {
        Mockito.doReturn(results).when(sweepTaskRunner).run(Mockito.eq(tableRef), Mockito.any(), Mockito.any());
    }

}
