/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.workflow;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.Optional;

public final class TransientRowsWorkflows {
    private static final int SUMMARY_ROW = -1;
    private static final int COLUMN = 1;

    private TransientRowsWorkflows() {}

    public static Workflow create(
            InteractiveTransactionStore store,
            TransientRowsWorkflowConfiguration workflowConfiguration,
            ListeningExecutorService executionExecutor) {
        return DefaultWorkflow.create(
                store,
                (txn, taskIndex) -> run(store, workflowConfiguration, taskIndex),
                workflowConfiguration,
                executionExecutor);
    }

    private static Optional<WitnessedTransaction> run(
            InteractiveTransactionStore store, TransientRowsWorkflowConfiguration configuration, int taskIndex) {
        return store.readWrite(txn -> {
            String tableName = configuration.tableConfiguration().tableName();
            txn.write(tableName, ImmutableWorkloadCell.of(taskIndex, COLUMN), 1);
            txn.delete(tableName, ImmutableWorkloadCell.of(taskIndex - 1, COLUMN));
            txn.write(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex), 0);
            txn.delete(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex - 1));
        });
    }
}
