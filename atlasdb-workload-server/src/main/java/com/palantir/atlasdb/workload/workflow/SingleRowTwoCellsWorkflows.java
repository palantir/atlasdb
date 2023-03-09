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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Consider a single row in a database that has two cells, which should between them maintain the invariant that
 * at most one cell has a value (this workflow is extremely common in AtlasDB Proxy). This workflow maintains
 * concurrent transactions that read the row (both cells), and then sends an update to one of the cells and attempts
 * to delete the other.
 */
public final class SingleRowTwoCellsWorkflows {
    private static final int SINGLE_ROW = 1;
    private static final int FIRST_COLUMN = 1;
    private static final int SECOND_COLUMN = 2;

    @VisibleForTesting
    static final WorkloadCell FIRST_CELL = ImmutableWorkloadCell.of(SINGLE_ROW, FIRST_COLUMN);

    static final WorkloadCell SECOND_CELL = ImmutableWorkloadCell.of(SINGLE_ROW, SECOND_COLUMN);

    private SingleRowTwoCellsWorkflows() {
        // static factory
    }

    public static Workflow createSingleRowTwoCell(
            TransactionStore store, SingleRowTwoCellsWorkflowConfiguration singleRowTwoCellsWorkflowConfiguration) {
        return DefaultWorkflow.create(
                store,
                (txnStore, index) -> run(txnStore, index, singleRowTwoCellsWorkflowConfiguration),
                singleRowTwoCellsWorkflowConfiguration.genericWorkflowConfiguration());
    }

    private static Optional<WitnessedTransaction> run(
            TransactionStore store, int taskIndex, SingleRowTwoCellsWorkflowConfiguration workflowConfiguration) {
        workflowConfiguration.transactionRateLimiter().acquire();

        String tableName = workflowConfiguration.tableConfiguration().tableName();
        List<TransactionAction> cellReads = ImmutableList.of(
                ReadTransactionAction.of(tableName, FIRST_CELL), ReadTransactionAction.of(tableName, SECOND_CELL));
        List<TransactionAction> cellUpdates = shouldWriteToFirstCell(taskIndex)
                ? ImmutableList.of(
                        WriteTransactionAction.of(tableName, FIRST_CELL, taskIndex),
                        DeleteTransactionAction.of(tableName, SECOND_CELL))
                : ImmutableList.of(
                        DeleteTransactionAction.of(tableName, FIRST_CELL),
                        WriteTransactionAction.of(tableName, SECOND_CELL, taskIndex));
        return store.readWrite(Streams.concat(cellReads.stream(), cellUpdates.stream(), cellReads.stream())
                .collect(Collectors.toList()));
    }

    private static boolean shouldWriteToFirstCell(int taskIndex) {
        return taskIndex % 2 == 0;
    }
}
