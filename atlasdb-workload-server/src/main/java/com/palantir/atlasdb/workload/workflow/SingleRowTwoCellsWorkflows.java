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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.workload.store.ColumnValue;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Consider a single row in a database that has two cells, which should between them maintain the invariant that
 * at most one cell has a value (this workflow is extremely common in AtlasDB Proxy). This workflow maintains
 * concurrent transactions that read the row (both cells), and then sends an update to one of the cells and attempts
 * to delete the other.
 */
public final class SingleRowTwoCellsWorkflows {
    private static final SafeLogger log = SafeLoggerFactory.get(SingleRowTwoCellsWorkflows.class);
    private static final int SINGLE_ROW = 1;
    private static final int FIRST_COLUMN = 1;
    private static final int SECOND_COLUMN = 2;

    @VisibleForTesting
    static final WorkloadCell FIRST_CELL = ImmutableWorkloadCell.of(SINGLE_ROW, FIRST_COLUMN);

    @VisibleForTesting
    static final WorkloadCell SECOND_CELL = ImmutableWorkloadCell.of(SINGLE_ROW, SECOND_COLUMN);

    private SingleRowTwoCellsWorkflows() {
        // static factory
    }

    public static Workflow createSingleRowTwoCell(
            InteractiveTransactionStore store,
            SingleRowTwoCellsWorkflowConfiguration singleRowTwoCellsWorkflowConfiguration,
            ListeningExecutorService executionExecutor) {
        store.readWrite(txn -> txn.write(singleRowTwoCellsWorkflowConfiguration.tableConfiguration().tableName(), FIRST_CELL, 1));
        return DefaultWorkflow.create(
                store,
                (txnStore, index) -> run(txnStore, index, singleRowTwoCellsWorkflowConfiguration),
                singleRowTwoCellsWorkflowConfiguration,
                executionExecutor);
    }

    private static Optional<WitnessedTransaction> run(
            InteractiveTransactionStore store, int taskIndex, SingleRowTwoCellsWorkflowConfiguration workflowConfiguration) {
        workflowConfiguration.transactionRateLimiter().acquire();
        store.readWrite(txn -> {
            List<ColumnValue> values = txn.getRowColumnRange(
                    workflowConfiguration.tableConfiguration().tableName(),
                    SINGLE_ROW,
                    ColumnRangeSelection.builder().build());
            Map<Integer, Integer> tableState = values.stream()
                    .collect(Collectors.toMap(ColumnValue::column, ColumnValue::value));
            if (tableState.size() != 1) {
                log.error("Wrong number of values found in table state {}",
                        SafeArg.of("tableState", tableState));
            }

            if (tableState.containsKey(FIRST_COLUMN)) {
                txn.delete(workflowConfiguration.tableConfiguration().tableName(), FIRST_CELL);
                txn.write(workflowConfiguration.tableConfiguration().tableName(), SECOND_CELL, taskIndex);
            } else {
                txn.delete(workflowConfiguration.tableConfiguration().tableName(), SECOND_CELL);
                txn.write(workflowConfiguration.tableConfiguration().tableName(), FIRST_CELL, taskIndex);
            }
        });

        List<TransactionAction> transactionActions = createTransactionActions(
                taskIndex, workflowConfiguration.tableConfiguration().tableName());
        Optional<WitnessedTransaction> maybeTransaction = store.readWrite(transactionActions);
        maybeTransaction.ifPresent(_transaction -> log.info(
                "Transaction successfully committed for single row two cells workflow for task index {}.",
                SafeArg.of("taskIndex", taskIndex)));
        return maybeTransaction;
    }

    @VisibleForTesting
    static List<TransactionAction> createTransactionActions(int taskIndex, String tableName) {
        List<TransactionAction> cellReads = createCellReadActions(tableName);
        List<TransactionAction> cellUpdates = createCellUpdateActions(taskIndex, tableName);
        return Streams.concat(cellReads.stream(), cellUpdates.stream(), cellReads.stream())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static boolean shouldWriteToFirstCell(int taskIndex) {
        return taskIndex % 2 == 0;
    }

    private static List<TransactionAction> createCellUpdateActions(int taskIndex, String tableName) {
        return shouldWriteToFirstCell(taskIndex)
                ? ImmutableList.of(
                WriteTransactionAction.of(tableName, FIRST_CELL, taskIndex),
                DeleteTransactionAction.of(tableName, SECOND_CELL))
                : ImmutableList.of(
                DeleteTransactionAction.of(tableName, FIRST_CELL),
                WriteTransactionAction.of(tableName, SECOND_CELL, taskIndex));
    }

    private static List<TransactionAction> createCellReadActions(String tableName) {
        return ImmutableList.of(
                ReadTransactionAction.of(tableName, FIRST_CELL), ReadTransactionAction.of(tableName, SECOND_CELL));
    }
}
