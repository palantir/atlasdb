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
import com.palantir.atlasdb.workload.invariant.ImmutableInvariantReporter;
import com.palantir.atlasdb.workload.invariant.Invariant;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.invariant.MultiCellViolation;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TransientRowsWorkflows {
    private static final SafeLogger log = SafeLoggerFactory.get(TransientRowsWorkflows.class);

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
            txn.write(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex), 0);
            if (taskIndex > 0) {
                txn.delete(tableName, ImmutableWorkloadCell.of(taskIndex - 1, COLUMN));
                txn.delete(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex - 1));
            }
        });
    }

    public static InvariantReporter<List<MultiCellViolation>> getSummaryLogInvariantReporter(
            TransientRowsWorkflowConfiguration configuration) {
        return ImmutableInvariantReporter.<List<MultiCellViolation>>builder()
                .invariant(createSummaryRowInvariant(configuration))
                .consumer(violations -> {
                    if (!violations.isEmpty()) {
                        log.error(
                                "Detected rows which did not agree with state in a summary index: {}",
                                SafeArg.of("violations", violations));
                    }
                })
                .build();
    }

    private static Invariant<List<MultiCellViolation>> createSummaryRowInvariant(
            TransientRowsWorkflowConfiguration configuration) {
        return (workflowHistory, notifier) -> {
            List<MultiCellViolation> violations = new ArrayList<>();
            ReadableTransactionStore store = workflowHistory.transactionStore();

            String tableName = configuration.tableConfiguration().tableName();
            Set<Integer> taskIndices =
                    IntStream.range(0, configuration.iterationCount()).boxed().collect(Collectors.toSet());
            taskIndices.forEach(index -> checkSummaryConsistencyForIndex(violations, store, tableName, index));

            notifier.accept(violations);
        };
    }

    private static void checkSummaryConsistencyForIndex(
            List<MultiCellViolation> violations, ReadableTransactionStore store, String tableName, Integer index) {
        WorkloadCell primaryCell = ImmutableWorkloadCell.of(index, COLUMN);
        Optional<Integer> valueFromPrimaryRow = store.get(tableName, primaryCell);
        WorkloadCell summaryCell = ImmutableWorkloadCell.of(SUMMARY_ROW, index);
        Optional<Integer> valueFromSummaryRow = store.get(tableName, summaryCell);
        if (valueFromSummaryRow.isPresent() ^ valueFromPrimaryRow.isPresent()) {
            violations.add(MultiCellViolation.builder()
                    .putViolatingCellValues(TableAndWorkloadCell.of(tableName, primaryCell), valueFromPrimaryRow)
                    .putViolatingCellValues(TableAndWorkloadCell.of(tableName, summaryCell), valueFromSummaryRow)
                    .build());
        }
    }
}
