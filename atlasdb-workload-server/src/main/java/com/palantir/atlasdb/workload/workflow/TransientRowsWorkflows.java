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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.invariant.CrossCellInconsistency;
import com.palantir.atlasdb.workload.invariant.ImmutableInvariantReporter;
import com.palantir.atlasdb.workload.invariant.Invariant;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class TransientRowsWorkflows {
    private static final SafeLogger log = SafeLoggerFactory.get(TransientRowsWorkflows.class);

    @VisibleForTesting
    static final int SUMMARY_ROW = -1;

    @VisibleForTesting
    static final int COLUMN = 1;

    @VisibleForTesting
    static final int VALUE = 0;

    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private TransientRowsWorkflows() {}

    public static Workflow create(
            InteractiveTransactionStore store,
            TransientRowsWorkflowConfiguration workflowConfiguration,
            ListeningExecutorService executionExecutor) {
        TransientRowsTask transientRowsTask =
                new TransientRowsTask(workflowConfiguration, TransientRowsWorkflows::logViolations);
        return DefaultWorkflow.create(store, transientRowsTask, workflowConfiguration, executionExecutor);
    }

    public static Workflow create(
            InteractiveTransactionStore store,
            TransientRowsWorkflowConfiguration workflowConfiguration,
            ListeningExecutorService executionExecutor,
            Consumer<List<CrossCellInconsistency>> onFailure) {
        TransientRowsTask transientRowsTask = new TransientRowsTask(workflowConfiguration, onFailure);
        return DefaultWorkflow.create(store, transientRowsTask, workflowConfiguration, executionExecutor);
    }

    private static final class TransientRowsTask
            extends StoppableKeyedTransactionTask<InteractiveTransactionStore, List<CrossCellInconsistency>> {

        private final TransientRowsWorkflowConfiguration configuration;

        private TransientRowsTask(
                TransientRowsWorkflowConfiguration configuration, Consumer<List<CrossCellInconsistency>> onFailure) {
            super(new AtomicBoolean(), onFailure);
            this.configuration = configuration;
        }

        @Override
        protected Optional<WitnessedTransaction> run(InteractiveTransactionStore store, Integer taskIndex) {
            if (taskIndex % configuration.validateEveryNIterations() == 0) {
                // This is cheeky, as we are not including our reads in our transaction history!
                List<CrossCellInconsistency> violations =
                        findInconsistencyInIndexStateDuringWorkflow(configuration, store);

                if (!violations.isEmpty()) {
                    recordFailure(violations);
                    return Optional.empty();
                }
            }

            return store.readWrite(txn -> {
                String tableName = configuration.tableConfiguration().tableName();
                txn.write(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex), VALUE);
                txn.write(tableName, ImmutableWorkloadCell.of(taskIndex, COLUMN), VALUE);
                if (taskIndex > 0) {
                    int indexToRead = SECURE_RANDOM.nextInt(taskIndex);
                    txn.read(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, indexToRead));
                    txn.read(tableName, ImmutableWorkloadCell.of(indexToRead, COLUMN));

                    if (txn.read(tableName, ImmutableWorkloadCell.of(taskIndex - 1, COLUMN))
                            .isPresent()) {
                        txn.delete(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex - 1));
                        txn.delete(tableName, ImmutableWorkloadCell.of(taskIndex - 1, COLUMN));
                    }
                }
            });
        }
    }

    public static InvariantReporter<List<CrossCellInconsistency>> getSummaryLogInvariantReporter(
            TransientRowsWorkflowConfiguration configuration) {
        return ImmutableInvariantReporter.<List<CrossCellInconsistency>>builder()
                .invariant(createSummaryRowInvariant(configuration))
                .consumer(TransientRowsWorkflows::logViolations)
                .build();
    }

    private static Invariant<List<CrossCellInconsistency>> createSummaryRowInvariant(
            TransientRowsWorkflowConfiguration configuration) {
        return (workflowHistory, notifier) -> {
            List<CrossCellInconsistency> violations = new ArrayList<>();
            ReadableTransactionStore store = workflowHistory.transactionStore();

            violations.addAll(findInconsistencyInTransactionHistory(workflowHistory.history()));
            violations.addAll(findInconsistencyInFinalIndexState(configuration, store));

            notifier.accept(violations);
        };
    }

    private static List<CrossCellInconsistency> findInconsistencyInTransactionHistory(
            List<WitnessedTransaction> history) {
        return history.stream()
                .map(WitnessedTransaction::actions)
                .flatMap(TransientRowsWorkflows::findInconsistencyInTransactionActions)
                .collect(Collectors.toList());
    }

    private static Stream<CrossCellInconsistency> findInconsistencyInTransactionActions(
            List<WitnessedTransactionAction> actions) {
        Set<WitnessedSingleCellReadTransactionAction> readTransactionActions = actions.stream()
                .filter(WitnessedSingleCellReadTransactionAction.class::isInstance)
                .map(action -> (WitnessedSingleCellReadTransactionAction) action)
                .collect(Collectors.toSet());
        if (readTransactionActions.isEmpty()) {
            return Stream.empty();
        }
        WitnessedSingleCellReadTransactionAction summaryRowRead = readTransactionActions.stream()
                .filter(action -> action.cell().key() == SUMMARY_ROW)
                .findAny()
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Expected to find a read of the summary row", SafeArg.of("actions", actions)));
        WitnessedSingleCellReadTransactionAction normalRowRead = readTransactionActions.stream()
                .filter(action ->
                        action.cell().key().equals(summaryRowRead.cell().column()))
                .findAny()
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Expected to find a read of a corresponding normal row", SafeArg.of("actions", actions)));
        if (!summaryRowRead.value().equals(normalRowRead.value())) {
            return Stream.of(CrossCellInconsistency.builder()
                    .putInconsistentValues(
                            TableAndWorkloadCell.of(summaryRowRead.table(), summaryRowRead.cell()),
                            summaryRowRead.value())
                    .putInconsistentValues(
                            TableAndWorkloadCell.of(normalRowRead.table(), normalRowRead.cell()), normalRowRead.value())
                    .build());
        } else {
            return Stream.empty();
        }
    }

    private static List<CrossCellInconsistency> findInconsistencyInIndexStateDuringWorkflow(
            TransientRowsWorkflowConfiguration configuration, InteractiveTransactionStore store) {
        List<CrossCellInconsistency> violations = new ArrayList<>();
        String tableName = configuration.tableConfiguration().tableName();
        Set<Integer> taskIndices =
                IntStream.range(0, configuration.iterationCount()).boxed().collect(Collectors.toSet());
        taskIndices.forEach(index ->
                store.readWrite(txn -> checkSummaryConsistencyForIndex(violations, txn::read, tableName, index)));
        return violations;
    }

    private static List<CrossCellInconsistency> findInconsistencyInFinalIndexState(
            TransientRowsWorkflowConfiguration configuration, ReadableTransactionStore store) {
        List<CrossCellInconsistency> violations = new ArrayList<>();
        String tableName = configuration.tableConfiguration().tableName();
        Set<Integer> taskIndices =
                IntStream.range(0, configuration.iterationCount()).boxed().collect(Collectors.toSet());
        taskIndices.forEach(index -> checkSummaryConsistencyForIndex(violations, store::get, tableName, index));
        return violations;
    }

    private static void checkSummaryConsistencyForIndex(
            List<CrossCellInconsistency> violations,
            BiFunction<String, WorkloadCell, Optional<Integer>> reader,
            String tableName,
            Integer index) {
        WorkloadCell primaryCell = ImmutableWorkloadCell.of(index, COLUMN);
        Optional<Integer> valueFromPrimaryRow = reader.apply(tableName, primaryCell);
        WorkloadCell summaryCell = ImmutableWorkloadCell.of(SUMMARY_ROW, index);
        Optional<Integer> valueFromSummaryRow = reader.apply(tableName, summaryCell);
        if (valueFromSummaryRow.isPresent() ^ valueFromPrimaryRow.isPresent()) {
            violations.add(CrossCellInconsistency.builder()
                    .putInconsistentValues(TableAndWorkloadCell.of(tableName, primaryCell), valueFromPrimaryRow)
                    .putInconsistentValues(TableAndWorkloadCell.of(tableName, summaryCell), valueFromSummaryRow)
                    .build());
        }
    }

    private static void logViolations(List<CrossCellInconsistency> violations) {
        if (!violations.isEmpty()) {
            log.error(
                    "Detected rows which did not agree with state in a summary index: {}",
                    SafeArg.of("violations", violations));
        }
    }
}
