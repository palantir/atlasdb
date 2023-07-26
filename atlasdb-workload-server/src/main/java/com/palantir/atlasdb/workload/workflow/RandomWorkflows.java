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
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ImmutableDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ImmutableRowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.ImmutableRowColumnRangeReadTransactionAction.Builder;
import com.palantir.atlasdb.workload.transaction.ImmutableWriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.SingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.jetbrains.annotations.VisibleForTesting;

public final class RandomWorkflows {
    private static final SecureRandom RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private RandomWorkflows() {
        // static factory
    }

    public static Workflow create(
            TransactionStore store,
            RandomWorkflowConfiguration randomWorkflowConfig,
            ListeningExecutorService executionExecutor) {
        return create(store, randomWorkflowConfig, executionExecutor, RANDOM);
    }

    @VisibleForTesting
    static Workflow create(
            TransactionStore store,
            RandomWorkflowConfiguration randomWorkflowConfig,
            ListeningExecutorService executionExecutor,
            SecureRandom random) {
        RandomWorkflowTask task = new RandomWorkflowTask(randomWorkflowConfig, random);
        return DefaultWorkflow.create(
                store, (txnStore, _index) -> task.run(txnStore), randomWorkflowConfig, executionExecutor);
    }

    private static final class RandomWorkflowTask {

        private final RandomWorkflowConfiguration workflowConfiguration;
        private final SecureRandom random;

        public RandomWorkflowTask(RandomWorkflowConfiguration workflowConfiguration, SecureRandom random) {
            this.workflowConfiguration = workflowConfiguration;
            this.random = random;
        }

        public Optional<WitnessedTransaction> run(TransactionStore store) {
            workflowConfiguration.transactionRateLimiter().acquire();
            List<TransactionAction> actions = Stream.of(
                            generateReadActions(),
                            generateWriteActions(),
                            generateDeleteActions(),
                            generateRowColumnRangeReadActions())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            Collections.shuffle(actions, random);
            return store.readWrite(actions);
        }

        private List<SingleCellReadTransactionAction> generateReadActions() {
            return IntStream.range(0, random.nextInt(workflowConfiguration.maxReads() + 1))
                    .boxed()
                    .map(_index -> SingleCellReadTransactionAction.of(
                            workflowConfiguration.tableConfiguration().tableName(), randomCell()))
                    .collect(Collectors.toList());
        }

        private List<WriteTransactionAction> generateWriteActions() {
            return IntStream.range(0, random.nextInt(workflowConfiguration.maxWrites() + 1))
                    .boxed()
                    .map(_index -> ImmutableWriteTransactionAction.of(
                            workflowConfiguration.tableConfiguration().tableName(),
                            randomCell(),
                            random.nextInt(100_000)))
                    .collect(Collectors.toList());
        }

        private List<DeleteTransactionAction> generateDeleteActions() {
            return IntStream.range(0, random.nextInt(workflowConfiguration.maxDeletes() + 1))
                    .boxed()
                    .map(_index -> ImmutableDeleteTransactionAction.of(
                            workflowConfiguration.tableConfiguration().tableName(), randomCell()))
                    .collect(Collectors.toList());
        }

        private List<RowColumnRangeReadTransactionAction> generateRowColumnRangeReadActions() {
            return IntStream.range(0, random.nextInt(workflowConfiguration.maxRowRangeScans() + 1))
                    .boxed()
                    .map(_index -> {
                        WorkloadCell startCell = randomCell();
                        int endColumn = random.nextInt(workflowConfiguration.maxColumns());
                        Builder builder = ImmutableRowColumnRangeReadTransactionAction.builder()
                                .table(workflowConfiguration
                                        .tableConfiguration()
                                        .tableName())
                                .row(startCell.key());
                        if (startCell.column() < endColumn) {
                            builder.columnRangeSelection(ColumnRangeSelection.builder()
                                    .startColumnInclusive(startCell.column())
                                    .endColumnExclusive(endColumn + 1)
                                    .build());
                        } else {
                            builder.columnRangeSelection(ColumnRangeSelection.builder()
                                    .startColumnInclusive(endColumn)
                                    .endColumnExclusive(startCell.column() + 1)
                                    .build());
                        }
                        return builder.build();
                    })
                    .collect(Collectors.toList());
        }

        private WorkloadCell randomCell() {
            return ImmutableWorkloadCell.of(
                    random.nextInt(workflowConfiguration.maxRows()),
                    random.nextInt(workflowConfiguration.maxColumns()));
        }
    }
}
