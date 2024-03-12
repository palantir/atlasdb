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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class MultipleBusyCellWorkflowsTest {
    private static final String TABLE_NAME = "multiple_busy.cell";
    private static final int ITERATION_COUNT = 1000;
    private static final double DELETE_PROBABILITY = 0.2;
    private static final int MAX_CELLS = 20;
    private static final double PROPORTION_OF_ITERATION_COUNT_AS_UPDATES = 0.8;
    private static final MultipleBusyCellWorkflowConfiguration CONFIGURATION =
            ImmutableMultipleBusyCellWorkflowConfiguration.builder()
                    .tableConfiguration(ImmutableTableConfiguration.builder()
                            .tableName(TABLE_NAME)
                            .isolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())
                    .iterationCount(ITERATION_COUNT)
                    .deleteProbability(DELETE_PROBABILITY)
                    .maxCells(MAX_CELLS)
                    .proportionOfIterationCountAsUpdates(PROPORTION_OF_ITERATION_COUNT_AS_UPDATES)
                    .build();

    private final InteractiveTransactionStore transactionStore = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(
                    TableReference.createWithEmptyNamespace(TABLE_NAME),
                    AtlasDbUtils.tableMetadata(IsolationLevel.SERIALIZABLE)));
    private final Workflow workflow = MultipleBusyCellWorkflows.create(
            transactionStore,
            CONFIGURATION,
            MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(4)),
            MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(4)));

    @Test
    public void workflowHistoryTransactionStoreShouldBeReadOnly() {
        WorkflowHistory history = workflow.run();
        assertThat(history.transactionStore()).isInstanceOf(ReadOnlyTransactionStore.class);
    }

    @Test
    public void transactionsDoNotConflictExcessively() {
        WorkflowHistory workflowHistory = workflow.run();
        assertThat(workflowHistory.history()).hasSizeBetween(ITERATION_COUNT / 2, ITERATION_COUNT);
    }

    @Test
    public void transactionsWriteToAtMostMaxCells() {
        WorkflowHistory workflowHistory = workflow.run();
        assertThat(workflowHistory.history().stream()
                        .flatMap(x -> x.actions().stream())
                        .map(x -> x.accept(new WitnessedTransactionActionVisitor<WorkloadCell>() {

                            @Override
                            public WorkloadCell visit(WitnessedSingleCellReadTransactionAction readTransactionAction) {
                                return readTransactionAction.cell();
                            }

                            @Override
                            public WorkloadCell visit(WitnessedWriteTransactionAction writeTransactionAction) {
                                return writeTransactionAction.cell();
                            }

                            @Override
                            public WorkloadCell visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
                                return deleteTransactionAction.cell();
                            }

                            @Override
                            public WorkloadCell visit(
                                    WitnessedRowColumnRangeReadTransactionAction rowColumnRangeReadTransactionAction) {
                                throw new RuntimeException("We don't expect to see this action");
                            }
                        }))
                        .collect(Collectors.toSet()))
                .hasSizeBetween(1, MAX_CELLS);
    }

    @Test
    public void transactionWriteActionsConvergeToConfiguredProbabilities() {
        WorkflowHistory workflowHistory = workflow.run();
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger deletes = new AtomicInteger();
        workflowHistory.history().stream()
                .flatMap(x -> x.actions().stream())
                .forEach(x -> x.accept(new WitnessedTransactionActionVisitor<Void>() {

                    @Override
                    public Void visit(WitnessedSingleCellReadTransactionAction readTransactionAction) {
                        return null;
                    }

                    @Override
                    public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
                        writes.incrementAndGet();
                        return null;
                    }

                    @Override
                    public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
                        deletes.incrementAndGet();
                        return null;
                    }

                    @Override
                    public Void visit(
                            WitnessedRowColumnRangeReadTransactionAction rowColumnRangeReadTransactionAction) {
                        throw new RuntimeException("We don't expect to see this action");
                    }
                }));
        double posteriorProbability = deletes.get() / (double) (writes.get() + deletes.get());
        assertThat(posteriorProbability).isCloseTo(DELETE_PROBABILITY, within(0.05));
    }

    @Test
    public void transactionReadsAndWritesDoesNotExceedConfiguredLimits() {
        WorkflowHistory workflowHistory = workflow.run();
        AtomicInteger updates = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        workflowHistory.history().stream()
                .flatMap(x -> x.actions().stream())
                .forEach(x -> x.accept(new WitnessedTransactionActionVisitor<Void>() {

                    @Override
                    public Void visit(WitnessedSingleCellReadTransactionAction readTransactionAction) {
                        reads.incrementAndGet();
                        return null;
                    }

                    @Override
                    public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
                        updates.incrementAndGet();
                        return null;
                    }

                    @Override
                    public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
                        updates.incrementAndGet();
                        return null;
                    }

                    @Override
                    public Void visit(
                            WitnessedRowColumnRangeReadTransactionAction rowColumnRangeReadTransactionAction) {
                        throw new RuntimeException("We don't expect to see this action");
                    }
                }));
        assertThat(updates.get()).isBetween(1, CONFIGURATION.maxUpdates());
        assertThat(reads.get()).isBetween(1, CONFIGURATION.maxReads());
    }
}
