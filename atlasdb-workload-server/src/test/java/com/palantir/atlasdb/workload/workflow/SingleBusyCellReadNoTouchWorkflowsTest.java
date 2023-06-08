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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.common.concurrent.PTExecutors;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class SingleBusyCellReadNoTouchWorkflowsTest {
    private static final String TABLE_NAME = "busy.readnotouch.cell";
    private static final int ITERATION_COUNT = 1_000;
    private static final SingleBusyCellReadNoTouchWorkflowConfiguration CONFIGURATION =
            ImmutableSingleBusyCellReadNoTouchWorkflowConfiguration.builder()
                    .tableConfiguration(ImmutableTableConfiguration.builder()
                            .tableName(TABLE_NAME)
                            .isolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())
                    .iterationCount(ITERATION_COUNT)
                    .build();

    private final InteractiveTransactionStore transactionStore = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(
                    TableReference.createWithEmptyNamespace(TABLE_NAME),
                    AtlasDbUtils.tableMetadata(IsolationLevel.SERIALIZABLE)));
    private final Workflow workflow = SingleBusyCellReadNoTouchWorkflows.create(
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
    public void successfulTransactionsOnlyReadOrModifyTheTargetCell() {
        WorkflowHistory workflowHistory = workflow.run();
        List<WitnessedTransactionAction> witnessedTransactionActions = workflowHistory.history().stream()
                .map(WitnessedTransaction::actions)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        assertThat(witnessedTransactionActions)
                .allMatch(action -> action.table().equals(TABLE_NAME));
        assertThat(witnessedTransactionActions)
                .allMatch(action -> action.cell().equals(SingleBusyCellReadNoTouchWorkflows.BUSY_CELL));
    }

    @Test
    public void targetCellIsReadModifiedAndDeleted() {
        WorkflowHistory workflowHistory = workflow.run();
        List<WitnessedTransactionAction> witnessedTransactionActions = workflowHistory.history().stream()
                .map(WitnessedTransaction::actions)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        assertThat(witnessedTransactionActions).anyMatch(WitnessedReadTransactionAction.class::isInstance);
        assertThat(witnessedTransactionActions).anyMatch(WitnessedWriteTransactionAction.class::isInstance);
        assertThat(witnessedTransactionActions).anyMatch(WitnessedDeleteTransactionAction.class::isInstance);
    }

    @Test
    public void readTransactionsDontDoWrites() {
        WorkflowHistory workflowHistory = workflow.run();

        Set<WitnessedTransaction> witnessedTransactionsWithReads = workflowHistory.history().stream()
                .filter(txn -> txn.actions().stream().anyMatch(WitnessedReadTransactionAction.class::isInstance))
                .collect(Collectors.toSet());

        Set<WitnessedTransaction> witnessedTransactionsWithWritesOrDeletes = workflowHistory.history().stream()
                .filter(txn -> txn.actions().stream()
                        .anyMatch(action -> action instanceof WitnessedWriteTransactionAction
                                || action instanceof WitnessedDeleteTransactionAction))
                .collect(Collectors.toSet());

        Set<WitnessedTransaction> transactionsWithReadsAndWritesOrDeletes =
                Sets.intersection(witnessedTransactionsWithReads, witnessedTransactionsWithWritesOrDeletes);

        assertThat(transactionsWithReadsAndWritesOrDeletes).isEmpty();
    }
}
