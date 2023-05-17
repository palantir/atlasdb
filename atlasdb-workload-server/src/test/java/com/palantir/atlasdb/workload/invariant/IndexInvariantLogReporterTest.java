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

package com.palantir.atlasdb.workload.invariant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStoreFactory;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.IndexTable;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers;
import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class IndexInvariantLogReporterTest {

    @Test
    public void doesNotThrowIfNoViolationsProvided() {
        IndexInvariant meaninglessInvariant = IndexInvariant.createWithinSameTable(
                store -> ImmutableMap.of(),
                store -> ImmutableMap.of(),
                cell -> ImmutableWorkloadCell.of(-cell.key(), cell.column()),
                cell -> ImmutableWorkloadCell.of(-cell.key(), cell.column()));
        IndexInvariantLogReporter logReporter = new IndexInvariantLogReporter(meaninglessInvariant);

        WorkflowHistory workflowHistory = getWorkflowHistory();
        assertThatCode(() -> logReporter.report(workflowHistory)).doesNotThrowAnyException();
    }

    @Test
    public void doesNotThrowIfViolationsProvided() {
        IndexInvariant alwaysFailingInvariant = IndexInvariant.createWithinSameTable(
                store -> ImmutableMap.of(TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, WorkloadTestHelpers.WORKLOAD_CELL_ONE), 1),
                store -> ImmutableMap.of(),
                cell -> ImmutableWorkloadCell.of(-cell.key(), cell.column()),
                cell -> ImmutableWorkloadCell.of(-cell.key(), cell.column()));
        IndexInvariantLogReporter logReporter = new IndexInvariantLogReporter(alwaysFailingInvariant);

        WorkflowHistory workflowHistory = getWorkflowHistory();
        assertThatCode(() -> logReporter.report(workflowHistory)).doesNotThrowAnyException();
    }

    private static WorkflowHistory getWorkflowHistory() {
        AtlasDbTransactionStoreFactory factory = new AtlasDbTransactionStoreFactory(
                TransactionManagers.createInMemory(Set.of()), Optional.of("keyspace"));
        TransactionStore store = factory.create(
                Map.of(
                        WorkloadTestHelpers.TABLE_1,
                        IsolationLevel.SERIALIZABLE,
                        WorkloadTestHelpers.TABLE_2,
                        IsolationLevel.SERIALIZABLE),
                ImmutableSet.of());
        return ImmutableWorkflowHistory.builder()
                .transactionStore(store)
                .history(List.of(FullyWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(
                                WorkloadTestHelpers.TABLE_1,
                                WorkloadTestHelpers.WORKLOAD_CELL_ONE,
                                WorkloadTestHelpers.VALUE_ONE))
                        .addActions(WitnessedWriteTransactionAction.of(
                                WorkloadTestHelpers.TABLE_2,
                                WorkloadTestHelpers.WORKLOAD_CELL_ONE,
                                WorkloadTestHelpers.VALUE_ONE))
                        .build()))
                .build();
    }
}
