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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.workload.DurableWritesMetrics;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStoreFactory;
import com.palantir.atlasdb.workload.store.IndexTable;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers;
import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public final class DurableWritesInvariantMetricReporterTest {

    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
    private final DurableWritesMetrics metrics = DurableWritesMetrics.of(taggedMetricRegistry);

    @Test
    public void runIncreasesMetricsIfViolationsFound() {
        DurableWritesInvariantMetricReporter reporter =
                new DurableWritesInvariantMetricReporter(WorkloadTestHelpers.WORKFLOW, metrics);
        AtlasDbTransactionStoreFactory factory = new AtlasDbTransactionStoreFactory(
                TransactionManagers.createInMemory(Set.of()), Optional.of("keyspace"));
        TransactionStore store = factory.create(
                Map.of(
                        WorkloadTestHelpers.TABLE_1,
                        IsolationLevel.SERIALIZABLE,
                        WorkloadTestHelpers.TABLE_2,
                        IsolationLevel.SERIALIZABLE),
                Set.of(IndexTable.builder()
                        .indexTableName(WorkloadTestHelpers.TABLE_1_INDEX_1)
                        .primaryTableName(WorkloadTestHelpers.TABLE_1)
                        .build()));
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
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
        reporter.report(workflowHistory);
        assertThat(getViolationCount(WorkloadTestHelpers.WORKFLOW, WorkloadTestHelpers.TABLE_1))
                .isEqualTo(1);
        assertThat(getViolationCount(WorkloadTestHelpers.WORKFLOW, WorkloadTestHelpers.TABLE_2))
                .isEqualTo(1);
        assertThat(getViolationCount(WorkloadTestHelpers.WORKFLOW, WorkloadTestHelpers.TABLE_1_INDEX_1))
                .isEqualTo(0);
    }

    private long getViolationCount(String workflow, String table) {
        return metrics.numberOfViolations()
                .workflow(workflow)
                .table(table)
                .build()
                .getCount();
    }
}
