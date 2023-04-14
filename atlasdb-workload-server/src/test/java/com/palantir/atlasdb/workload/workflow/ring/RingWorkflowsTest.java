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

package com.palantir.atlasdb.workload.workflow.ring;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.atlasdb.workload.workflow.ImmutableTableConfiguration;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.common.concurrent.PTExecutors;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class RingWorkflowsTest {

    private static final String TABLE_NAME = "ring.ring";
    private static final RingWorkflowConfiguration CONFIGURATION = ImmutableRingWorkflowConfiguration.builder()
            .tableConfiguration(ImmutableTableConfiguration.builder()
                    .tableName(TABLE_NAME)
                    .isolationLevel(IsolationLevel.SERIALIZABLE)
                    .build())
            .iterationCount(1)
            .build();

    private final InteractiveTransactionStore memoryStore = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(
                    TableReference.createWithEmptyNamespace(TABLE_NAME),
                    AtlasDbUtils.tableMetadata(IsolationLevel.SERIALIZABLE)));

    private final AtomicBoolean skipRunning = new AtomicBoolean(false);

    private final Workflow workflow = RingWorkflows.create(
            memoryStore,
            CONFIGURATION,
            MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(1)),
            skipRunning);

    @Test
    public void workflowHistoryTransactionStoreShouldBeReadOnly() {
        WorkflowHistory history = workflow.run();
        assertThat(history.transactionStore())
                .as("should return a read only tranasction store")
                .isInstanceOf(ReadOnlyTransactionStore.class);
    }

    @Test
    public void ringWorkflowContainsInitialReadsForRing() {
        WorkflowHistory history = workflow.run();

        List<WitnessedTransactionAction> actions =
                Iterables.getOnlyElement(history.history()).actions();

        List<WitnessedReadTransactionAction> initialReadActions = IntStream.range(0, CONFIGURATION.ringSize())
                .boxed()
                .map(index ->
                        WitnessedReadTransactionAction.of(TABLE_NAME, RingWorkflows.cell(index), Optional.empty()))
                .collect(Collectors.toList());
        assertThat(actions).containsAll(initialReadActions);
    }

    @Test
    public void ringWorkflowContainsInitialWritesForRing() {
        WorkflowHistory history = workflow.run();

        List<WitnessedTransactionAction> actions =
                Iterables.getOnlyElement(history.history()).actions();

        List<WorkloadCell> actualWrittenCells = actions.stream()
                .filter(WitnessedWriteTransactionAction.class::isInstance)
                .map(WitnessedWriteTransactionAction.class::cast)
                .map(WitnessedWriteTransactionAction::cell)
                .collect(Collectors.toList());

        List<WorkloadCell> expectedWrittenCells = IntStream.range(0, CONFIGURATION.ringSize())
                .boxed()
                .map(RingWorkflows::cell)
                .collect(Collectors.toList());

        assertThat(actualWrittenCells).containsAll(expectedWrittenCells);
    }

    @Test
    public void ringWorkflowSetsSkipRunningToTrueWhenInvalidRingPresent() {
        memoryStore.readWrite(txn -> txn.write(TABLE_NAME, RingWorkflows.cell(0), 99));
        workflow.run();
        assertThat(skipRunning).isTrue();
    }

    @Test
    public void ringWorkflowDoesNotWitnessTransactionWhenSkipRunningSetToTrue() {
        skipRunning.set(true);
        assertThat(workflow.run().history()).isEmpty();
    }
}
