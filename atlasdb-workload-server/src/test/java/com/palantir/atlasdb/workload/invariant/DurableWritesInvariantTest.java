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

import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_REFERENCE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_TWO;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_TWO;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

public final class DurableWritesInvariantTest {

    private AtlasDbTransactionStore store;

    @Before
    public void before() {
        store = AtlasDbTransactionStore.create(
                TransactionManagers.createInMemory(Set.of()),
                Map.of(TABLE_REFERENCE, AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE)));
    }

    @Test
    public void cellsExpectedToBeDeletedAreFound() {
        AtomicReference<Map<TableAndWorkloadCell, MismatchedValue>> mismatchingCells = new AtomicReference<>();
        List<WitnessedTransaction> witnessedTransactions = List.of(
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE))
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO))
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .build());
        WorkflowHistory history = ImmutableWorkflowHistory.builder()
                .history(witnessedTransactions)
                .transactionStore(store)
                .build();

        store.readWrite(List.of(WriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE)));
        DurableWritesInvariant.INSTANCE.accept(history, mismatchingCells::set);
        assertThat(mismatchingCells.get())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        TableAndWorkloadCell.of(TABLE_1, WORKLOAD_CELL_TWO),
                        MismatchedValue.of(Optional.of(VALUE_ONE), Optional.empty())));
    }

    @Test
    public void cellsThatDoNotMatchAreFoundAndMatchingAreIgnored() {
        AtomicReference<Map<TableAndWorkloadCell, MismatchedValue>> mismatchingCells = new AtomicReference<>();
        List<WitnessedTransaction> witnessedTransactions = List.of(
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE))
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_TWO))
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .build());
        WorkflowHistory history = ImmutableWorkflowHistory.builder()
                .history(witnessedTransactions)
                .transactionStore(store)
                .build();

        store.readWrite(List.of(
                WriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE),
                WriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE)));
        DurableWritesInvariant.INSTANCE.accept(history, mismatchingCells::set);
        assertThat(mismatchingCells.get())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        TableAndWorkloadCell.of(TABLE_1, WORKLOAD_CELL_TWO),
                        MismatchedValue.of(Optional.of(VALUE_ONE), Optional.of(VALUE_TWO))));
    }
}
