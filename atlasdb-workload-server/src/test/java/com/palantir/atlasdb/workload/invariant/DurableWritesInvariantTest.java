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
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.TableWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
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

    private static final String TABLE = "foo";
    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);

    private static final WorkloadCell WORKLOAD_CELL_1 = ImmutableWorkloadCell.of(1, 2);
    private static final WorkloadCell WORKLOAD_CELL_2 = ImmutableWorkloadCell.of(3, 4);
    private static final Integer VALUE_ONE = 5;
    private static final Integer VALUE_TWO = 3;

    private TransactionManager manager;
    private AtlasDbTransactionStore store;

    @Before
    public void before() {
        manager = TransactionManagers.createInMemory(Set.of());
        store = AtlasDbTransactionStore.create(
                manager, Map.of(TABLE_REFERENCE, AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE)));
    }

    @Test
    public void undeletedCellsAreFoundAndDeletedAreIgnored() {
        AtomicReference<Map<TableWorkloadCell, Integer>> undeletedCells = new AtomicReference<>();

        DurableWritesInvariant durableWritesInvariant = new DurableWritesInvariant(_ignore -> {}, undeletedCells::set);
        List<WitnessedTransaction> witnessedTransactions = List.of(
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE, WORKLOAD_CELL_1))
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE, WORKLOAD_CELL_2))
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .build());
        WorkflowHistory history = ImmutableWorkflowHistory.builder()
                .history(witnessedTransactions)
                .transactionStore(store)
                .build();

        store.readWrite(List.of(WriteTransactionAction.of(TABLE, WORKLOAD_CELL_2, VALUE_ONE)));
        durableWritesInvariant.accept(history);
        assertThat(undeletedCells.get())
                .containsExactlyEntriesOf(Map.of(TableWorkloadCell.of(TABLE, WORKLOAD_CELL_2), VALUE_ONE));
    }

    @Test
    public void cellsThatDoNotMatchAreFoundAndMatchingAreIgnored() {
        AtomicReference<Map<TableWorkloadCell, MismatchedValue>> mismatchingCells = new AtomicReference<>();
        DurableWritesInvariant durableWritesInvariant =
                new DurableWritesInvariant(mismatchingCells::set, _ignore -> {});
        List<WitnessedTransaction> witnessedTransactions = List.of(
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_1, VALUE_ONE))
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_2, VALUE_TWO))
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .build());
        WorkflowHistory history = ImmutableWorkflowHistory.builder()
                .history(witnessedTransactions)
                .transactionStore(store)
                .build();

        store.readWrite(List.of(
                WriteTransactionAction.of(TABLE, WORKLOAD_CELL_1, VALUE_ONE),
                WriteTransactionAction.of(TABLE, WORKLOAD_CELL_2, VALUE_ONE)));
        durableWritesInvariant.accept(history);
        assertThat(mismatchingCells.get())
                .containsExactlyEntriesOf(Map.of(
                        TableWorkloadCell.of(TABLE, WORKLOAD_CELL_2),
                        MismatchedValue.of(Optional.of(VALUE_ONE), Optional.of(VALUE_TWO))));
    }
}
