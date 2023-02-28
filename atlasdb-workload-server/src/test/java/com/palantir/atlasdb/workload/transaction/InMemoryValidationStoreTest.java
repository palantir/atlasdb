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

package com.palantir.atlasdb.workload.transaction;

import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_WORKLOAD_CELL_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_WORKLOAD_CELL_2;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_TWO;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public final class InMemoryValidationStoreTest {

    @Test
    public void writeActionsAreHandled() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of(
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_1, VALUE_ONE))
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_2, VALUE_TWO))
                        .build()));
        assertThat(store.values())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(TABLE_WORKLOAD_CELL_1, VALUE_ONE, TABLE_WORKLOAD_CELL_2, VALUE_TWO));
        assertThat(store.deletedCells()).isEmpty();
    }

    @Test
    public void deletesAreHandled() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of(
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_1, VALUE_ONE))
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_2, VALUE_ONE))
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE, WORKLOAD_CELL_1))
                        .build()));
        assertThat(store.values()).containsExactlyEntriesOf(Map.of(TABLE_WORKLOAD_CELL_2, VALUE_ONE));
        assertThat(store.deletedCells()).containsExactlyInAnyOrder(TABLE_WORKLOAD_CELL_1);
    }

    @Test
    public void writesDeletesAreHandledInOrder() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of(
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_1, VALUE_ONE))
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE, WORKLOAD_CELL_1))
                        .build(),
                ImmutableWitnessedTransaction.builder()
                        .startTimestamp(5)
                        .commitTimestamp(6)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_1, VALUE_TWO))
                        .build()));
        assertThat(store.values()).containsExactlyEntriesOf(Map.of(TABLE_WORKLOAD_CELL_1, VALUE_TWO));
        assertThat(store.deletedCells()).isEmpty();
    }

    @Test
    public void valuesCannotBeModified() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of());
        assertThatThrownBy(() -> store.values().remove(new Object())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.values().put(TABLE_WORKLOAD_CELL_1, 10))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void deletedCellsCannotBeModified() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of());
        assertThatThrownBy(() -> store.deletedCells().remove(new Object()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.deletedCells().add(TABLE_WORKLOAD_CELL_1))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
