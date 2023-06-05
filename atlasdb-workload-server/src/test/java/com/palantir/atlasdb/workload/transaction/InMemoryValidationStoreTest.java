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

import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_TWO;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_TWO;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.workload.store.InMemoryValidationStore;
import com.palantir.atlasdb.workload.store.ValidationStore;
import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import java.util.List;
import org.junit.Test;

public final class InMemoryValidationStoreTest {

    @Test
    public void writeActionsPersistValues() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of(
                FullyWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE))
                        .build(),
                FullyWitnessedTransaction.builder()
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_TWO))
                        .build()));
        assertThat(store.values()).hasSize(1);
        assertThat(store.values().get(TABLE_1).get()).satisfies(table -> {
            assertThat(table.get(WORKLOAD_CELL_ONE.key(), WORKLOAD_CELL_ONE.column()))
                    .contains(VALUE_ONE);
            assertThat(table.get(WORKLOAD_CELL_TWO.key(), WORKLOAD_CELL_TWO.column()))
                    .contains(VALUE_TWO);
        });
    }

    @Test
    public void deleteActionsPutsEmptyOptionalForCell() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of(
                FullyWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE))
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE))
                        .build(),
                FullyWitnessedTransaction.builder()
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE))
                        .build()));
        assertThat(store.values()).hasSize(1);
        assertThat(store.values().get(TABLE_1).get()).satisfies(table -> {
            assertThat(table.get(WORKLOAD_CELL_ONE.key(), WORKLOAD_CELL_ONE.column()))
                    .isEmpty();
            assertThat(table.containsKey(WORKLOAD_CELL_ONE.key(), WORKLOAD_CELL_ONE.column()))
                    .as("a tombstone should have been explicitly written")
                    .isTrue();
            assertThat(table.get(WORKLOAD_CELL_TWO.key(), WORKLOAD_CELL_TWO.column()))
                    .contains(VALUE_TWO);
        });
    }

    @Test
    public void writesDeletesAreProcessedInOrder() {
        InMemoryValidationStore store = InMemoryValidationStore.create(List.of(
                FullyWitnessedTransaction.builder()
                        .startTimestamp(1)
                        .commitTimestamp(2)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE))
                        .build(),
                FullyWitnessedTransaction.builder()
                        .startTimestamp(3)
                        .commitTimestamp(4)
                        .addActions(WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE))
                        .build(),
                FullyWitnessedTransaction.builder()
                        .startTimestamp(5)
                        .commitTimestamp(6)
                        .addActions(WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_TWO))
                        .build()));
        assertThat(store.values()).hasSize(1);
        assertThat(store.values().get(TABLE_1).get()).satisfies(table -> {
            assertThat(table.get(WORKLOAD_CELL_ONE.key(), WORKLOAD_CELL_ONE.column()))
                    .contains(VALUE_TWO);
        });
    }

    @Test
    public void valuesCannotBeModified() {
        ValidationStore validationStore = InMemoryValidationStore.create(List.of());
        validationStore.values().get(TABLE_1).get().put(WORKLOAD_CELL_ONE.key(), WORKLOAD_CELL_ONE.column(), VALUE_ONE);
        assertThat(validationStore.values()).isEmpty();
    }
}
