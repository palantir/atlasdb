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

package com.palantir.atlasdb.workload.store;

import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.INDEX_REFERENCE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_1_INDEX_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_REFERENCE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_THREE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_TWO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.WitnessToActionVisitor;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public final class AtlasDbTransactionStoreTest {

    private TransactionManager manager;

    private AtlasDbTransactionStore store;

    @Before
    public void before() {
        manager = TransactionManagers.createInMemory(Set.of());
        store = AtlasDbTransactionStore.create(
                manager,
                Map.of(
                        TABLE_REFERENCE,
                        AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE),
                        INDEX_REFERENCE,
                        AtlasDbUtils.indexMetadata(ConflictHandler.SERIALIZABLE)));
    }

    @Test
    public void createsTables() {
        assertThat(manager.getKeyValueService().getAllTableNames()).contains(TABLE_REFERENCE, INDEX_REFERENCE);
    }

    @Test
    public void canWriteDataToStore() {
        Optional<WitnessedTransaction> witnessedTransactionPrimaryTable =
                store.readWrite(List.of(WriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE)));
        Optional<WitnessedTransaction> witnessedTransactionIndexTable =
                store.readWrite(List.of(WriteTransactionAction.of(TABLE_1_INDEX_1, WORKLOAD_CELL_TWO, VALUE_ONE)));
        assertThat(witnessedTransactionPrimaryTable).isPresent();
        assertThat(witnessedTransactionIndexTable).isPresent();
        assertThat(store.get(TABLE_1, WORKLOAD_CELL_ONE)).contains(VALUE_ONE);
        assertThat(store.get(TABLE_1_INDEX_1, WORKLOAD_CELL_TWO)).contains(VALUE_ONE);
    }

    @Test
    public void witnessedTransactionMaintainsOrder() {
        List<WitnessedTransactionAction> actions = List.of(
                WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, 100),
                WitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, Optional.of(100)),
                WitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_THREE, Optional.empty()),
                WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, 24),
                WitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.of(24)));
        Optional<WitnessedTransaction> maybeTransaction = store.readWrite(actions.stream()
                .map(action -> action.accept(WitnessToActionVisitor.INSTANCE))
                .collect(Collectors.toList()));
        assertThat(maybeTransaction).isPresent().hasValueSatisfying(txn -> {
            assertThat(txn.actions()).containsExactlyElementsOf(actions);
            assertThat(txn.commitTimestamp()).isNotEmpty();
        });
    }

    @Test
    public void readWriteHandlesAllTransactionTypes() {
        store.readWrite(List.of(WriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE)));
        assertThat(store.readWrite(List.of(ReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE))))
                .isPresent()
                .map(WitnessedTransaction::actions)
                .map(Iterables::getOnlyElement)
                .map(WitnessedReadTransactionAction.class::cast)
                .map(WitnessedReadTransactionAction::value)
                .contains(Optional.of(VALUE_ONE));
        store.readWrite(List.of(DeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE)));
        assertThat(store.get(TABLE_1, WORKLOAD_CELL_ONE)).isEmpty();
    }

    @Test
    public void readWriteThrowsWhenTableDoesNotExist() {
        assertThatThrownBy(() -> store.readWrite(List.of(ReadTransactionAction.of("chocolate", WORKLOAD_CELL_ONE))))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Transaction action has unknown table.");
    }

    @Test
    public void readWriteReturnsEmptyWhenExceptionThrown() {
        TransactionManager transactionManager = mock(TransactionManager.class);
        KeyValueService keyValueService = mock(KeyValueService.class);
        when(transactionManager.getKeyValueService()).thenReturn(keyValueService);
        when(transactionManager.runTaskWithRetry(any())).thenThrow(new RuntimeException());
        AtlasDbTransactionStore transactionStore = AtlasDbTransactionStore.create(
                transactionManager, Map.of(TABLE_REFERENCE, AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE)));
        assertThat(transactionStore.readWrite(List.of(ReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE))))
                .isEmpty();
    }

    @Test
    public void readWriteHandlesReadOnlyTransaction() {
        Optional<WitnessedTransaction> witnessedTransaction =
                store.readWrite(List.of(ReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE)));
        assertThat(witnessedTransaction).isPresent();
        assertThat(witnessedTransaction.get().commitTimestamp()).isEmpty();
    }
}
