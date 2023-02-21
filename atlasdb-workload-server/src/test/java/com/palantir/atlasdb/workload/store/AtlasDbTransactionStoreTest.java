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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
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
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class AtlasDbTransactionStoreTest {

    private static final String TABLE = "foo";
    private static final String INDEX_TABLE = TABLE + "_index";
    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);
    private static final TableReference INDEX_REFERENCE =
            TableReference.create(TABLE_REFERENCE.getNamespace(), INDEX_TABLE);

    private static final WorkloadCell WORKLOAD_CELL_ONE =
            ImmutableWorkloadCell.builder().key(50).column(10).build();
    private static final WorkloadCell WORKLOAD_CELL_TWO =
            ImmutableWorkloadCell.builder().key(1257).column(521).build();
    private static final WorkloadCell WORKLOAD_CELL_THREE =
            ImmutableWorkloadCell.builder().key(567).column(405234).build();

    private static final Integer VALUE_ONE = 100;

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
                        AtlasDbUtils.indexMetadata()));
    }

    @Test
    public void createsTables() {
        assertThat(manager.getKeyValueService().getAllTableNames()).contains(TABLE_REFERENCE, INDEX_REFERENCE);
    }

    @Test
    public void canWriteDataToStore() {
        Optional<WitnessedTransaction> witnessedTransactionPrimaryTable =
                store.readWrite(List.of(WriteTransactionAction.of(TABLE, WORKLOAD_CELL_ONE, VALUE_ONE)));
        Optional<WitnessedTransaction> witnessedTransactionIndexTable =
                store.readWrite(List.of(WriteTransactionAction.of(INDEX_TABLE, WORKLOAD_CELL_TWO, VALUE_ONE)));
        assertThat(witnessedTransactionPrimaryTable).isPresent();
        assertThat(witnessedTransactionIndexTable).isPresent();
        assertThat(store.get(TABLE, WORKLOAD_CELL_ONE)).contains(VALUE_ONE);
        assertThat(store.get(INDEX_TABLE, WORKLOAD_CELL_TWO)).contains(VALUE_ONE);
    }

    @Test
    public void witnessedTransactionMaintainsOrder() {
        List<WitnessedTransactionAction> actions = List.of(
                WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_TWO, 100),
                WitnessedReadTransactionAction.of(TABLE, WORKLOAD_CELL_TWO, Optional.of(100)),
                WitnessedReadTransactionAction.of(TABLE, WORKLOAD_CELL_THREE, Optional.empty()),
                WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL_ONE, 24),
                WitnessedReadTransactionAction.of(TABLE, WORKLOAD_CELL_ONE, Optional.of(24)));
        Optional<WitnessedTransaction> maybeTransaction = store.readWrite(actions.stream()
                .map(action -> action.accept(WitnessToActionVisitor.INSTANCE))
                .collect(Collectors.toList()));
        assertThat(maybeTransaction).isPresent();
        WitnessedTransaction transaction = maybeTransaction.get();
        assertThat(transaction.commitTimestamp()).isNotEmpty();
        assertThat(transaction.actions()).containsExactlyElementsOf(actions);
    }

    @Test
    public void readWriteHandlesAllTransactionTypes() {
        store.readWrite(List.of(WriteTransactionAction.of(TABLE, WORKLOAD_CELL_ONE, VALUE_ONE)));
        assertThat(store.readWrite(List.of(ReadTransactionAction.of(TABLE, WORKLOAD_CELL_ONE))))
                .isPresent()
                .map(WitnessedTransaction::actions)
                .map(Iterables::getOnlyElement)
                .map(WitnessedReadTransactionAction.class::cast)
                .map(WitnessedReadTransactionAction::value)
                .contains(Optional.of(VALUE_ONE));
        store.readWrite(List.of(DeleteTransactionAction.of(TABLE, WORKLOAD_CELL_ONE)));
        assertThat(store.get(TABLE, WORKLOAD_CELL_ONE)).isEmpty();
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
        assertThat(transactionStore.readWrite(List.of(ReadTransactionAction.of(TABLE, WORKLOAD_CELL_ONE))))
                .isEmpty();
    }

    @Test
    public void readWriteHandlesReadOnlyTransaction() {
        Optional<WitnessedTransaction> witnessedTransaction =
                store.readWrite(List.of(ReadTransactionAction.of(TABLE, WORKLOAD_CELL_ONE)));
        assertThat(witnessedTransaction).isPresent();
        assertThat(witnessedTransaction.get().commitTimestamp()).isEmpty();
    }
}
