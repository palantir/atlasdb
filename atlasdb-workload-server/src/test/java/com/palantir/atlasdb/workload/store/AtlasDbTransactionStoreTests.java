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

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.transaction.ImmutableWriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.*;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AtlasDbTransactionStoreTests {

    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "foo");
    private static final TableReference INDEX_REFERENCE =
            TableReference.create(TABLE_REFERENCE.getNamespace(), TABLE_REFERENCE.getTableName() + "_index");

    private static final WorkloadCell WORKLOAD_CELL_ONE =
            ImmutableWorkloadCell.builder().key(50).column(10).build();
    private static final WorkloadCell WORKLOAD_CELL_TWO =
            ImmutableWorkloadCell.builder().key(1257).column(521).build();
    private static final WorkloadCell WORKLOAD_CELL_THREE =
            ImmutableWorkloadCell.builder().key(567).column(405234).build();

    private TransactionManager manager;

    @Before
    public void before() {
        manager = TransactionManagers.createInMemory(Set.of());
    }

    @Test
    public void createsTableAndIndex() {
        AtlasDbTransactionStore.create(manager, TABLE_REFERENCE, ConflictHandler.SERIALIZABLE);
        assertThat(manager.getKeyValueService().getAllTableNames()).contains(TABLE_REFERENCE, INDEX_REFERENCE);
    }

    @Test
    public void canWriteDataToStore() {
        AtlasDbTransactionStore store =
                AtlasDbTransactionStore.create(manager, TABLE_REFERENCE, ConflictHandler.SERIALIZABLE);
        WorkloadCell workloadCell =
                ImmutableWorkloadCell.builder().key(50).column(10).build();
        Integer value = 100;
        Optional<WitnessedTransaction> witnessedTransaction =
                store.readWrite(List.of(ImmutableWriteTransactionAction.builder()
                        .cell(workloadCell)
                        .value(value)
                        .build()));
        assertThat(witnessedTransaction).isPresent();
        assertThat(store.get(workloadCell)).contains(value);
    }

    @Test
    public void witnessedTransactionMaintainsOrder() {
        AtlasDbTransactionStore store =
                AtlasDbTransactionStore.create(manager, TABLE_REFERENCE, ConflictHandler.SERIALIZABLE);
        List<WitnessedTransactionAction> actions = List.of(
                ImmutableWitnessedWriteTransactionAction.of(WORKLOAD_CELL_TWO, 100),
                ImmutableWitnessedReadTransactionAction.of(WORKLOAD_CELL_TWO, Optional.of(100)),
                ImmutableWitnessedReadTransactionAction.of(WORKLOAD_CELL_THREE, Optional.empty()),
                ImmutableWitnessedWriteTransactionAction.of(WORKLOAD_CELL_ONE, 24),
                ImmutableWitnessedReadTransactionAction.of(WORKLOAD_CELL_ONE, Optional.of(24)));
        WitnessToActionVisitor visitor = new WitnessToActionVisitor();
        Optional<WitnessedTransaction> maybeTransaction = store.readWrite(
                actions.stream().map(action -> action.accept(visitor)).collect(Collectors.toList()));
        assertThat(maybeTransaction)
                .isPresent()
                .map(WitnessedTransaction::actions)
                .contains(actions);
    }
}
