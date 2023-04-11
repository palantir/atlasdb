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
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_WORKLOAD_CELL_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_WORKLOAD_CELL_TWO;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_TWO;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_TWO;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import io.vavr.Tuple2;
import java.util.Optional;
import org.junit.Test;

public final class InMemoryTransactionReplayerTest {

    private static final WitnessedWriteTransactionAction WRITE_TRANSACTION_ACTION_1 =
            ImmutableWitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE);
    private static final WitnessedWriteTransactionAction WRITE_TRANSACTION_ACTION_2 =
            ImmutableWitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_TWO);

    @Test
    public void readActionsAreIgnored() {
        InMemoryTransactionReplayer inMemoryTransactionReplayer = new InMemoryTransactionReplayer();
        inMemoryTransactionReplayer.visit(
                ImmutableWitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.of(VALUE_ONE)));
        assertThat(inMemoryTransactionReplayer.getValues()).isEmpty();
    }

    @Test
    public void writesAreRecorded() {
        InMemoryTransactionReplayer inMemoryTransactionReplayer = new InMemoryTransactionReplayer();
        inMemoryTransactionReplayer.visit(WRITE_TRANSACTION_ACTION_1);
        inMemoryTransactionReplayer.visit(WRITE_TRANSACTION_ACTION_2);
        assertThat(inMemoryTransactionReplayer.getValues())
                .containsExactlyInAnyOrder(
                        new Tuple2<>(TABLE_WORKLOAD_CELL_ONE, Optional.of(VALUE_ONE)),
                        new Tuple2<>(TABLE_WORKLOAD_CELL_TWO, Optional.of(VALUE_TWO)));
    }

    @Test
    public void deletesAreRecorded() {
        InMemoryTransactionReplayer inMemoryTransactionReplayer = new InMemoryTransactionReplayer();
        assertThat(inMemoryTransactionReplayer.getValues()).isEmpty();
        inMemoryTransactionReplayer.visit(WRITE_TRANSACTION_ACTION_1);
        inMemoryTransactionReplayer.visit(WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE));
        inMemoryTransactionReplayer.visit(WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO));
        assertThat(inMemoryTransactionReplayer.getValues())
                .containsExactlyInAnyOrder(
                        new Tuple2<>(TABLE_WORKLOAD_CELL_ONE, Optional.empty()),
                        new Tuple2<>(TABLE_WORKLOAD_CELL_TWO, Optional.empty()));
    }
}
