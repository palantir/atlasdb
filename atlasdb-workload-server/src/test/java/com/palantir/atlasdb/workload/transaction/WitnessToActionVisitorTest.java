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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import java.util.Optional;
import org.junit.Test;

public final class WitnessToActionVisitorTest {

    private static final String TABLE = "table";
    private static final WorkloadCell WORKLOAD_CELL = ImmutableWorkloadCell.of(523, 2999);
    private static final Integer VALUE = 912;

    @Test
    public void convertsReadsCorrectly() {
        WitnessedReadTransactionAction witnessedRead =
                WitnessedReadTransactionAction.of(TABLE, WORKLOAD_CELL, Optional.of(VALUE));
        ReadTransactionAction read = WitnessToActionVisitor.INSTANCE.visit(witnessedRead);
        assertThat(read.table()).isEqualTo(witnessedRead.table());
        assertThat(read.cell()).isEqualTo(witnessedRead.cell());
    }

    @Test
    public void convertsWritesCorrectly() {
        WitnessedWriteTransactionAction witnessedWrite =
                WitnessedWriteTransactionAction.of(TABLE, WORKLOAD_CELL, VALUE);
        WriteTransactionAction write = WitnessToActionVisitor.INSTANCE.visit(witnessedWrite);
        assertThat(write.table()).isEqualTo(witnessedWrite.table());
        assertThat(write.cell()).isEqualTo(witnessedWrite.cell());
        assertThat(write.value()).isEqualTo(witnessedWrite.value());
    }

    @Test
    public void convertsDeletesCorrectly() {
        WitnessedDeleteTransactionAction witnessedDelete = WitnessedDeleteTransactionAction.of(TABLE, WORKLOAD_CELL);
        DeleteTransactionAction delete = WitnessToActionVisitor.INSTANCE.visit(witnessedDelete);
        assertThat(delete.table()).isEqualTo(witnessedDelete.table());
        assertThat(delete.cell()).isEqualTo(witnessedDelete.cell());
    }
}
