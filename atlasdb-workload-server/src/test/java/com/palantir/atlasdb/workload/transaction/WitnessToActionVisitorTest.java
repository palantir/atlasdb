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
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedWriteTransactionAction;
import java.util.Optional;
import org.junit.Test;

public class WitnessToActionVisitorTest {

    private static final WorkloadCell WORKLOAD_CELL = ImmutableWorkloadCell.of(523, 2999);
    private static final Integer VALUE = 912;

    @Test
    public void convertsReadsCorrectly() {
        ReadTransactionAction action = WitnessToActionVisitor.INSTANCE.visit(
                ImmutableWitnessedReadTransactionAction.of(WORKLOAD_CELL, Optional.of(VALUE)));
        assertThat(action.cell()).isEqualTo(WORKLOAD_CELL);
    }

    @Test
    public void convertsWritesCorrectly() {
        WriteTransactionAction action = WitnessToActionVisitor.INSTANCE.visit(
                ImmutableWitnessedWriteTransactionAction.of(WORKLOAD_CELL, VALUE));
        assertThat(action.cell()).isEqualTo(WORKLOAD_CELL);
        assertThat(action.value()).isEqualTo(VALUE);
    }

    @Test
    public void convertsDeletesCorrectly() {
        DeleteTransactionAction action =
                WitnessToActionVisitor.INSTANCE.visit(ImmutableWitnessedDeleteTransactionAction.of(WORKLOAD_CELL));
        assertThat(action.cell()).isEqualTo(WORKLOAD_CELL);
    }
}
