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

package com.palantir.atlasdb.workload.transaction.witnessed.range;

import com.palantir.atlasdb.workload.store.CellReferenceAndValue;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.logsafe.Preconditions;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
public interface WitnessedRowsColumnRangeReadTransactionAction extends WitnessedTransactionAction {
    UUID iteratorIdentifier();

    String table();

    int specificRow();

    Optional<WorkloadCell> cell();

    Optional<Integer> value();

    @Value.Lazy
    default Optional<CellReferenceAndValue> cellReferenceAndValue() {
        return cell().map(presentCell -> CellReferenceAndValue.builder()
                .tableAndWorkloadCell(TableAndWorkloadCell.of(table(), presentCell))
                .value(value())
                .build());
    }

    @Override
    default <T> T accept(WitnessedTransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(
                cell().isPresent() == value().isPresent(), "Either both or neither of cell and value must be present");
    }

    static ImmutableWitnessedRowsColumnRangeReadTransactionAction.Builder builder() {
        return ImmutableWitnessedRowsColumnRangeReadTransactionAction.builder();
    }
}
