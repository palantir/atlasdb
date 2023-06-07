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

import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;

public enum WitnessToActionVisitor implements WitnessedTransactionActionVisitor<TransactionAction> {
    INSTANCE;

    @Override
    public SingleCellReadTransactionAction visit(WitnessedSingleCellReadTransactionAction readTransactionAction) {
        return ImmutableSingleCellReadTransactionAction.of(readTransactionAction.table(), readTransactionAction.cell());
    }

    @Override
    public WriteTransactionAction visit(WitnessedWriteTransactionAction writeTransactionAction) {
        return ImmutableWriteTransactionAction.of(
                writeTransactionAction.table(), writeTransactionAction.cell(), writeTransactionAction.value());
    }

    @Override
    public DeleteTransactionAction visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
        return ImmutableDeleteTransactionAction.of(deleteTransactionAction.table(), deleteTransactionAction.cell());
    }

    @Override
    public RowColumnRangeReadTransactionAction visit(WitnessedRowColumnRangeReadTransactionAction rowColumnRangeReadTransactionAction) {
        return rowColumnRangeReadTransactionAction.originalQuery();
    }

    @Override
    public RowRangeReadTransactionAction visit(WitnessedRowRangeReadTransactionAction rowReadTransactionAction) {
        return rowReadTransactionAction.originalQuery();
    }
}
