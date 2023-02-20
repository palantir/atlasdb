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
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;

public enum WitnessToActionVisitor implements WitnessedTransactionActionVisitor<TransactionAction> {
    INSTANCE;

    @Override
    public ReadTransactionAction visit(WitnessedReadTransactionAction readTransactionAction) {
        return ImmutableReadTransactionAction.of(readTransactionAction.cell());
    }

    @Override
    public WriteTransactionAction visit(WitnessedWriteTransactionAction writeTransactionAction) {
        return ImmutableWriteTransactionAction.of(writeTransactionAction.cell(), writeTransactionAction.value());
    }

    @Override
    public DeleteTransactionAction visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
        return ImmutableDeleteTransactionAction.of(deleteTransactionAction.cell());
    }
}
