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

package com.palantir.atlasdb.workload.workflow;

import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.MaybeWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionVisitor;
import java.util.Optional;

public final class DefaultWitnessedTransactionVisitor
        implements WitnessedTransactionVisitor<Optional<WitnessedTransaction>> {

    private final ReadOnlyTransactionStore readOnlyTransactionStore;

    public DefaultWitnessedTransactionVisitor(ReadOnlyTransactionStore readOnlyTransactionStore) {
        this.readOnlyTransactionStore = readOnlyTransactionStore;
    }

    @Override
    public Optional<WitnessedTransaction> visit(FullyWitnessedTransaction witnessedTransaction) {
        return Optional.of(witnessedTransaction);
    }

    @Override
    public Optional<WitnessedTransaction> visit(MaybeWitnessedTransaction maybeWitnessedTransaction) {
        return readOnlyTransactionStore.isCommitted(maybeWitnessedTransaction.startTimestamp())
                ? Optional.of(maybeWitnessedTransaction)
                : Optional.empty();
    }
}
