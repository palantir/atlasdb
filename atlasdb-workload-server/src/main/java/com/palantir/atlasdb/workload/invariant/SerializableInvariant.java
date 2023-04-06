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

package com.palantir.atlasdb.workload.invariant;

import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.transaction.InMemoryTransactionReplayer;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import one.util.streamex.StreamEx;

public enum SerializableInvariant implements Invariant<List<InvalidWitnessedTransaction>> {
    INSTANCE;

    @Override
    public void accept(
            WorkflowHistory workflowHistory, Consumer<List<InvalidWitnessedTransaction>> invalidWitnessedTransactions) {
        SerializableInvariantVisitor visitor = new SerializableInvariantVisitor();
        List<InvalidWitnessedTransaction> transactions = StreamEx.of(workflowHistory.history())
                .sequential()
                .mapPartial(witnessedTransaction -> {
                    List<InvalidWitnessedTransactionAction> invalidTransactions = StreamEx.of(
                                    witnessedTransaction.actions())
                            .sequential()
                            .mapPartial(action -> action.accept(visitor))
                            .toList();

                    if (invalidTransactions.isEmpty()) {
                        return Optional.empty();
                    }

                    return Optional.of(InvalidWitnessedTransaction.of(witnessedTransaction, invalidTransactions));
                })
                .collect(Collectors.toList());
        invalidWitnessedTransactions.accept(transactions);
    }

    private static final class SerializableInvariantVisitor
            implements WitnessedTransactionActionVisitor<Optional<InvalidWitnessedTransactionAction>> {

        private final InMemoryTransactionReplayer inMemoryTransactionReplayer = new InMemoryTransactionReplayer();

        @Override
        public Optional<InvalidWitnessedTransactionAction> visit(WitnessedReadTransactionAction readTransactionAction) {
            Optional<Integer> expectedValue = inMemoryTransactionReplayer
                    .getValues()
                    .get(TableAndWorkloadCell.of(readTransactionAction.table(), readTransactionAction.cell()))
                    .toJavaOptional()
                    .orElseGet(Optional::empty);

            if (!expectedValue.equals(readTransactionAction.value())) {
                return Optional.of(InvalidWitnessedTransactionAction.of(
                        readTransactionAction, MismatchedValue.of(readTransactionAction.value(), expectedValue)));
            }

            return Optional.empty();
        }

        @Override
        public Optional<InvalidWitnessedTransactionAction> visit(
                WitnessedWriteTransactionAction writeTransactionAction) {
            inMemoryTransactionReplayer.visit(writeTransactionAction);
            return Optional.empty();
        }

        @Override
        public Optional<InvalidWitnessedTransactionAction> visit(
                WitnessedDeleteTransactionAction deleteTransactionAction) {
            inMemoryTransactionReplayer.visit(deleteTransactionAction);
            return Optional.empty();
        }
    }
}
