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

package com.palantir.atlasdb.workload.workflow.random;

import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.RowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Value;

public final class RandomTransactions {
    private RandomTransactions() {
        // utility
    }

    interface RandomTransactionAction {}

    @Value.Immutable
    interface DirectRandomTransactionAction extends RandomTransactionAction {
        @Value.Parameter
        TransactionAction action();

        default void accept(RandomTransactionActionVisitor visitor) {
            visitor.visit(this);
        }
    }

    @Value.Immutable
    interface RangeIteratorProgressionRandomTransactionAction extends RandomTransactionAction {
        @Value.Parameter
        int specificRow();

        @Value.Parameter
        UUID correlationId();

        default void accept(RandomTransactionActionVisitor visitor) {
            visitor.visit(this);
        }
    }

    interface RandomTransactionActionVisitor {
        void visit(DirectRandomTransactionAction action);

        void visit(RangeIteratorProgressionRandomTransactionAction action);
    }

    static class InteractiveExecutionRandomTransactionActionVisitor implements RandomTransactionActionVisitor {
        private final InteractiveTransaction txn;
        private final Map<>

        InteractiveExecutionRandomTransactionActionVisitor(InteractiveTransaction txn) {
            this.txn = txn;
        }

        @Override
        public void visit(DirectRandomTransactionAction action) {
            action.action().accept(new TransactionActionVisitor<Void>() {
                @Override
                public Void visit(ReadTransactionAction readTransactionAction) {
                    txn.read(readTransactionAction.table(), readTransactionAction.cell());
                    return null;
                }

                @Override
                public Void visit(WriteTransactionAction writeTransactionAction) {
                    txn.write(writeTransactionAction.table(), writeTransactionAction.cell(), writeTransactionAction.value());
                    return null;
                }

                @Override
                public Void visit(DeleteTransactionAction deleteTransactionAction) {
                    txn.delete(deleteTransactionAction.table(), deleteTransactionAction.cell());
                    return null;
                }

                @Override
                public Void visit(RowsColumnRangeReadTransactionAction rowsColumnRangeReadTransactionAction) {
                    throw new UnsupportedOperationException("Row column range reads are not expected to be embedded into direct random transaction actions");
                }
            });
        }

        @Override
        public void visit(RangeIteratorProgressionRandomTransactionAction action) {
        }
    }

    static class RandomTransactionTask {
        private final List<RandomTransactionAction> randomTransactionActions;

        public Optional<WitnessedTransaction> run(InteractiveTransactionStore store) {
            store.readWrite(txn -> {
                for (RandomTransactionAction action : randomTransactionActions) {

                }
            });
        }
    }
}
