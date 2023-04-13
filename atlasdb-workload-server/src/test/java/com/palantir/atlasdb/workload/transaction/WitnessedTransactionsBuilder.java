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

import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.logsafe.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class WitnessedTransactionsBuilder {

    private final List<WitnessedTransaction> witnessedTransactions = new ArrayList<>();

    private final AtomicLong timestampCounter = new AtomicLong();
    private final String table;

    public WitnessedTransactionsBuilder(String table) {
        this.table = table;
    }

    public WitnessedTransactionBuilder startTransaction() {
        return new WitnessedTransactionBuilder(timestampCounter.incrementAndGet());
    }

    public List<WitnessedTransaction> build() {
        return List.copyOf(witnessedTransactions);
    }

    public final class WitnessedTransactionBuilder {
        private final List<WitnessedTransactionAction> actions = new ArrayList<>();

        private boolean needsCommitTimestamp = false;

        private final long startTimestamp;

        public WitnessedTransactionBuilder(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public WitnessedTransactionBuilder read(Integer row, Integer column, Optional<Integer> valueRead) {
            actions.add(WitnessedReadTransactionAction.of(table, ImmutableWorkloadCell.of(row, column), valueRead));
            return this;
        }

        public WitnessedTransactionBuilder read(Integer row, Integer column, Integer valueRead) {
            return read(row, column, Optional.of(valueRead));
        }

        public WitnessedTransactionBuilder read(Integer row, Integer column) {
            return read(row, column, Optional.empty());
        }

        public WitnessedTransactionBuilder write(Integer row, Integer column, Integer value) {
            actions.add(WitnessedWriteTransactionAction.of(table, ImmutableWorkloadCell.of(row, column), value));
            needsCommitTimestamp = true;
            return this;
        }

        public WitnessedTransactionBuilder delete(Integer row, Integer column) {
            actions.add(WitnessedDeleteTransactionAction.of(table, ImmutableWorkloadCell.of(row, column)));
            needsCommitTimestamp = true;
            return this;
        }

        public WitnessedTransactionsBuilder endTransaction() {
            Optional<Long> commitTimestamp =
                    needsCommitTimestamp ? Optional.of(timestampCounter.incrementAndGet()) : Optional.empty();
            return endTransaction(commitTimestamp);
        }

        public WitnessedTransactionsBuilder endTransaction(Long commitTimestamp) {
            return endTransaction(Optional.of(commitTimestamp));
        }

        public WitnessedTransactionsBuilder endTransaction(Optional<Long> commitTimestamp) {
            if (needsCommitTimestamp) {
                Preconditions.checkArgument(
                        commitTimestamp.isPresent(),
                        "Commit timestamp must be provided when the transaction has any writes or deletes.");
                Preconditions.checkArgument(
                        startTimestamp < commitTimestamp.get(),
                        "Commit timestamp must be greater than the start timestamp.");
            }
            witnessedTransactions.add(ImmutableWitnessedTransaction.builder()
                    .startTimestamp(startTimestamp)
                    .actions(actions)
                    .commitTimestamp(commitTimestamp)
                    .build());
            return WitnessedTransactionsBuilder.this;
        }
    }
}
