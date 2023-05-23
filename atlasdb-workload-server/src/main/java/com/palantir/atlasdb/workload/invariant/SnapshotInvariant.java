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

import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransactionAction;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import io.vavr.collection.Map;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import one.util.streamex.StreamEx;

/**
 * This invariant checks that the snapshot isolation property is maintained.
 * <p>
 * The way this check works, is by replaying all transactions sorted by their effective timestamp. Transactions are
 * replayed to an immutable map, allowing us to track each table's state at any given time. These table views are
 * persisted to a {@link java.util.NavigableMap} by the commit timestamp of each transaction, with the value being
 * the version of the immutable map at that point-in-time.
 * <p>
 * Reads are validated by comparing values from the view of a table at the start timestamp (read view),
 * and comparing to what we've witnessed. Local writes are replayed onto the read view to ensure we
 * don't flag false-positives, but is not witnessed by other transactions.
 * <p>
 * Writes are validated by first checking that the value we're writing to hasn't changed between the read view and the
 * latest view. This would indicate a write-write conflict miss, as we should've conflicted with this transaction.
 * Otherwise, once the transaction is replayed on top of the latest view, we persist this new view at the
 * commit timestamp.
 */
public enum SnapshotInvariant implements TransactionInvariant {
    INSTANCE;

    @Override
    public void accept(
            WorkflowHistory workflowHistory, Consumer<List<InvalidWitnessedTransaction>> invalidWitnessedTransactions) {
        VersionedTableView<TableAndWorkloadCell, ValueAndTimestamp> tableView = new VersionedTableView<>();
        List<InvalidWitnessedTransaction> transactions = StreamEx.of(workflowHistory.history())
                .mapPartial(witnessedTransaction -> {
                    StructureHolder<Map<TableAndWorkloadCell, ValueAndTimestamp>> latestTableView =
                            tableView.getLatestTableView();

                    SnapshotInvariantVisitor visitor = new SnapshotInvariantVisitor(
                            witnessedTransaction.startTimestamp(),
                            latestTableView,
                            tableView.getView(witnessedTransaction.startTimestamp()));

                    List<InvalidWitnessedTransactionAction> invalidTransactions = StreamEx.of(
                                    witnessedTransaction.actions())
                            .mapPartial(action -> action.accept(visitor))
                            .toList();

                    witnessedTransaction
                            .commitTimestamp()
                            .ifPresent(
                                    commitTimestamp -> tableView.put(commitTimestamp, latestTableView.getSnapshot()));

                    if (invalidTransactions.isEmpty()) {
                        return Optional.empty();
                    }

                    return Optional.of(InvalidWitnessedTransaction.of(witnessedTransaction, invalidTransactions));
                })
                .toList();
        invalidWitnessedTransactions.accept(transactions);
    }
}
