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

public final class SnapshotIsolationInvariant implements TransactionInvariant {
    @Override
    public void accept(
            WorkflowHistory workflowHistory, Consumer<List<InvalidWitnessedTransaction>> invalidWitnessedTransactions) {
        TableView tableView = new TableView();
        List<InvalidWitnessedTransaction> transactions = StreamEx.of(workflowHistory.history())
                .mapPartial(witnessedTransaction -> {
                    StructureHolder<Map<TableAndWorkloadCell, ValueAndTimestamp>> latestTableView =
                            StructureHolder.create(tableView::getLatestTableView);
                    StructureHolder<Map<TableAndWorkloadCell, ValueAndTimestamp>> readView =
                            StructureHolder.create(() -> tableView.getView(witnessedTransaction.startTimestamp()));
                    SnapshotIsolationInvariantVisitor visitor = new SnapshotIsolationInvariantVisitor(
                            witnessedTransaction.startTimestamp(), latestTableView, readView);
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
