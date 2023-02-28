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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table;
import com.palantir.atlasdb.workload.Invariant;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

public class DurableWritesInvariant implements Invariant {

    private static final SafeLogger log = SafeLoggerFactory.get(AtlasDbTransactionStore.class);

    @Override
    public void accept(WorkflowHistory workflowHistory) {
        InMemoryKvsTransactionReplayer replayer = new InMemoryKvsTransactionReplayer();
        workflowHistory.history().stream()
                .map(WitnessedTransaction::actions)
                .forEach(witnessedTransactionActions ->
                        witnessedTransactionActions.forEach(action -> action.accept(replayer)));
        replayer.getTables().forEach(tableName -> {
            checkCellsExist(tableName, replayer.getValues(tableName), workflowHistory.transactionStore());
            checkCellsAreDeleted(tableName, replayer.getDeletedCells(tableName), workflowHistory.transactionStore());
        });
    }

    private static void checkCellsExist(
            String tableName, Map<WorkloadCell, Integer> expectedCells, ReadableTransactionStore store) {
        expectedCells.forEach((cell, value) -> {
            Optional<Integer> actualValue = store.get(tableName, cell);
            boolean equalValues = actualValue.map(value::equals).orElse(false);
            if (!equalValues) {
                log.error(
                        "InMemoryKvs does not match external KVS.",
                        SafeArg.of("table", tableName),
                        SafeArg.of("cell", cell),
                        SafeArg.of("expectedValue", value),
                        SafeArg.of("actualValue", actualValue));
            }
        });
    }

    private static void checkCellsAreDeleted(
            String tableName, Set<WorkloadCell> cells, ReadableTransactionStore store) {
        cells.forEach(deletedCell -> store.get(tableName, deletedCell)
                .ifPresent(value -> log.error(
                        "Cell existed when it is expected to be deleted.",
                        SafeArg.of("tableName", tableName),
                        SafeArg.of("cell", deletedCell),
                        SafeArg.of("value", value))));
    }

    @NotThreadSafe
    private static class InMemoryKvsTransactionReplayer implements WitnessedTransactionActionVisitor<Void> {

        private final Table<String, WorkloadCell, Integer> kvs = HashBasedTable.create();
        private final SetMultimap<String, WorkloadCell> deletedCells = HashMultimap.create();
        private boolean hasFinished = false;

        @Override
        public Void visit(WitnessedReadTransactionAction readTransactionAction) {
            checkMutable();
            return null;
        }

        @Override
        public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
            checkMutable();
            kvs.put(writeTransactionAction.table(), writeTransactionAction.cell(), writeTransactionAction.value());
            deletedCells.remove(writeTransactionAction.table(), writeTransactionAction.cell());
            return null;
        }

        @Override
        public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
            checkMutable();
            kvs.remove(deleteTransactionAction.table(), deleteTransactionAction.cell());
            deletedCells.put(deleteTransactionAction.table(), deleteTransactionAction.cell());
            return null;
        }

        private void checkMutable() {
            Preconditions.checkState(
                    !hasFinished, "Cannot replay transaction, as we've already calculated our view of the KVS.");
        }

        private Set<String> getTables() {
            hasFinished = true;
            return Collections.unmodifiableSet(kvs.rowKeySet());
        }

        private Map<WorkloadCell, Integer> getValues(String tableName) {
            hasFinished = true;
            return Collections.unmodifiableMap(kvs.row(tableName));
        }

        private Set<WorkloadCell> getDeletedCells(String tableName) {
            hasFinished = true;
            return Collections.unmodifiableSet(deletedCells.get(tableName));
        }
    }
}
