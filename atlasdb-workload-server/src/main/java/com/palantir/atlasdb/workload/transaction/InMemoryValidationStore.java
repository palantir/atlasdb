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

import com.palantir.atlasdb.workload.store.TableWorkloadCell;
import com.palantir.atlasdb.workload.store.ValidationStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

public final class InMemoryValidationStore implements ValidationStore {

    private final Map<TableWorkloadCell, Integer> values;

    private final Set<TableWorkloadCell> deletedCells;

    private InMemoryValidationStore(Map<TableWorkloadCell, Integer> values, Set<TableWorkloadCell> deletedCells) {
        this.deletedCells = Collections.unmodifiableSet(deletedCells);
        this.values = Collections.unmodifiableMap(values);
    }

    public static InMemoryValidationStore create(List<WitnessedTransaction> history) {
        InMemoryTransactionReplayer replayer = new InMemoryTransactionReplayer();
        history.stream()
                .map(WitnessedTransaction::actions)
                .forEach(witnessedTransactionActions ->
                        witnessedTransactionActions.forEach(action -> action.accept(replayer)));
        return new InMemoryValidationStore(replayer.getValues(), replayer.getDeletedCells());
    }

    @Override
    public Map<TableWorkloadCell, Integer> values() {
        return values;
    }

    @Override
    public Set<TableWorkloadCell> deletedCells() {
        return deletedCells;
    }

    @NotThreadSafe
    private static final class InMemoryTransactionReplayer implements WitnessedTransactionActionVisitor<Void> {

        private final Map<TableWorkloadCell, Integer> values = new HashMap<>();
        private final Set<TableWorkloadCell> deletedCells = new HashSet<>();

        @Override
        public Void visit(WitnessedReadTransactionAction readTransactionAction) {
            return null;
        }

        @Override
        public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
            TableWorkloadCell tableWorkloadCell =
                    TableWorkloadCell.of(writeTransactionAction.table(), writeTransactionAction.cell());
            values.put(tableWorkloadCell, writeTransactionAction.value());
            deletedCells.remove(tableWorkloadCell);
            return null;
        }

        @Override
        public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
            TableWorkloadCell tableWorkloadCell =
                    TableWorkloadCell.of(deleteTransactionAction.table(), deleteTransactionAction.cell());
            values.remove(tableWorkloadCell);
            deletedCells.add(tableWorkloadCell);
            return null;
        }

        public Map<TableWorkloadCell, Integer> getValues() {
            return values;
        }

        public Set<TableWorkloadCell> getDeletedCells() {
            return deletedCells;
        }
    }
}
