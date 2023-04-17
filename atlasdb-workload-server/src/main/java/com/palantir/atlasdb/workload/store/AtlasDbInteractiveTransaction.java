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

package com.palantir.atlasdb.workload.store;

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class AtlasDbInteractiveTransaction implements InteractiveTransaction {

    private final Transaction transaction;

    private final Map<String, TableReference> tables;

    private final List<WitnessedTransactionAction> witnessedTransactionActions = new ArrayList<>();

    private boolean hasFinished = false;

    public AtlasDbInteractiveTransaction(Transaction transaction, Map<String, TableReference> tables) {
        this.transaction = transaction;
        this.tables = tables;
    }

    @Override
    public Optional<Integer> read(String table, WorkloadCell workloadCell) {
        return run(
                (tableReference, atlasCell) -> {
                    Map<Cell, byte[]> values = transaction.get(tableReference, Set.of(atlasCell));
                    Optional<Integer> valueRead =
                            Optional.ofNullable(values.get(atlasCell)).map(Ints::fromByteArray);
                    witnessedTransactionActions.add(WitnessedReadTransactionAction.of(table, workloadCell, valueRead));
                    return valueRead;
                },
                table,
                workloadCell);
    }

    @Override
    public void write(String table, WorkloadCell workloadCell, Integer value) {
        run(
                (tableReference, atlasCell) -> {
                    transaction.put(tableReference, Map.of(atlasCell, Ints.toByteArray(value)));
                    witnessedTransactionActions.add(WitnessedWriteTransactionAction.of(table, workloadCell, value));
                    return null;
                },
                table,
                workloadCell);
    }

    @Override
    public void delete(String table, WorkloadCell workloadCell) {
        run(
                (tableReference, atlasCell) -> {
                    transaction.delete(tableReference, Set.of(atlasCell));
                    witnessedTransactionActions.add(WitnessedDeleteTransactionAction.of(table, workloadCell));
                    return null;
                },
                table,
                workloadCell);
    }

    @Override
    public List<WitnessedTransactionAction> witness() {
        hasFinished = true;
        return witnessedTransactionActions;
    }

    private TableReference getTableReferenceOrThrow(String table) {
        return Optional.ofNullable(tables.get(table))
                .orElseThrow(() -> new SafeIllegalArgumentException(
                        "Transaction action has unknown table.",
                        SafeArg.of("tableName", table),
                        SafeArg.of("availableTables", tables)));
    }

    private <T> T run(BiFunction<TableReference, Cell, T> function, String table, WorkloadCell workloadCell) {
        Preconditions.checkState(
                !hasFinished, "Transaction has already been witnessed and can no longer perform any actions.");
        Cell atlasCell = AtlasDbUtils.toAtlasCell(workloadCell);
        TableReference tableReference = getTableReferenceOrThrow(table);
        return function.apply(tableReference, atlasCell);
    }
}
