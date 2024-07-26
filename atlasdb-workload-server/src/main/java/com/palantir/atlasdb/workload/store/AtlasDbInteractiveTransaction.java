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
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import one.util.streamex.EntryStream;

@NotThreadSafe
final class AtlasDbInteractiveTransaction implements InteractiveTransaction {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasDbInteractiveTransaction.class);

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
                    log.info(
                            "Starting read for transaction {} for workflow cell {} on table {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("cell", atlasCell),
                            SafeArg.of("table", tableReference));

                    Map<Cell, byte[]> values = transaction.get(tableReference, Set.of(atlasCell));
                    Optional<Integer> valueRead =
                            Optional.ofNullable(values.get(atlasCell)).map(Ints::fromByteArray);
                    witnessedTransactionActions.add(
                            WitnessedSingleCellReadTransactionAction.of(table, workloadCell, valueRead));

                    log.info(
                            "Have completed read for transaction {} for workflow cell {} on table {} with value {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("cell", atlasCell),
                            SafeArg.of("table", tableReference),
                            SafeArg.of("value", valueRead));

                    return valueRead;
                },
                table,
                workloadCell);
    }

    @Override
    public void write(String table, WorkloadCell workloadCell, Integer value) {
        run(
                (tableReference, atlasCell) -> {
                    log.info(
                            "Starting write for transaction {} for workflow cell {} on table {} with value {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("cell", atlasCell),
                            SafeArg.of("table", tableReference),
                            SafeArg.of("value", value));
                    transaction.put(tableReference, Map.of(atlasCell, Ints.toByteArray(value)));
                    witnessedTransactionActions.add(WitnessedWriteTransactionAction.of(table, workloadCell, value));
                    log.info(
                            "Have completed write for transaction {} for workflow cell {} for table {} with value {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("cell", atlasCell),
                            SafeArg.of("table", tableReference),
                            SafeArg.of("value", value));
                    return null;
                },
                table,
                workloadCell);
    }

    @Override
    public void delete(String table, WorkloadCell workloadCell) {
        run(
                (tableReference, atlasCell) -> {
                    log.info(
                            "Starting delete for transaction {} for workflow cell {} on table {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("cell", atlasCell),
                            SafeArg.of("table", tableReference));
                    transaction.delete(tableReference, Set.of(atlasCell));
                    witnessedTransactionActions.add(WitnessedDeleteTransactionAction.of(table, workloadCell));
                    log.info(
                            "Have completed delete for transaction {} for workflow cell {} for table {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("cell", atlasCell),
                            SafeArg.of("table", tableReference));
                    return null;
                },
                table,
                workloadCell);
    }

    @Override
    public List<ColumnAndValue> getRowColumnRange(String table, int row, ColumnRangeSelection columnRangeSelection) {
        return run(
                tableReference -> {
                    // Having a non-configurable batch hint is a bit iffy, but suffices as this won't be used in
                    // production.
                    log.info(
                            "Starting read row column range for transaction {} for row {} for column range "
                                    + "selection {} on table {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("row", row),
                            SafeArg.of("columnRangeSelection", columnRangeSelection),
                            SafeArg.of("table", tableReference));

                    Map<byte[], Iterator<Entry<Cell, byte[]>>> iterators = transaction.getRowsColumnRangeIterator(
                            tableReference,
                            List.of(AtlasDbUtils.toAtlasKey(row)),
                            BatchColumnRangeSelection.create(
                                    AtlasDbUtils.toAtlasColumnRangeSelection(columnRangeSelection), 100));
                    Preconditions.checkState(
                            iterators.size() == 1,
                            "Expected exactly one iterator to be returned",
                            SafeArg.of("iteratorsReturned", iterators.size()));
                    List<ColumnAndValue> columnsAndValues = EntryStream.of(iterators.get(AtlasDbUtils.toAtlasKey(row)))
                            .mapKeys(Cell::getColumnName)
                            .mapKeys(AtlasDbUtils::fromAtlasColumn)
                            .mapValues(AtlasDbUtils::fromAtlasValue)
                            .map(entry -> ColumnAndValue.of(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());
                    witnessedTransactionActions.add(RowColumnRangeReadTransactionAction.builder()
                            .table(table)
                            .row(row)
                            .columnRangeSelection(columnRangeSelection)
                            .build()
                            .witness(columnsAndValues));

                    log.info(
                            "Have completed read row column range for transaction {} for row {} for column "
                                    + "range selection {} on table {} with returned columns and values {}",
                            SafeArg.of("startTimestamp", transaction.getTimestamp()),
                            SafeArg.of("row", row),
                            SafeArg.of("cell", columnRangeSelection),
                            SafeArg.of("table", tableReference),
                            SafeArg.of("columnsAndValues", columnsAndValues));
                    return columnsAndValues;
                },
                table);
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
        checkTransactionHasNotFinished();
        Cell atlasCell = AtlasDbUtils.toAtlasCell(workloadCell);
        TableReference tableReference = getTableReferenceOrThrow(table);
        return function.apply(tableReference, atlasCell);
    }

    private <T> T run(Function<TableReference, T> function, String table) {
        checkTransactionHasNotFinished();
        TableReference tableReference = getTableReferenceOrThrow(table);
        return function.apply(tableReference);
    }

    private void checkTransactionHasNotFinished() {
        Preconditions.checkState(
                !hasFinished, "Transaction has already been witnessed and can no longer perform any actions.");
    }
}
