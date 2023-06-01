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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedSingleCellTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowsColumnRangeExhaustionTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import io.vavr.collection.Map;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Replays transactions and validates for conflicts on the latest and read view.
 * <p>
 * The latest view will be a view of the database at the commit timestamp, while the read view will be the view of
 * the database at the start timestamp.
 * <p>
 * Objects created from this class should only be used within a scope of a single transaction.
 */
final class SnapshotInvariantVisitor
        implements WitnessedTransactionActionVisitor<List<InvalidWitnessedTransactionAction>> {

    private final Long startTimestamp;
    private final StructureHolder<Map<TableAndWorkloadCell, ValueAndMaybeTimestamp>> latestView;
    private final StructureHolder<Map<TableAndWorkloadCell, ValueAndMaybeTimestamp>> readView;

    SnapshotInvariantVisitor(
            Long startTimestamp,
            StructureHolder<Map<TableAndWorkloadCell, ValueAndMaybeTimestamp>> latestView,
            StructureHolder<Map<TableAndWorkloadCell, ValueAndMaybeTimestamp>> readView) {
        this.startTimestamp = startTimestamp;
        this.latestView = latestView;
        this.readView = readView;
    }

    @Override
    public List<InvalidWitnessedTransactionAction> visit(WitnessedReadTransactionAction readTransactionAction) {
        Optional<Integer> expected = fetchValueFromView(
                        readTransactionAction.table(), readTransactionAction.cell(), readView)
                .value();
        if (!expected.equals(readTransactionAction.value())) {
            return ImmutableList.of(InvalidWitnessedSingleCellTransactionAction.of(
                    readTransactionAction, MismatchedValue.of(readTransactionAction.value(), expected)));
        }
        return ImmutableList.of();
    }

    @Override
    public List<InvalidWitnessedTransactionAction> visit(WitnessedWriteTransactionAction writeTransactionAction) {
        Optional<InvalidWitnessedSingleCellTransactionAction> invalidAction = checkForWriteWriteConflicts(
                        writeTransactionAction.table(), writeTransactionAction.cell())
                .map(mismatchedValue ->
                        InvalidWitnessedSingleCellTransactionAction.of(writeTransactionAction, mismatchedValue));

        applyWrites(
                writeTransactionAction.table(),
                writeTransactionAction.cell(),
                Optional.of(writeTransactionAction.value()));
        return invalidAction.stream().collect(Collectors.toList());
    }

    @Override
    public List<InvalidWitnessedTransactionAction> visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
        Optional<InvalidWitnessedSingleCellTransactionAction> invalidAction = checkForWriteWriteConflicts(
                        deleteTransactionAction.table(), deleteTransactionAction.cell())
                .map(mismatchedValue ->
                        InvalidWitnessedSingleCellTransactionAction.of(deleteTransactionAction, mismatchedValue));

        applyWrites(deleteTransactionAction.table(), deleteTransactionAction.cell(), Optional.empty());
        return invalidAction.stream().collect(Collectors.toList());
    }

    @Override
    public List<InvalidWitnessedTransactionAction> visit(
            WitnessedRowsColumnRangeReadTransactionAction rowsColumnRangeReadTransactionAction) {
        // TODO(jkong): Not implemented yet
        return ImmutableList.of();
    }

    @Override
    public List<InvalidWitnessedTransactionAction> visit(
            WitnessedRowsColumnRangeExhaustionTransactionAction rowsColumnRangeExhaustionTransactionAction) {
        // TODO(jkong): Not implemented yet
        return ImmutableList.of();
    }

    /**
     * Applies writes to both our read and latest table view.
     * Latest table view needs to be updated to apply our writes from our transaction,
     * while the read view is updated to reflect local writes.
     */
    private void applyWrites(String tableName, WorkloadCell cell, Optional<Integer> value) {
        readView.with(table ->
                table.put(TableAndWorkloadCell.of(tableName, cell), ValueAndMaybeTimestamp.of(value, startTimestamp)));
        latestView.with(table ->
                table.put(TableAndWorkloadCell.of(tableName, cell), ValueAndMaybeTimestamp.of(value, startTimestamp)));
    }

    /**
     * Checks that the value we are writing has not changed from our read view and the latest view. If it has,
     * that means we have missed a write-write conflict, as it should have conflicted with this transaction.
     */
    private Optional<MismatchedValue> checkForWriteWriteConflicts(String table, WorkloadCell cell) {
        ValueAndMaybeTimestamp previous = fetchValueFromView(table, cell, readView);
        ValueAndMaybeTimestamp latest = fetchValueFromView(table, cell, latestView);

        if (!previous.equals(latest)) {
            return Optional.of(MismatchedValue.of(latest, previous));
        }

        return Optional.empty();
    }

    private static ValueAndMaybeTimestamp fetchValueFromView(
            String tableName,
            WorkloadCell workloadCell,
            StructureHolder<Map<TableAndWorkloadCell, ValueAndMaybeTimestamp>> view) {
        TableAndWorkloadCell tableAndWorkloadCell = TableAndWorkloadCell.of(tableName, workloadCell);
        return view.getSnapshot().get(tableAndWorkloadCell).toJavaOptional().orElseGet(ValueAndMaybeTimestamp::empty);
    }
}
