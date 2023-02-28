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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableWorkloadCell;
import com.palantir.atlasdb.workload.transaction.InMemoryValidationStore;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;

public class DurableWritesInvariant implements Invariant {

    private static final SafeLogger log = SafeLoggerFactory.get(DurableWritesInvariant.class);

    private final Consumer<Map<TableWorkloadCell, MismatchedValue>> mismatchingCellsConsumer;
    private final Consumer<Map<TableWorkloadCell, Integer>> undeletedCellsConsumer;

    public DurableWritesInvariant() {
        this(
                mismatchingValues -> mismatchingValues.forEach((cell, mismatchedValue) -> log.error(
                        "In-memory state does not match external transactional store.",
                        SafeArg.of("table", cell.tableName()),
                        SafeArg.of("cell", cell.cell()),
                        SafeArg.of("mismatchedValue", mismatchedValue))),
                undeletedCells -> undeletedCells.forEach((cell, value) -> log.error(
                        "Cell existed when it is expected to be deleted.",
                        SafeArg.of("tableName", cell.tableName()),
                        SafeArg.of("cell", cell.cell()),
                        SafeArg.of("value", value))));
    }

    @VisibleForTesting
    DurableWritesInvariant(
            Consumer<Map<TableWorkloadCell, MismatchedValue>> mismatchingCellsConsumer,
            Consumer<Map<TableWorkloadCell, Integer>> undeletedCellsConsumer) {
        this.mismatchingCellsConsumer = mismatchingCellsConsumer;
        this.undeletedCellsConsumer = undeletedCellsConsumer;
    }

    @Override
    public void accept(WorkflowHistory workflowHistory) {
        InMemoryValidationStore expectedState = InMemoryValidationStore.create(workflowHistory.history());
        mismatchingCellsConsumer.accept(
                findCellsThatDoNotMatch(expectedState.values(), workflowHistory.transactionStore()));
        undeletedCellsConsumer.accept(
                findCellsThatAreNotDeleted(expectedState.deletedCells(), workflowHistory.transactionStore()));
    }

    @VisibleForTesting
    Map<TableWorkloadCell, MismatchedValue> findCellsThatDoNotMatch(
            Map<TableWorkloadCell, Integer> expectedCells, ReadableTransactionStore storeToValidate) {
        return EntryStream.of(expectedCells)
                .mapToValue((writtenCell, expectedValue) -> {
                    Optional<Integer> actualValue = storeToValidate.get(writtenCell.tableName(), writtenCell.cell());
                    if (!actualValue.map(expectedValue::equals).orElse(false)) {
                        return Optional.of(MismatchedValue.of(actualValue, Optional.of(expectedValue)));
                    }
                    return Optional.<MismatchedValue>empty();
                })
                .removeValues(Optional::isEmpty)
                .mapValues(Optional::get)
                .toMap();
    }

    @VisibleForTesting
    Map<TableWorkloadCell, Integer> findCellsThatAreNotDeleted(
            Set<TableWorkloadCell> cells, ReadableTransactionStore storeToValidate) {
        return StreamEx.of(cells)
                .mapToEntry(cell -> storeToValidate.get(cell.tableName(), cell.cell()))
                .removeValues(Optional::isEmpty)
                .mapValues(Optional::get)
                .toMap();
    }
}
