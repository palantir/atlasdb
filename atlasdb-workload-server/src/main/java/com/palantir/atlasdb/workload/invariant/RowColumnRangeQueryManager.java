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
import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeIteratorCreationTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashMultimap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.Multimap;
import io.vavr.collection.Set;
import io.vavr.collection.Traversable;
import io.vavr.control.Option;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Value;

/**
 * The lifecycle of a row column range query looks as follows:
 * <ol>
 *     <li>The range query is <b>created</b>.</li>
 *     <li>One or more cells are <b>read</b> from the range query</li>
 *     <li>The iterator returned has been <b>exhausted</b>; all cells in the range have been read, OR</li>
 *     <li>The iterator is <b>invalidated</b> because of a concurrent modification; we do not make guarantees in
 *     this case.</li>
 * </ol>
 * While the AtlasDB and Workload Server APIs support getRowsColumnRange with multiple rows, we consider the validation
 * of each of these rows separately.
 * <p>
 * This implementation does NOT scale in the number of active range queries (one could improve asymptotic performance
 * by using an interval or segment tree for the row-level backing storage). It is intended to be used for workflow
 * verification only.
 */
@NotThreadSafe
public final class RowColumnRangeQueryManager {
    private final StructureHolder<Map<String, SingleTableLiveQueries>> queriesByTable;

    @VisibleForTesting
    RowColumnRangeQueryManager(StructureHolder<Map<String, SingleTableLiveQueries>> queriesByTable) {
        this.queriesByTable = queriesByTable;
    }

    public static RowColumnRangeQueryManager create() {
        return new RowColumnRangeQueryManager(StructureHolder.create(HashMap::empty));
    }

    public void invalidateOverlappingQueries(WitnessedWriteTransactionAction witnessedWriteTransactionAction) {
        invalidateOverlappingQueries(witnessedWriteTransactionAction.table(), witnessedWriteTransactionAction.cell());
    }

    public void invalidateOverlappingQueries(WitnessedDeleteTransactionAction witnessedDeleteTransactionAction) {
        invalidateOverlappingQueries(witnessedDeleteTransactionAction.table(), witnessedDeleteTransactionAction.cell());
    }

    private void invalidateOverlappingQueries(String table, WorkloadCell cell) {
        Map<String, SingleTableLiveQueries> snapshot = queriesByTable.getSnapshot();
        if (!snapshot.containsKey(table)) {
            return;
        }
        SingleTableLiveQueries liveQueries = snapshot.get(table)
                .getOrElseThrow(() -> new SafeIllegalStateException(
                        "The table not being present should be handled above", SafeArg.of("table", table)));
        liveQueries.invalidateOverlappingQueries(cell);
    }

    public void trackQueryCreation(WitnessedRowsColumnRangeIteratorCreationTransactionAction witness) {
        queriesByTable.with(map -> {
            Tuple2<SingleTableLiveQueries, ? extends Map<String, SingleTableLiveQueries>> queriesAndPossiblyUpdatedMap =
                    map.computeIfAbsent(
                            witness.originalAction().table(),
                            unused -> new SingleTableLiveQueries(StructureHolder.create(
                                    () -> HashMultimap.withSeq().empty())));
            queriesAndPossiblyUpdatedMap._1().trackQueryCreation(witness);
            return queriesAndPossiblyUpdatedMap._2();
        });
    }

    /**
     * Returns the live query state corresponding to a given read transaction action, or optional if we no longer
     * are holding any validation for that read transaction action to be reasonable.
     */
    public Optional<RowColumnRangeQueryState> getLiveQueryState(
            WitnessedRowsColumnRangeReadTransactionAction readTransactionAction) {
        return queriesByTable
                .getSnapshot()
                .get(readTransactionAction.table())
                .flatMap(liveQueries -> liveQueries.getLiveQueryState(readTransactionAction))
                .toJavaOptional();
    }

    public void trackQueryRead(WitnessedRowsColumnRangeReadTransactionAction rowsColumnRangeReadTransactionAction) {
        queriesByTable.with(map -> {
            SingleTableLiveQueries liveQueries = map.get(rowsColumnRangeReadTransactionAction.table())
                    .getOrElseThrow(() -> new SafeIllegalStateException(
                            "Should not attempt to read from nonexistent iterator",
                            SafeArg.of("table", rowsColumnRangeReadTransactionAction.table())));
            liveQueries.trackQueryRead(rowsColumnRangeReadTransactionAction);
            return map;
        });
    }

    @VisibleForTesting
    static final class SingleTableLiveQueries {
        private final StructureHolder<Multimap<Integer, RowColumnRangeQueryState>> queriesByRow;

        private SingleTableLiveQueries(StructureHolder<Multimap<Integer, RowColumnRangeQueryState>> queriesByRow) {
            this.queriesByRow = queriesByRow;
        }

        public void invalidateOverlappingQueries(WorkloadCell cell) {
            queriesByRow.with(map -> {
                Option<Traversable<RowColumnRangeQueryState>> maybeRowColumnRangeQueryStates = map.get(cell.key());
                if (maybeRowColumnRangeQueryStates.isDefined()) {
                    Set<RowColumnRangeQueryState> invalidatedQueries =
                            HashSet.ofAll(maybeRowColumnRangeQueryStates.get().filter(queryState -> queryState
                                    .workloadColumnRangeSelection()
                                    .contains(cell.column())));
                    return map.filter((row, queryState) ->
                            !Objects.equals(row, cell.key()) || !invalidatedQueries.contains(queryState));
                } else {
                    // Do nothing: there are no live queries in this row.
                    return map;
                }
            });
        }

        public void trackQueryCreation(WitnessedRowsColumnRangeIteratorCreationTransactionAction witness) {
            queriesByRow.with(map -> map.put(
                    witness.specificRow(),
                    ImmutableRowColumnRangeQueryState.builder()
                            .iteratorId(witness.iteratorIdentifier())
                            .workloadColumnRangeSelection(
                                    witness.originalAction().batchColumnRangeSelection())
                            .build()));
        }

        public Option<RowColumnRangeQueryState> getLiveQueryState(
                WitnessedRowsColumnRangeReadTransactionAction readTransactionAction) {
            return queriesByRow
                    .getSnapshot()
                    .get(readTransactionAction.specificRow())
                    .flatMap(queryStates -> queryStates.find(queryState ->
                            Objects.equals(queryState.iteratorId(), readTransactionAction.iteratorIdentifier())));
        }

        public void trackQueryRead(WitnessedRowsColumnRangeReadTransactionAction witness) {
            queriesByRow.with(map -> {
                if (!map.containsKey(witness.specificRow())) {
                    // Is fine. All the queries for this row have been invalidated.
                    return map;
                }
                Traversable<RowColumnRangeQueryState> matchingQueryState = map.get(witness.specificRow())
                        .get()
                        .filter(state -> state.iteratorId().equals(witness.iteratorIdentifier()));
                RowColumnRangeQueryState singleMatchingQuery = matchingQueryState.single();
                return map.replace(
                        witness.specificRow(),
                        singleMatchingQuery,
                        singleMatchingQuery.copyWithLastReadCell(witness.cell()));
            });
        }
    }

    @Value.Immutable
    interface RowColumnRangeQueryState {
        UUID iteratorId();

        WorkloadColumnRangeSelection workloadColumnRangeSelection();

        // State used to check reads

        Optional<WorkloadCell> lastReadCell();

        default RowColumnRangeQueryState copyWithLastReadCell(Optional<WorkloadCell> cell) {
            return ImmutableRowColumnRangeQueryState.builder()
                    .from(this)
                    .lastReadCell(cell)
                    .build();
        }
    }
}
