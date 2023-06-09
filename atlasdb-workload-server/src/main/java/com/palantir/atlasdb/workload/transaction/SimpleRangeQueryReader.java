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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.invariant.ValueAndMaybeTimestamp;
import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.store.RowResult;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import io.vavr.Tuple2;
import io.vavr.collection.Map;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public final class SimpleRangeQueryReader implements RangeQueryReader {
    private static final SafeLogger log = SafeLoggerFactory.get(SimpleRangeQueryReader.class);

    private static final int LARGE_HISTORY_LIMIT = 30_000;

    private final Supplier<Map<TableAndWorkloadCell, Optional<Integer>>> rawValueSupplier;

    @VisibleForTesting
    SimpleRangeQueryReader(Supplier<Map<TableAndWorkloadCell, Optional<Integer>>> rawValueSupplier) {
        this.rawValueSupplier = rawValueSupplier;
    }

    public static RangeQueryReader create(InMemoryTransactionReplayer replayer) {
        return new SimpleRangeQueryReader(replayer::getValues);
    }

    public static RangeQueryReader createForSnapshot(
            StructureHolder<Map<TableAndWorkloadCell, ValueAndMaybeTimestamp>> readView) {
        return new SimpleRangeQueryReader(() -> readView.getSnapshot().mapValues(ValueAndMaybeTimestamp::value));
    }

    @Override
    public List<ColumnAndValue> readColumnRange(RowColumnRangeReadTransactionAction readTransactionAction) {
        Map<TableAndWorkloadCell, Optional<Integer>> allValues = getValuesAndLogIfLarge();
        return allValues
                .filterKeys(tableAndWorkloadCell -> {
                    if (!tableAndWorkloadCell.tableName().equals(readTransactionAction.table())) {
                        return false;
                    }
                    if (tableAndWorkloadCell.cell().key() != readTransactionAction.row()) {
                        return false;
                    }
                    return readTransactionAction
                            .columnRangeSelection()
                            .contains(tableAndWorkloadCell.cell().column());
                })
                .filterValues(Optional::isPresent)
                .map(entry -> ColumnAndValue.of(
                        entry._1().cell().column(),
                        entry._2()
                                .orElseThrow(() -> new SafeRuntimeException(
                                        "Empty values should already have been filtered out!"))))
                .sortBy(ColumnAndValue::column)
                .toJavaList();
    }

    @Override
    public List<RowResult> readRowRange(RowRangeReadTransactionAction readTransactionAction) {
        Map<TableAndWorkloadCell, Optional<Integer>> allValues = getValuesAndLogIfLarge();
        return allValues
                .filterKeys(tableAndWorkloadCell -> {
                    if (!tableAndWorkloadCell.tableName().equals(readTransactionAction.table())) {
                        return false;
                    }
                    if (!readTransactionAction
                            .rowsToRead()
                            .contains(tableAndWorkloadCell.cell().key())) {
                        return false;
                    }
                    return readTransactionAction
                            .columns()
                            .contains(tableAndWorkloadCell.cell().column());
                })
                .filterValues(Optional::isPresent)
                .groupBy(tuple -> tuple._1().cell().key())
                .toSortedSet(Comparator.comparing(Tuple2::_1))
                // This is a hack to work around what I view as an unexpected Vavr API (sorted set's map necessarily
                // producing an ordered structure), when I only rely on the ordering above.
                .toList()
                .<RowResult>map(tuple -> RowResult.builder()
                        .row(tuple._1())
                        .addAllColumns(tuple._2()
                                .map(entry -> ColumnAndValue.of(
                                        entry._1().cell().column(),
                                        entry._2()
                                                .orElseThrow(() -> new SafeRuntimeException(
                                                        "Empty values should already have been filtered out!"))))
                                .sortBy(ColumnAndValue::column)
                                .toJavaList())
                        .build())
                .toJavaList();
    }

    private Map<TableAndWorkloadCell, Optional<Integer>> getValuesAndLogIfLarge() {
        Map<TableAndWorkloadCell, Optional<Integer>> allValues = rawValueSupplier.get();
        if (allValues.size() > LARGE_HISTORY_LIMIT) {
            log.error(
                    "Attempted to do range queries in a simple way, even though the history is large ({} entries)! If"
                        + " you're seeing this message, consider simplifying your workflow and/or switching to a more"
                        + " efficient range query implementation.",
                    SafeArg.of("size", allValues.size()));
        }
        return allValues;
    }
}
