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
import com.palantir.atlasdb.workload.store.ColumnValue;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import io.vavr.collection.Map;
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

    @Override
    public List<ColumnValue> readRange(RowColumnRangeReadTransactionAction readTransactionAction) {
        Map<TableAndWorkloadCell, Optional<Integer>> allValues = rawValueSupplier.get();
        if (allValues.size() > LARGE_HISTORY_LIMIT) {
            log.error(
                    "Attempted to do range queries in a simple way, even though the history is large ({} entries)! If"
                            + " you're seeing this message, consider simplifying your workflow and/or switching to a more"
                            + " efficient range query implementation.",
                    SafeArg.of("size", allValues.size()));
        }
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
                .mapValues(Optional::get)
                .map(entry -> ColumnValue.of(entry._1().cell().column(), entry._2()))
                .sortBy(ColumnValue::column)
                .toJavaList();
    }
}
