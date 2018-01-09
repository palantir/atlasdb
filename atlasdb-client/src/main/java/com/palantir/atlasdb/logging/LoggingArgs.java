/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.logging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

/**
 * Includes utilities for generating logging args that may be safe or unsafe, depending on table metadata.
 *
 * Always returns unsafe, until hydrated.
 */
public final class LoggingArgs {

    private static final String PLACEHOLDER_TABLE_NAME = "{table}";

    @VisibleForTesting
    public static final TableReference PLACEHOLDER_TABLE_REFERENCE =
            TableReference.createWithEmptyNamespace(PLACEHOLDER_TABLE_NAME);

    @Value.Immutable
    public interface SafeAndUnsafeTableReferences {
        SafeArg<List<TableReference>> safeTableRefs();
        UnsafeArg<List<TableReference>> unsafeTableRefs();
    }

    private static volatile KeyValueServiceLogArbitrator logArbitrator = KeyValueServiceLogArbitrator.ALL_UNSAFE;

    private LoggingArgs() {
        // no
    }

    public static synchronized void hydrate(Map<TableReference, byte[]> tableRefToMetadata) {
        logArbitrator = SafeLoggableDataUtils.fromTableMetadata(tableRefToMetadata);
    }

    @VisibleForTesting
    static synchronized void setLogArbitrator(KeyValueServiceLogArbitrator arbitrator) {
        logArbitrator = arbitrator;
    }

    public static SafeAndUnsafeTableReferences tableRefs(Collection<TableReference> tableReferences) {
        List<TableReference> safeTableRefs = new ArrayList<>();
        List<TableReference> unsafeTableRefs = new ArrayList<>();

        for (TableReference tableRef : tableReferences) {
            if (logArbitrator.isTableReferenceSafe(tableRef)) {
                safeTableRefs.add(tableRef);
            } else {
                unsafeTableRefs.add(tableRef);
            }
        }

        return ImmutableSafeAndUnsafeTableReferences.builder()
                .safeTableRefs(SafeArg.of("safeTableRefs", safeTableRefs))
                .unsafeTableRefs(UnsafeArg.of("unsafeTableRefs", unsafeTableRefs))
                .build();
    }

    public static Iterable<TableReference> safeTablesOrPlaceholder(Collection<TableReference> tables) {
        //noinspection StaticPseudoFunctionalStyleMethod - Use lazy iterator.
        return Iterables.transform(tables, LoggingArgs::safeTableOrPlaceholder);
    }

    /**
     * Returns a safe or unsafe arg corresponding to the supplied table reference, with name "tableRef".
     */
    public static Arg<String> tableRef(TableReference tableReference) {
        return tableRef("tableRef", tableReference);
    }

    /**
     * If table is safe, returns the table. If unsafe, returns a placeholder.
     */
    public static TableReference safeTableOrPlaceholder(TableReference tableReference) {
        if (logArbitrator.isTableReferenceSafe(tableReference)) {
            return tableReference;
        } else {
            return PLACEHOLDER_TABLE_REFERENCE;
        }
    }

    /**
     * If table is safe, returns the table. If unsafe, returns a placeholder name.
     */
    public static String safeInternalTableNameOrPlaceholder(String internalTableReference) {
        if (logArbitrator.isInternalTableReferenceSafe(internalTableReference)) {
            return internalTableReference;
        } else {
            return PLACEHOLDER_TABLE_NAME;
        }
    }

    public static Arg<String> tableRef(String argName, TableReference tableReference) {
        return getArg(argName, tableReference.toString(), logArbitrator.isTableReferenceSafe(tableReference));
    }

    public static Arg<String> customTableName(TableReference tableReference, String tableName) {
        return getArg("tableName", tableName, logArbitrator.isTableReferenceSafe(tableReference));
    }

    public static Arg<String> rowComponent(String argName, TableReference tableReference, String rowComponentName) {
        return getArg(argName,
                rowComponentName,
                logArbitrator.isRowComponentNameSafe(tableReference, rowComponentName));
    }

    public static Arg<String> columnName(String argName, TableReference tableReference, String columnName) {
        return getArg(argName,
                columnName,
                logArbitrator.isColumnNameSafe(tableReference, columnName));
    }

    public static Arg<Long> durationMillis(Stopwatch stopwatch) {
        return getArg("durationMillis", stopwatch.elapsed(TimeUnit.MILLISECONDS), true);
    }

    public static Arg<String> method(String method) {
        return getArg("method", method, true);
    }

    public static Arg<Integer> cellCount(int cellCount) {
        return getArg("cellCount", cellCount, true);
    }

    public static Arg<Integer> tableCount(int tableCount) {
        return getArg("tableCount", tableCount, true);
    }

    public static Arg<Integer> rowCount(int rowCount) {
        return getArg("rowCount", rowCount, true);
    }

    public static Arg<Long> sizeInBytes(long sizeInBytes) {
        return getArg("sizeInBytes", sizeInBytes, true);
    }

    public static Arg<?> columnCount(ColumnSelection columnSelection) {
        return getArg("columnCount", columnSelection.allColumnsSelected()
                ? "all" : Iterables.size(columnSelection.getSelectedColumns()), true);
    }

    public static Arg<Integer> batchHint(int batchHint) {
        return getArg("batchHint", batchHint, true);
    }

    public static Arg<RangeRequest> range(TableReference tableReference, RangeRequest range) {
        return getArg("range",
                range,
                range.getColumnNames().stream().allMatch(
                        (columnName) ->  logArbitrator.isColumnNameSafe(tableReference, PtBytes.toString(columnName))));
    }

    public static Arg<BatchColumnRangeSelection> batchColumnRangeSelection(TableReference tableReference,
            BatchColumnRangeSelection batchColumnRangeSelection) {
        String startCol = PtBytes.toString(batchColumnRangeSelection.getStartCol());
        String endCol = PtBytes.toString(batchColumnRangeSelection.getEndCol());
        return getArg("batchColumnRangeSelection", batchColumnRangeSelection,
                logArbitrator.isColumnNameSafe(tableReference, startCol)
                        && logArbitrator.isColumnNameSafe(tableReference, endCol));
    }

    public static Arg<ColumnRangeSelection> columnRangeSelection(TableReference tableReference,
            ColumnRangeSelection columnRangeSelection) {
        String startCol = PtBytes.toString(columnRangeSelection.getStartCol());
        String endCol = PtBytes.toString(columnRangeSelection.getEndCol());
        return getArg("columnRangeSelection", columnRangeSelection,
                logArbitrator.isColumnNameSafe(tableReference, startCol)
                        && logArbitrator.isColumnNameSafe(tableReference, endCol));
    }

    private static <T> Arg<T> getArg(String name, T value, boolean safe) {
        return safe ? SafeArg.of(name, value) : UnsafeArg.of(name, value);
    }

}
