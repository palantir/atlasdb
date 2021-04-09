/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.logging;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.immutables.value.Value;

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
    private static Optional<Boolean> allSafeForLogging = Optional.empty();

    // MUTABLE. The producer is final, but still, MUTABLE.
    private static final TableForkingSensitiveLoggingArgProducer tableForkingProducer =
            new TableForkingSensitiveLoggingArgProducer(DefaultSensitiveLoggingArgProducers.ALWAYS_UNSAFE);

    private LoggingArgs() {
        // no
    }

    public static synchronized void hydrate(Map<TableReference, byte[]> tableRefToMetadata) {
        logArbitrator = SafeLoggableDataUtils.fromTableMetadata(tableRefToMetadata);
    }

    public static synchronized void combineAndSetNewAllSafeForLoggingFlag(
            boolean isNewKeyValueServiceAllSafeForLogging) {
        if (!allSafeForLogging.isPresent()) {
            // if allSafeForLogging is never set, set it to the new keyValueService's allSafeForLogging setting
            allSafeForLogging = Optional.of(isNewKeyValueServiceAllSafeForLogging);
        } else {
            // if allSafeForLogging is currently safe, but the newKeyValueService is not allSafeForLogging by default
            // set the global allSafeForLogging flag to false
            if (!isNewKeyValueServiceAllSafeForLogging) {
                allSafeForLogging = Optional.of(isNewKeyValueServiceAllSafeForLogging);
            }
        }

        if (allSafeForLogging.get()) {
            logArbitrator = KeyValueServiceLogArbitrator.ALL_SAFE;
        }
    }

    public static void registerSensitiveLoggingArgProducerForTable(
            TableReference tableRef, SensitiveLoggingArgProducer sensitiveLoggingArgProducer) {
        tableForkingProducer.register(tableRef, sensitiveLoggingArgProducer);
    }

    @VisibleForTesting
    static synchronized void setLogArbitrator(KeyValueServiceLogArbitrator arbitrator) {
        logArbitrator = arbitrator;
    }

    public static Arg<String> internalTableName(TableReference tableReference) {
        return safeInternalTableName(AbstractKeyValueService.internalTableName(tableReference));
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
        return Collections2.transform(tables, LoggingArgs::safeTableOrPlaceholder);
    }

    /**
     * Returns a safe or unsafe arg corresponding to the supplied table reference, with name "tableRef".
     */
    public static Arg<String> tableRef(TableReference tableReference) {
        return tableRef("tableRef", tableReference);
    }

    public static boolean isSafe(TableReference tableReference) {
        return logArbitrator.isTableReferenceSafe(tableReference);
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

    public static Arg<String> safeInternalTableName(String internalTableReference) {
        if (logArbitrator.isInternalTableReferenceSafe(internalTableReference)) {
            return SafeArg.of("tableRef", internalTableReference);
        } else {
            return UnsafeArg.of("unsafeTableRef", internalTableReference);
        }
    }

    public static Arg<String> tableRef(String argName, TableReference tableReference) {
        return getArg(argName, tableReference.toString(), logArbitrator.isTableReferenceSafe(tableReference));
    }

    public static Arg<String> customTableName(TableReference tableReference, String tableName) {
        return getArg("tableName", tableName, logArbitrator.isTableReferenceSafe(tableReference));
    }

    public static Arg<Long> durationMillis(Stopwatch stopwatch) {
        return getArg("durationMillis", stopwatch.elapsed(TimeUnit.MILLISECONDS), true);
    }

    public static Arg<Long> startTimeMillis(long startTime) {
        return getArg("startTimeMillis", startTime, true);
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

    public static Arg<Integer> keyCount(int keyCount) {
        return getArg("keyCount", keyCount, true);
    }

    public static Arg<Integer> rowCount(int rowCount) {
        return getArg("rowCount", rowCount, true);
    }

    public static Arg<Long> sizeInBytes(long sizeInBytes) {
        return getArg("sizeInBytes", sizeInBytes, true);
    }

    public static Arg<?> columnCount(ColumnSelection columnSelection) {
        return getArg(
                "columnCount",
                columnSelection.allColumnsSelected() ? "all" : Iterables.size(columnSelection.getSelectedColumns()),
                true);
    }

    public static Arg<?> columnCount(int numberOfColumns) {
        return getArg("columnCount", numberOfColumns == Integer.MAX_VALUE ? "all" : numberOfColumns, true);
    }

    public static Arg<Integer> batchHint(int batchHint) {
        return getArg("batchHint", batchHint, true);
    }

    public static Arg<RangeRequest> range(TableReference tableReference, RangeRequest range) {
        return getArg(
                "range",
                range,
                range.getColumnNames().stream()
                        .allMatch(columnName ->
                                logArbitrator.isColumnNameSafe(tableReference, PtBytes.toString(columnName))));
    }

    public static Arg<BatchColumnRangeSelection> batchColumnRangeSelection(
            BatchColumnRangeSelection batchColumnRangeSelection) {
        return getArg("batchColumnRangeSelection", batchColumnRangeSelection, false);
    }

    public static Arg<ColumnRangeSelection> columnRangeSelection(ColumnRangeSelection columnRangeSelection) {
        return getArg("columnRangeSelection", columnRangeSelection, false);
    }

    public static Arg<?> row(TableReference tableReference, byte[] row, Function<byte[], Object> transform) {
        return tableForkingProducer
                .getArgForRow(tableReference, row, transform)
                .orElseThrow(
                        () -> new SafeRuntimeException("if the forking producer returns optional empty it's forked"));
    }

    public static Arg<?> column(TableReference tableReference, byte[] column, Function<byte[], Object> transform) {
        return tableForkingProducer
                .getArgForColumn(tableReference, column, transform)
                .orElseThrow(
                        () -> new SafeRuntimeException("if the forking producer returns optional empty it's forked"));
    }

    public static Arg<?> value(
            TableReference tableReference, Cell cell, byte[] value, Function<byte[], Object> transform) {
        return tableForkingProducer
                .getArgForValue(tableReference, cell, value, transform)
                .orElseThrow(
                        () -> new SafeRuntimeException("if the forking producer returns optional empty it's forked"));
    }

    public static Arg<?> namedValue(
            TableReference tableReference, Cell cell, byte[] value, Function<byte[], Object> transform, String name) {
        return tableForkingProducer
                .getNamedArgForValue(tableReference, cell, value, transform, name)
                .orElseThrow(
                        () -> new SafeRuntimeException("if the forking producer returns optional empty it's forked"));
    }

    private static <T> Arg<T> getArg(String name, T value, boolean safe) {
        return safe ? SafeArg.of(name, value) : UnsafeArg.of(name, value);
    }
}
