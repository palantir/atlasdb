/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.kafka;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.ValueType;

public final class CommandTypeSerialisers {
    private CommandTypeSerialisers() {
    }

    public static byte[] createTables(Map<TableReference, byte[]> tableRefToMetadata) {
        return putMetadataForTables(tableRefToMetadata);
    }

    public static byte[] putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        return tableRefToMetadata.entrySet().stream()
                .map(entry -> serialiseCreateTableEntry(entry.getKey(), entry.getValue()))
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));
    }

    public static byte[] truncateTables(Set<TableReference> tableRefs) {
        return tableRefs.stream()
                .map(CommandTypeSerialisers::serialiseTableRef)
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));
    }

    public static byte[] dropTables(Set<TableReference> tableRefs) {
        return tableRefs.stream()
                .map(CommandTypeSerialisers::serialiseTableRef)
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));
    }

    public static byte[] multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        byte[] encodedTimestamp = serialiseLong(timestamp);
        byte[] encodedCellValues = valuesByTable.entrySet().stream()
                .map(entry -> {
                    byte[] encodedTableRef = serialiseTableRef(entry.getKey());
                    byte[] encodedCellToArrayMap = serialiseCellToArrayMap(entry.getValue());
                    byte[] encodedCellToArrayMapSize = serialiseLong(encodedCellToArrayMap.length);

                    return combineArrays(encodedTableRef, encodedCellToArrayMapSize, encodedCellToArrayMap);
                })
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));

        return combineArrays(encodedTimestamp, encodedCellValues);
    }

    public static byte[] putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues) {
        byte[] encodedTableRef = serialiseTableRef(tableRef);
        byte[] encodedCellValues = cellValues.entries().stream()
                .map(entry -> combineArrays(serialiseCell(entry.getKey()), serialiseValue(entry.getValue())))
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));

        return combineArrays(encodedTableRef, encodedCellValues);
    }

    public static byte[] delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        byte[] encodedTableRef = serialiseTableRef(tableRef);
        byte[] encodedCellValues = keys.entries().stream()
                .map(entry -> combineArrays(serialiseCell(entry.getKey()), serialiseLong(entry.getValue())))
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));

        return combineArrays(encodedTableRef, encodedCellValues);
    }

    public static byte[] putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) {
        byte[] encodedTableRef = serialiseTableRef(tableRef);
        byte[] encodedCellValues = serialiseCellToArrayMap(values);

        return combineArrays(encodedTableRef, encodedCellValues);
    }

    private static byte[] serialiseCellToArrayMap(Map<Cell, byte[]> values) {
        return values.entrySet().stream()
                .map(entry -> combineArrays(serialiseCell(entry.getKey()), serialiseArray(entry.getValue())))
                .collect(Collectors.collectingAndThen(Collectors.toList(), CommandTypeSerialisers::combineArrays));
    }


    private static byte[] serialiseCreateTableEntry(TableReference tableRef, byte[] tableMetadata) {
        byte[] encodedTableRef = serialiseTableRef(tableRef);
        byte[] encodedTableMetadata = serialiseArray(tableMetadata);

        return combineArrays(encodedTableRef, encodedTableMetadata);
    }

    private static byte[] serialiseCell(Cell cell) {
        byte[] encodedRow = serialiseArray(cell.getRowName());
        byte[] encodedCol = serialiseArray(cell.getColumnName());
        byte[] encodedTtl = serialiseLong(cell.getTtlDurationMillis());

        return combineArrays(encodedRow, encodedCol, encodedTtl);
    }

    private static byte[] serialiseValue(Value value) {
        byte[] encodedTimestamp = serialiseLong(value.getTimestamp());
        byte[] encodedContents = serialiseArray(value.getContents());

        return combineArrays(encodedTimestamp, encodedContents);
    }

    private static byte[] serialiseLong(long longValue) {
        return ValueType.FIXED_LONG.convertFromJava(longValue);
    }

    private static byte[] serialiseTableRef(TableReference tableRef) {
        return ValueType.VAR_STRING.convertFromJava(tableRef.getQualifiedName());
    }

    private static byte[] serialiseArray(byte[] array) {
        return ValueType.SIZED_BLOB.convertFromJava(array);
    }

    public static byte[] combineArrays(byte[]... arrays) {
        return combineArrays(ImmutableList.copyOf(arrays));
    }

    public static byte[] combineArrays(List<byte[]> arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }

        byte[] combinedArrays = new byte[totalLength];
        int combinedOffset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, combinedArrays, combinedOffset, array.length);
            combinedOffset += array.length;
        }
        return combinedArrays;
    }
}
