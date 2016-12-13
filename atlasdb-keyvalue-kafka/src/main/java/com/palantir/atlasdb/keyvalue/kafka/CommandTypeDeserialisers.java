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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.kafka.data.CreateTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.DeleteData;
import com.palantir.atlasdb.keyvalue.kafka.data.DropTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutableCreateTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutableDeleteData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutableDropTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutableMultiPutData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutablePutMetadataForTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutablePutUnlessExistsData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutablePutWithTimestampsData;
import com.palantir.atlasdb.keyvalue.kafka.data.ImmutableTruncateTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.MultiPutData;
import com.palantir.atlasdb.keyvalue.kafka.data.PutMetadataForTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.PutUnlessExistsData;
import com.palantir.atlasdb.keyvalue.kafka.data.PutWithTimestampsData;
import com.palantir.atlasdb.keyvalue.kafka.data.TruncateTablesData;
import com.palantir.atlasdb.table.description.ValueType;

public final class CommandTypeDeserialisers {
    private CommandTypeDeserialisers() {
    }

    public static CreateTablesData createTables(byte[] data) {
        int offset = 0;
        ImmutableMap.Builder<TableReference, byte[]> tableRefToTableMetadata = ImmutableMap.builder();
        while (offset < data.length) {
            TableReference decodedTableReference = deserialiseTableReference(data, offset);
            offset += sizeOfTableReference(decodedTableReference);
            byte[] decodedMetadata = deserialiseArray(data, offset);
            offset += sizeOfArray(decodedMetadata);

            tableRefToTableMetadata.put(decodedTableReference, decodedMetadata);
        }

        Preconditions.checkArgument(data.length == offset,
                "Data size didn't match size needed. %s > %s", data.length, offset);

        return ImmutableCreateTablesData.builder()
                .tableRefToTableMetadata(tableRefToTableMetadata.build())
                .build();
    }

    public static PutMetadataForTablesData putMetadataForTables(byte[] data) {
        int offset = 0;
        ImmutableMap.Builder<TableReference, byte[]> tableRefToTableMetadata = ImmutableMap.builder();
        while (offset < data.length) {
            TableReference decodedTableReference = deserialiseTableReference(data, offset);
            offset += sizeOfTableReference(decodedTableReference);
            byte[] decodedMetadata = deserialiseArray(data, offset);
            offset += sizeOfArray(decodedMetadata);

            tableRefToTableMetadata.put(decodedTableReference, decodedMetadata);
        }

        Preconditions.checkArgument(data.length == offset,
                "Data size didn't match size needed. %s > %s", data.length, offset);

        return ImmutablePutMetadataForTablesData.builder()
                .tableRefToTableMetadata(tableRefToTableMetadata.build())
                .build();
    }

    public static TruncateTablesData truncateTables(byte[] data) {
        int offset = 0;
        ImmutableSet.Builder<TableReference> tableRefs = ImmutableSet.builder();
        while (offset < data.length) {
            TableReference decodedTableReference = deserialiseTableReference(data, offset);
            offset += sizeOfTableReference(decodedTableReference);
            tableRefs.add(decodedTableReference);
        }

        return ImmutableTruncateTablesData.builder()
                .tableRefs(tableRefs.build())
                .build();
    }

    public static DropTablesData dropTables(byte[] data) {
        int offset = 0;
        ImmutableSet.Builder<TableReference> tableRefs = ImmutableSet.builder();
        while (offset < data.length) {
            TableReference decodedTableReference = deserialiseTableReference(data, offset);
            offset += sizeOfTableReference(decodedTableReference);
            tableRefs.add(decodedTableReference);
        }

        return ImmutableDropTablesData.builder()
                .tableRefs(tableRefs.build())
                .build();
    }

    public static PutWithTimestampsData putWithTimestamps(byte[] data) {
        int offset = 0;
        TableReference decodedTableReference = deserialiseTableReference(data, offset);
        offset += sizeOfTableReference(decodedTableReference);
        ImmutableListMultimap.Builder<Cell, Value> cellValues = ImmutableListMultimap.builder();
        while (offset < data.length) {
            Cell cell = deserialiseCell(data, offset);
            offset += sizeOfCell(cell);
            Value value = deserialiseValue(data, offset);
            offset += sizeOfValue(value);
            cellValues.put(cell, value);
        }

        return ImmutablePutWithTimestampsData.builder()
                .tableReference(decodedTableReference)
                .cellValues(cellValues.build())
                .build();
    }

    public static DeleteData delete(byte[] data) {
        int offset = 0;
        TableReference decodedTableReference = deserialiseTableReference(data, offset);
        offset += sizeOfTableReference(decodedTableReference);
        ImmutableListMultimap.Builder<Cell, Long> keys = ImmutableListMultimap.builder();
        while (offset < data.length) {
            Cell cell = deserialiseCell(data, offset);
            offset += sizeOfCell(cell);
            Long value = deserialiseLong(data, offset);
            offset += sizeOfLong(value);
            keys.put(cell, value);
        }

        return ImmutableDeleteData.builder()
                .tableReference(decodedTableReference)
                .keys(keys.build())
                .build();
    }

    public static MultiPutData multiPut(byte[] data) {
        int offset = 0;
        long decodedTimestamp = deserialiseLong(data, offset);
        offset += sizeOfLong(decodedTimestamp);

        ImmutableMap.Builder<TableReference, Map<Cell, byte[]>> valuesByTable = ImmutableMap.builder();
        while (offset < data.length) {
            TableReference decodedTableReference = deserialiseTableReference(data, offset);
            offset += sizeOfTableReference(decodedTableReference);

            long decodedMapSize = deserialiseLong(data, offset);
            offset += sizeOfLong(decodedMapSize);

            int offset2 = 0;
            ImmutableMap.Builder<Cell, byte[]> values = ImmutableMap.builder();
            while (offset2 < decodedMapSize) {
                Cell cell = deserialiseCell(data, offset + offset2);
                offset2 += sizeOfCell(cell);
                byte[] value = deserialiseArray(data, offset + offset2);
                offset2 += sizeOfArray(value);
                values.put(cell, value);
            }
            offset += offset2;

            valuesByTable.put(decodedTableReference, values.build());
        }

        return ImmutableMultiPutData.builder()
                .valuesByTable(valuesByTable.build())
                .timestamp(decodedTimestamp)
                .build();
    }

    public static PutUnlessExistsData putUnlessExists(byte[] data) {
        int offset = 0;
        TableReference decodedTableReference = deserialiseTableReference(data, offset);
        offset += sizeOfTableReference(decodedTableReference);
        ImmutableMap.Builder<Cell, byte[]> values = ImmutableMap.builder();
        while (offset < data.length) {
            Cell cell = deserialiseCell(data, offset);
            offset += sizeOfCell(cell);
            byte[] value = deserialiseArray(data, offset);
            offset += sizeOfArray(value);
            values.put(cell, value);
        }

        return ImmutablePutUnlessExistsData.builder()
                .tableReference(decodedTableReference)
                .values(values.build())
                .build();
    }

    private static TableReference deserialiseTableReference(byte[] data, int offset) {
        return TableReference.fromString((String) ValueType.VAR_STRING.convertToJava(data, offset));
    }

    private static Cell deserialiseCell(byte[] data, int inputOffset) {
        int offset = inputOffset;
        byte[] decodedRow = deserialiseArray(data, offset);
        offset += sizeOfArray(decodedRow);
        byte[] decodedCol = deserialiseArray(data, offset);
        offset += sizeOfArray(decodedCol);
        long decodedTtl = deserialiseLong(data, offset);

        return Cell.create(decodedRow, decodedCol, decodedTtl, TimeUnit.MILLISECONDS);
    }

    private static Value deserialiseValue(byte[] data, int inputOffset) {
        int offset = inputOffset;
        long decodedTimestamp = deserialiseLong(data, offset);
        offset += sizeOfLong(decodedTimestamp);
        byte[] decodedContents = deserialiseArray(data, offset);

        return Value.create(decodedContents, decodedTimestamp);
    }

    private static byte[] deserialiseArray(byte[] data, int offset) {
        return (byte[]) ValueType.SIZED_BLOB.convertToJava(data, offset);
    }

    private static long deserialiseLong(byte[] data, int offset) {
        return (long) ValueType.FIXED_LONG.convertToJava(data, offset);
    }

    private static int sizeOfTableReference(TableReference decodedTableReference) {
        return ValueType.VAR_STRING.sizeOf(decodedTableReference.getQualifiedName());
    }

    private static int sizeOfCell(Cell cell) {
        return sizeOfArray(cell.getRowName())
                + sizeOfArray(cell.getColumnName())
                + sizeOfLong(cell.getTtlDurationMillis());
    }

    private static int sizeOfValue(Value value) {
        return sizeOfLong(value.getTimestamp())
                + sizeOfArray(value.getContents());
    }

    private static int sizeOfArray(byte[] rowName) {
        return ValueType.SIZED_BLOB.sizeOf(rowName);
    }

    private static int sizeOfLong(long decodedTimestamp) {
        return ValueType.FIXED_LONG.sizeOf(decodedTimestamp);
    }
}
