/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.util.Pair;

public final class CassandraTimestampUtils {
    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription("timestamp_name", ValueType.STRING))),
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            "current_max_ts",
                            ColumnValueDescription.forType(ValueType.FIXED_LONG)))),
            ConflictHandler.IGNORE_ALL);

    public static final String ROW_AND_COLUMN_NAME = "ts";
    public static final String BACKUP_COLUMN_NAME = "oldTs";
    public static final ByteString INVALIDATED_VALUE = ByteString.copyFrom(new byte[1]);

    private static final long CASSANDRA_TIMESTAMP = -1;

    private static final String ROW_AND_COLUMN_NAME_HEX_STRING = encodeCassandraHexString(ROW_AND_COLUMN_NAME);

    @VisibleForTesting
    static final String APPLIED_COLUMN = "[applied]";
    private static final String COLUMN_NAME_COLUMN = "column1";
    private static final String VALUE_COLUMN = "value";

    @VisibleForTesting
    static final byte[] SUCCESSFUL_OPERATION = {1};

    private CassandraTimestampUtils() {
        // utility class
    }

    public static ByteBuffer constructSelectFromTimestampTableQuery() {
        return toByteBuffer(String.format(
                "SELECT %s, %s FROM %s WHERE key=%s;",
                COLUMN_NAME_COLUMN,
                VALUE_COLUMN,
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                ROW_AND_COLUMN_NAME_HEX_STRING));
    }

    public static ByteBuffer constructCheckAndSetMultipleQuery(Map<String, Pair<byte[], byte[]>> checkAndSetRequest) {
        StringBuilder builder = new StringBuilder();
        builder.append("BEGIN UNLOGGED BATCH\n"); // Safe, because all updates are on the same partition key

        // Safe, because ordering does not apply in batches
        checkAndSetRequest.entrySet().forEach(entry -> {
            String columnName = entry.getKey();
            byte[] expected = entry.getValue().getLhSide();
            byte[] target = entry.getValue().getRhSide();
            builder.append(constructCheckAndSetQuery(columnName, expected, target));
        });
        builder.append("APPLY BATCH;");
        return toByteBuffer(builder.toString());
    }

    public static boolean wasOperationApplied(CqlResult cqlResult) {
        byte[] applied = getNamedColumnValue(getColumnsFromOnlyRow(cqlResult), APPLIED_COLUMN);
        return Arrays.equals(applied, SUCCESSFUL_OPERATION);
    }

    public static Optional<Long> getLongValueFromApplicationResult(CqlResult result) {
        try {
            byte[] timestampData = getNamedColumnValue(getColumnsFromOnlyRow(result), VALUE_COLUMN);
            Preconditions.checkState(
                    isValidTimestampData(timestampData),
                    "Byte array returned cannot be deserialized as a long.");
            return Optional.of(PtBytes.toLong(timestampData));
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    public static Map<String, byte[]> getValuesFromSelectionResult(CqlResult result) {
        return result.getRows().stream()
                .map(CqlRow::getColumns)
                .collect(Collectors.toMap(
                        cols -> PtBytes.toString(getNamedColumnValue(cols, COLUMN_NAME_COLUMN)),
                        cols -> getNamedColumnValue(cols, VALUE_COLUMN)));
    }

    public static boolean isValidTimestampData(byte[] data) {
        return data != null && data.length == Long.BYTES;
    }

    private static String encodeCassandraHexString(String string) {
        return encodeCassandraHexBytes(PtBytes.toBytes(string));
    }

    private static String encodeCassandraHexBytes(byte[] bytes) {
        return "0x" + BaseEncoding.base16().upperCase().encode(bytes);
    }

    private static ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(PtBytes.toBytes(string));
    }

    private static String wrapInQuotes(String tableName) {
        return "\"" + tableName + "\"";
    }

    private static String constructCheckAndSetQuery(String columnName, byte[] expected, byte[] target) {
        if (target == null) {
            return "";
        }
        if (expected == null) {
            return constructInsertIfNotExistsQuery(columnName, target);
        }
        return constructUpdateIfEqualQuery(columnName, expected, target);
    }

    private static List<Column> getColumnsFromOnlyRow(CqlResult cqlResult) {
        return Iterables.getOnlyElement(cqlResult.getRows()).getColumns();
    }

    private static Column getNamedColumn(List<Column> columns, String columnName) {
        return Iterables.getOnlyElement(
                columns.stream()
                        .filter(column -> columnName.equals(PtBytes.toString(column.getName())))
                        .collect(Collectors.toList()));
    }

    private static byte[] getNamedColumnValue(List<Column> columns, String columnName) {
        return getNamedColumn(columns, columnName).getValue();
    }

    private static String constructInsertIfNotExistsQuery(String columnName, byte[] target) {
        return String.format(
                "INSERT INTO %s (key, column1, column2, value) VALUES (%s, %s, %s, %s) IF NOT EXISTS;",
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                ROW_AND_COLUMN_NAME_HEX_STRING,
                encodeCassandraHexString(columnName),
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexBytes(target));
    }

    private static String constructUpdateIfEqualQuery(String columnName, byte[] expected, byte[] target) {
        return String.format(
                "UPDATE %s SET value=%s WHERE key=%s AND column1=%s AND column2=%s IF value=%s;",
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                encodeCassandraHexBytes(target),
                ROW_AND_COLUMN_NAME_HEX_STRING,
                encodeCassandraHexString(columnName),
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexBytes(expected));
    }
}
