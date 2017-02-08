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
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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

    private static final long CASSANDRA_TIMESTAMP = -1;

    public static final String ROW_AND_COLUMN_NAME = "ts";
    private static final String ROW_AND_COLUMN_NAME_HEX_STRING = encodeCassandraHexString(ROW_AND_COLUMN_NAME);

    private static final String APPLIED_COLUMN = "[applied]";
    private static final String COLUMN_NAME_COLUMN = "column1";
    private static final String VALUE_COLUMN = "value";

    private static final byte[] SUCCESSFUL_OPERATION = {1};

    public static final String BACKUP_COLUMN_NAME = "oldTs";

    static final byte[] INVALIDATED_VALUE = new byte[1];

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

    private static String constructCheckAndSetQuery(String columnName, byte[] expected, byte[] target) {
        if (expected == null) {
            Preconditions.checkArgument(target != null, "[CAS] At least one of expected and target must be provided!");
            return constructInsertIfNotExistsQuery(columnName, target);
        }
        if (target == null) {
            // We treat this as a no op for now, which satisfies our purposes.
            return "";
        }
        return constructUpdateIfEqualQuery(columnName, expected, target);
    }

    public static boolean wasOperationApplied(CqlResult cqlResult) {
        Column appliedColumn = getNamedColumn(getColumnsFromOnlyRow(cqlResult), APPLIED_COLUMN);
        return Arrays.equals(appliedColumn.getValue(), SUCCESSFUL_OPERATION);
    }

    public static TimestampBoundStoreEntry getEntryFromApplicationResult(CqlResult result) {
        return TimestampBoundStoreEntry.createFromBytes(getNamedColumn(getColumnsFromOnlyRow(result), VALUE_COLUMN).getValue());
    }

    public static String getTimestampAsStringFromApplicationResult(CqlResult result) {
        return getEntryFromApplicationResult(result).getTimestampAsString();
    }

    public static Map<String, byte[]> getValuesFromSelectionResult(CqlResult result) {
        return result.getRows().stream()
                .map(CqlRow::getColumns)
                .collect(Collectors.toMap(
                        cols -> PtBytes.toString(getNamedColumn(cols, COLUMN_NAME_COLUMN).getValue()),
                        cols -> getNamedColumn(cols, VALUE_COLUMN).getValue()));
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

    private static String constructInsertIfNotExistsQuery(String columnName, byte[] target) {
        return String.format(
                "INSERT INTO %s (key, column1, column2, value) VALUES (%s, %s, %s, %s) IF NOT EXISTS;",
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                ROW_AND_COLUMN_NAME_HEX_STRING,
                encodeCassandraHexString(columnName),
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexBytes(target));
    }

    private static Column getNamedColumn(List<Column> columns, String columnName) {
        return Iterables.getOnlyElement(
                columns.stream()
                        .filter(column -> columnName.equals(PtBytes.toString(column.getName())))
                        .collect(Collectors.toList()));
    }

    private static String wrapInQuotes(String tableName) {
        return "\"" + tableName + "\"";
    }

    private static String encodeCassandraHexString(String string) {
        return encodeCassandraHexBytes(PtBytes.toBytes(string));
    }

    private static String encodeCassandraHexBytes(byte[] bytes) {
        return "0x" + DatatypeConverter.printHexBinary(bytes);
    }

    private static List<Column> getColumnsFromOnlyRow(CqlResult cqlResult) {
        return Iterables.getOnlyElement(cqlResult.getRows()).getColumns();
    }

    private static ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(PtBytes.toBytes(string));
    }
}
