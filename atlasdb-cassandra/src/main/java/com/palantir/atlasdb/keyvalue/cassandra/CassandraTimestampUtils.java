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
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;

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

    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final String ROW_AND_COLUMN_NAME_HEX_STRING = encodeCassandraHexString(ROW_AND_COLUMN_NAME);

    private static final String APPLIED_COLUMN = "[applied]";
    private static final String VALUE_COLUMN = "value";

    private static final byte[] SUCCESSFUL_OPERATION = {1};

    private static final String BACKUP_COLUMN_NAME = "oldTs";
    private static final byte[] INVALIDATED_VALUE = new byte[1];

    private CassandraTimestampUtils() {
        // utility class
    }

    public static ByteBuffer constructSelectTimestampBoundQuery() {
        String selectQuery = String.format(
                "SELECT %s FROM %s WHERE key=%s AND column1=%s AND column2=%s;",
                VALUE_COLUMN,
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                ROW_AND_COLUMN_NAME_HEX_STRING,
                ROW_AND_COLUMN_NAME_HEX_STRING,
                CASSANDRA_TIMESTAMP);
        return ByteBuffer.wrap(PtBytes.toBytes(selectQuery));
    }

    public static ByteBuffer constructCheckAndSetCqlQuery(Long expected, long target) {
        String updateQuery = (expected == null)
                ? constructInsertIfNotExistsQuery(target)
                : constructUpdateIfEqualQuery(expected, target);

        return ByteBuffer.wrap(PtBytes.toBytes(updateQuery));
    }

    public static boolean wasOperationApplied(CqlResult cqlResult) {
        Column appliedColumn = getNamedColumn(cqlResult, APPLIED_COLUMN);
        return Arrays.equals(appliedColumn.getValue(), SUCCESSFUL_OPERATION);
    }

    public static long getLongValue(CqlResult cqlResult) {
        Column valueColumn = getNamedColumn(cqlResult, VALUE_COLUMN);
        if (Arrays.equals(INVALIDATED_VALUE, valueColumn.getValue())) {
            throw new IllegalStateException("Couldn't extract a long from the invalidated value!");
        }
        return PtBytes.toLong(valueColumn.getValue());
    }

    private static Column getNamedColumn(CqlResult cqlResult, String columnName) {
        List<Column> columns = getColumnsFromOnlyRow(cqlResult);
        return Iterables.getOnlyElement(
                columns.stream()
                        .filter(column -> columnName.equals(PtBytes.toString(column.getName())))
                        .collect(Collectors.toList()));
    }

    private static String constructUpdateIfEqualQuery(Long expected, long target) {
        return String.format(
                "UPDATE %s SET value=%s WHERE key=%s AND column1=%s AND column2=%s IF value=%s;",
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                encodeCassandraHexBytes(PtBytes.toBytes(target)),
                ROW_AND_COLUMN_NAME_HEX_STRING,
                ROW_AND_COLUMN_NAME_HEX_STRING,
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexBytes(PtBytes.toBytes(expected)));
    }

    private static String constructInsertIfNotExistsQuery(long target) {
        return String.format(
                "INSERT INTO %s (key, column1, column2, value) VALUES (%s, %s, %s, %s) IF NOT EXISTS;",
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                ROW_AND_COLUMN_NAME_HEX_STRING,
                ROW_AND_COLUMN_NAME_HEX_STRING,
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexBytes(PtBytes.toBytes(target)));
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
}
