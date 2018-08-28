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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.logsafe.SafeArg;
import com.palantir.util.Pair;

public final class CassandraTimestampUtils {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampUtils.class);

    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription.Builder().componentName("timestamp_name").type(ValueType.STRING)
                            .build())),
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            "current_max_ts",
                            ColumnValueDescription.forType(ValueType.FIXED_LONG)))),
            ConflictHandler.IGNORE_ALL,
            TableMetadataPersistence.LogSafety.SAFE);

    public static final String ROW_AND_COLUMN_NAME = "ts";
    public static final String BACKUP_COLUMN_NAME = "oldTs";
    public static final ByteString INVALIDATED_VALUE = ByteString.copyFrom(new byte[] {0});
    public static final long INITIAL_VALUE = 10000;

    private static final long CASSANDRA_TIMESTAMP = -1;
    private static final String ROW_AND_COLUMN_NAME_HEX_STRING = encodeCassandraHexString(ROW_AND_COLUMN_NAME);
    private static final ByteString CQL_SUCCESS = ByteString.copyFrom(new byte[] {1});

    @VisibleForTesting
    static final String APPLIED_COLUMN = "[applied]";
    static final String COLUMN_NAME_COLUMN = "column1";
    static final String VALUE_COLUMN = "value";

    private CassandraTimestampUtils() {
        // utility class
    }

    public static CqlQuery constructCheckAndSetMultipleQuery(Map<String, Pair<byte[], byte[]>> checkAndSetRequest) {
        StringBuilder builder = new StringBuilder();
        builder.append("BEGIN UNLOGGED BATCH\n"); // Safe, because all updates are on the same partition key

        // Safe, because ordering does not apply in batches
        checkAndSetRequest.forEach((columnName, value) -> {
            byte[] expected = value.getLhSide();
            byte[] target = value.getRhSide();
            builder.append(constructCheckAndSetQuery(columnName, expected, target));
        });
        builder.append("APPLY BATCH;");

        // This looks awkward. However, we know that all expressions in this String pertain to timestamps and known
        // table references, hence this is actually safe. Doing this quickly owing to priority.
        // TODO (jkong): Build up a query by passing around legitimate formats and args.
        return CqlQuery.builder()
                .safeQueryFormat(builder.toString())
                .build();
    }

    public static boolean isValidTimestampData(byte[] data) {
        return data != null && data.length == Long.BYTES;
    }

    public static CqlQuery constructSelectFromTimestampTableQuery() {
        // Timestamps are safe.
        return ImmutableCqlQuery.builder()
                .safeQueryFormat("SELECT %s, %s FROM %s WHERE key=%s;")
                .addArgs(
                        SafeArg.of("columnName", COLUMN_NAME_COLUMN),
                        SafeArg.of("valueColumnName", VALUE_COLUMN),
                        SafeArg.of("tableRef", wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName())),
                        SafeArg.of("rowAndColumnValue", ROW_AND_COLUMN_NAME_HEX_STRING))
                .build();
    }

    public static Map<String, byte[]> getValuesFromSelectionResult(CqlResult result) {
        return result.getRows().stream()
                .map(CqlRow::getColumns)
                .collect(Collectors.toMap(
                        cols -> PtBytes.toString(getNamedColumnValue(cols, COLUMN_NAME_COLUMN)),
                        cols -> getNamedColumnValue(cols, VALUE_COLUMN)));
    }

    /**
     * This method checks that a result from a Cassandra lightweight transaction is either
     *
     *  (a) a successful application of proposed changes, or
     *  (b) an unsuccessful application, but the state of the world in Cassandra as indicated by the casResult
     *      matches the casMap.
     *
     * Otherwise, it throws an IllegalStateException.
     *
     * @param casResult the CAS result to verify
     * @param casMap the intended state of the world
     * @throws IllegalStateException if the transaction was not successful, and the state of the world does not match
     *         the intended state, as indicated in the casMap
     */
    public static void verifyCompatible(CqlResult casResult, Map<String, Pair<byte[], byte[]>> casMap) {
        if (!isSuccessfullyApplied(casResult)) {
            Set<Incongruency> incongruencies = getIncongruencies(casResult, casMap);
            if (!incongruencies.isEmpty()) {
                throw new IllegalStateException("[BACKUP/RESTORE] Observed incongruent state of the world; the"
                        + " following incongruencies were observed: " + incongruencies);
            }
            /*
             * If there are no incongruencies, then the state of the world is what we wanted it to be - possibly
             * because we were upgrading and this call was made in parallel. Since the DB is in the desired state,
             * we're continuing here.
             */
            log.debug("[BACKUP/RESTORE] CAS failed, but the DB state is as desired - continuing.");
        }
    }

    private static boolean isSuccessfullyApplied(CqlResult casResult) {
        byte[] appliedValue = getNamedColumnValue(casResult.getRows().get(0).getColumns(), APPLIED_COLUMN);
        return Arrays.equals(appliedValue, CQL_SUCCESS.toByteArray());
    }

    private static Set<Incongruency> getIncongruencies(CqlResult casResult, Map<String, Pair<byte[], byte[]>> casMap) {
        Map<String, byte[]> relevantCassandraState = getRelevantCassandraState(casResult, casMap);
        return casMap.entrySet().stream()
                .filter(entry ->
                        !Arrays.equals(relevantCassandraState.get(entry.getKey()), entry.getValue().getRhSide()))
                .map(entry -> createIncongruency(relevantCassandraState, entry.getKey(), entry.getValue().getRhSide()))
                .collect(Collectors.toSet());
    }

    private static Map<String, byte[]> getRelevantCassandraState(
            CqlResult casResult,
            Map<String, Pair<byte[], byte[]>> casMap) {
        return casResult.getRows().stream()
                    .filter(row -> {
                        String columnName = getColumnNameFromRow(row);
                        return casMap.containsKey(columnName);
                    })
                    .collect(Collectors.toMap(
                            CassandraTimestampUtils::getColumnNameFromRow,
                            row -> getNamedColumnValue(row.getColumns(), VALUE_COLUMN)));
    }

    private static String getColumnNameFromRow(CqlRow row) {
        return PtBytes.toString(getNamedColumnValue(row.getColumns(), COLUMN_NAME_COLUMN));
    }

    private static Incongruency createIncongruency(
            Map<String, byte[]> relevantCassandraState,
            String columnName,
            byte[] desiredValue) {
        return ImmutableIncongruency.builder()
                .columnName(columnName)
                .desiredState(desiredValue)
                .actualState(relevantCassandraState.get(columnName))
                .build();
    }

    private static byte[] getNamedColumnValue(List<Column> columns, String columnName) {
        return getNamedColumn(columns, columnName).getValue();
    }

    private static Column getNamedColumn(List<Column> columns, String columnName) {
        return Iterables.getOnlyElement(
                columns.stream()
                        .filter(column -> columnName.equals(PtBytes.toString(column.getName())))
                        .collect(Collectors.toList()));
    }

    private static String constructCheckAndSetQuery(String columnName, byte[] expected, byte[] target) {
        Preconditions.checkState(target != null, "Should not CAS to a null target!");
        if (expected == null) {
            return constructInsertIfNotExistsQuery(columnName, target);
        }
        return constructUpdateIfEqualQuery(columnName, expected, target);
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

    private static String encodeCassandraHexString(String string) {
        return encodeCassandraHexBytes(PtBytes.toBytes(string));
    }

    private static String encodeCassandraHexBytes(byte[] bytes) {
        return "0x" + BaseEncoding.base16().upperCase().encode(bytes);
    }

    private static String wrapInQuotes(String tableName) {
        return "\"" + tableName + "\"";
    }

    @Value.Immutable
    interface Incongruency {
        String columnName();
        byte[] desiredState();
        @Nullable
        byte[] actualState();
    }
}
