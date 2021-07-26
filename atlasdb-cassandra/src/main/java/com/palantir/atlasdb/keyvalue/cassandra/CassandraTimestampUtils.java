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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.Pair;
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

public final class CassandraTimestampUtils {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraTimestampUtils.class);

    public static final String ROW_AND_COLUMN_NAME = CassandraTimestampBoundStore.ROW_AND_COLUMN_NAME;
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
        ImmutableCqlSinglePartitionBatchQuery.Builder batchQueryBuilder = CqlSinglePartitionBatchQuery.builder();
        checkAndSetRequest.forEach((columnName, value) -> batchQueryBuilder.addIndividualQueryStatements(
                constructCheckAndSetQuery(columnName, value.getLhSide(), value.getRhSide())));
        return batchQueryBuilder.build().toCqlQuery();
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
                .filter(entry -> !Arrays.equals(
                        relevantCassandraState.get(entry.getKey()),
                        entry.getValue().getRhSide()))
                .map(entry -> createIncongruency(
                        relevantCassandraState, entry.getKey(), entry.getValue().getRhSide()))
                .collect(Collectors.toSet());
    }

    private static Map<String, byte[]> getRelevantCassandraState(
            CqlResult casResult, Map<String, Pair<byte[], byte[]>> casMap) {
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
            Map<String, byte[]> relevantCassandraState, String columnName, byte[] desiredValue) {
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
        return Iterables.getOnlyElement(columns.stream()
                .filter(column -> columnName.equals(PtBytes.toString(column.getName())))
                .collect(Collectors.toList()));
    }

    private static CqlQuery constructCheckAndSetQuery(String columnName, byte[] expected, byte[] target) {
        Preconditions.checkState(target != null, "Should not CAS to a null target!");
        if (expected == null) {
            return constructInsertIfNotExistsQuery(columnName, target);
        }
        return constructUpdateIfEqualQuery(columnName, expected, target);
    }

    private static CqlQuery constructInsertIfNotExistsQuery(String columnName, byte[] target) {
        return CqlQuery.builder()
                .safeQueryFormat(
                        "INSERT INTO \"%s\" (key, column1, column2, value)" + " VALUES (%s, %s, %s, %s) IF NOT EXISTS;")
                .addArgs(
                        LoggingArgs.internalTableName(AtlasDbConstants.TIMESTAMP_TABLE),
                        SafeArg.of("rowAndColumnName", ROW_AND_COLUMN_NAME_HEX_STRING),
                        SafeArg.of("columnName", encodeCassandraHexString(columnName)),
                        SafeArg.of("atlasTimestamp", CASSANDRA_TIMESTAMP),
                        SafeArg.of("newValue", encodeCassandraHexBytes(target)))
                .build();
    }

    private static CqlQuery constructUpdateIfEqualQuery(String columnName, byte[] expected, byte[] target) {
        // Timestamps are safe.
        return CqlQuery.builder()
                .safeQueryFormat("UPDATE \"%s\" SET value=%s WHERE key=%s AND column1=%s AND column2=%s IF value=%s;")
                .addArgs(
                        LoggingArgs.internalTableName(AtlasDbConstants.TIMESTAMP_TABLE),
                        SafeArg.of("newValue", encodeCassandraHexBytes(target)),
                        SafeArg.of("rowAndColumnName", ROW_AND_COLUMN_NAME_HEX_STRING),
                        SafeArg.of("columnName", encodeCassandraHexString(columnName)),
                        SafeArg.of("atlasTimestamp", CASSANDRA_TIMESTAMP),
                        SafeArg.of("oldValue", encodeCassandraHexBytes(expected)))
                .build();
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
