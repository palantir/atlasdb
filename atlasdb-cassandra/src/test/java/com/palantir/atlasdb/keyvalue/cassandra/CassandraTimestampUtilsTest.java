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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.util.Pair;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.junit.Test;

public class CassandraTimestampUtilsTest {
    private static final byte[] KEY_1 = {120};
    private static final String COLUMN_NAME_1 = "foo";
    private static final byte[] COLUMN_BYTES_1 = PtBytes.toBytes(COLUMN_NAME_1);
    private static final byte[] VALUE_1 = {3, 1, 4, 1};
    private static final byte[] KEY_2 = {121};
    private static final String COLUMN_NAME_2 = "bar";
    private static final byte[] COLUMN_BYTES_2 = PtBytes.toBytes(COLUMN_NAME_2);
    private static final byte[] VALUE_2 = {5, 9, 2, 6};
    private static final String COLUMN_NAME_3 = "baz";
    private static final byte[] COLUMN_BYTES_3 = PtBytes.toBytes(COLUMN_NAME_3);
    private static final byte[] VALUE_3 = {5, 3, 5, 8};
    private static final byte[] CQL_SUCCESS = {1};
    private static final byte[] CQL_FAILURE = {0};
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final Map<String, Pair<byte[], byte[]>> CAS_MAP_TWO_COLUMNS
            = ImmutableMap.<String, Pair<byte[], byte[]>>builder()
            .put(COLUMN_NAME_1, Pair.create(EMPTY_BYTE_ARRAY, VALUE_1))
            .put(COLUMN_NAME_2, Pair.create(EMPTY_BYTE_ARRAY, VALUE_2))
            .build();

    @Test
    public void canGetValuesFromSelectionResult() {
        List<Column> columnList1 = buildKeyValueColumnList(KEY_1, COLUMN_BYTES_1, VALUE_1);
        List<Column> columnList2 = buildKeyValueColumnList(KEY_2, COLUMN_BYTES_2, VALUE_2);

        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(
                        createMockCqlRow(columnList1),
                        createMockCqlRow(columnList2)));
        assertThat(CassandraTimestampUtils.getValuesFromSelectionResult(mockResult))
                .isEqualTo(ImmutableMap.of(COLUMN_NAME_1, VALUE_1, COLUMN_NAME_2, VALUE_2));
    }

    @Test
    public void canGetSelectQuery() {
        CqlQuery query = CassandraTimestampUtils.constructSelectFromTimestampTableQuery();
        assertThat(query.toString()).isEqualTo("SELECT column1, value FROM \"_timestamp\" WHERE key=0x7473;");
    }

    @Test
    public void longConvertedToBytesIsValidTimestampData() {
        assertThat(CassandraTimestampUtils.isValidTimestampData(PtBytes.toBytes(1234567L))).isTrue();
        assertThat(CassandraTimestampUtils.isValidTimestampData(new byte[Long.BYTES])).isTrue();
    }

    @Test
    public void invalidatedValueIsNotValidTimestampData() {
        assertThat(CassandraTimestampUtils.isValidTimestampData(
                CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray()))
                .isFalse();
    }

    @Test
    public void emptyByteArrayIsNotValidTimestampData() {
        assertThat(CassandraTimestampUtils.isValidTimestampData(EMPTY_BYTE_ARRAY)).isFalse();
    }

    @Test
    public void nullIsNotValidTimestampData() {
        assertThat(CassandraTimestampUtils.isValidTimestampData(null)).isFalse();
    }

    @Test
    public void largeByteArrayIsNotValidTimestampData() {
        assertThat(CassandraTimestampUtils.isValidTimestampData(new byte[100 * Long.BYTES])).isFalse();
    }

    @Test
    public void checkAndSetIsInsertIfNotExistsIfExpectedIsNull() {
        CqlQuery query = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                ImmutableMap.of(COLUMN_NAME_1, Pair.create(null, VALUE_1)));
        assertThat(query.toString()).contains("INSERT").contains("IF NOT EXISTS;");
    }

    @Test
    public void checkAndSetIsUpdateIfEqualIfExpectedIsNotNull() {
        CqlQuery query = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                ImmutableMap.of(COLUMN_NAME_1, Pair.create(VALUE_1, VALUE_2)));
        assertThat(query.toString()).contains("UPDATE").contains("IF " + CassandraTimestampUtils.VALUE_COLUMN + "=");
    }

    @Test
    public void checkAndSetGeneratesBatchedStatements() {
        CqlQuery query = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                ImmutableMap.of(COLUMN_NAME_1, Pair.create(VALUE_1, VALUE_2),
                        COLUMN_NAME_2, Pair.create(VALUE_2, VALUE_1)));
        assertThat(query.toString()).contains("BEGIN UNLOGGED BATCH").contains("APPLY BATCH;");
    }

    @Test
    public void checkAndSetThrowsIfTryingToSetToNull() {
        assertThatThrownBy(() -> CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                ImmutableMap.of(COLUMN_NAME_1, Pair.create(VALUE_1, null))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void appliedResultIsCompatible() {
        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(createMockCqlRow(buildAppliedColumnList())));
        CassandraTimestampUtils.verifyCompatible(mockResult, CAS_MAP_TWO_COLUMNS);
    }

    @Test
    public void unappliedResultIsCompatibleIfTheStateOfTheWorldMatchesTargets() {
        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_1, VALUE_1)),
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_2, VALUE_2))));
        CassandraTimestampUtils.verifyCompatible(mockResult, CAS_MAP_TWO_COLUMNS);
    }

    @Test
    public void unappliedResultIsCompatibleIfWeHaveExtraRows() {
        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_1, VALUE_1)),
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_2, VALUE_2)),
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_3, VALUE_3))));
        CassandraTimestampUtils.verifyCompatible(mockResult, CAS_MAP_TWO_COLUMNS);
    }

    @Test
    public void unappliedResultIsIncompatibleIfTheStateOfTheWorldMatchesExpecteds() {
        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_1, EMPTY_BYTE_ARRAY)),
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_2, EMPTY_BYTE_ARRAY))));
        assertThatThrownBy(() -> CassandraTimestampUtils.verifyCompatible(mockResult, CAS_MAP_TWO_COLUMNS))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void unappliedResultIsIncompatibleIfTheStateOfTheWorldIsIncorrect() {
        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_1, VALUE_2)),
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_2, VALUE_1))));
        assertThatThrownBy(() -> CassandraTimestampUtils.verifyCompatible(mockResult, CAS_MAP_TWO_COLUMNS))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void unappliedResultIsIncompatibleIfWeHaveMissingRows() {
        CqlResult mockResult = createMockCqlResult(
                ImmutableList.of(
                        createMockCqlRow(buildUnappliedColumnList(COLUMN_BYTES_1, VALUE_1))));
        assertThatThrownBy(() -> CassandraTimestampUtils.verifyCompatible(mockResult, CAS_MAP_TWO_COLUMNS))
                .isInstanceOf(IllegalStateException.class);
    }

    private static List<Column> buildAppliedColumnList() {
        return ImmutableList.<Column>builder()
                .add(createColumn(CassandraTimestampUtils.APPLIED_COLUMN, CQL_SUCCESS))
                .build();
    }

    private static List<Column> buildUnappliedColumnList(byte[] columnName, byte[] value) {
        return ImmutableList.<Column>builder()
                .add(createColumn(CassandraTimestampUtils.APPLIED_COLUMN, CQL_FAILURE))
                .add(createColumn(CassandraTimestampUtils.COLUMN_NAME_COLUMN, columnName))
                .add(createColumn(CassandraTimestampUtils.VALUE_COLUMN, value))
                .build();
    }

    private static List<Column> buildKeyValueColumnList(byte[] key, byte[] columnName, byte[] value) {
        return ImmutableList.<Column>builder()
                .add(createColumn("key", key))
                .add(createColumn(CassandraTimestampUtils.COLUMN_NAME_COLUMN, columnName))
                .add(createColumn(CassandraTimestampUtils.VALUE_COLUMN, value))
                .build();
    }

    private static CqlRow createMockCqlRow(List<Column> columns) {
        CqlRow row = mock(CqlRow.class);
        when(row.getColumns()).thenReturn(columns);
        return row;
    }

    private static CqlResult createMockCqlResult(List<CqlRow> rows) {
        CqlResult result = mock(CqlResult.class);
        when(result.getRows()).thenReturn(rows);
        return result;
    }

    private static Column createColumn(String name, byte[] value) {
        Column column = new Column();
        column.setName(PtBytes.toBytes(name));
        column.setValue(value);
        return column;
    }
}
