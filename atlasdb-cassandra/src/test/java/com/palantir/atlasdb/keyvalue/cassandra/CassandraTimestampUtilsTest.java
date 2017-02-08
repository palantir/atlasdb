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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;

public class CassandraTimestampUtilsTest {
    private static final long LONG_VALUE = 500L;

    private static final byte[] KEY_1 = {120};
    private static final String COLUMN_NAME_1 = "foo";
    private static final byte[] COLUMN_BYTES_1 = PtBytes.toBytes(COLUMN_NAME_1);
    private static final byte[] VALUE_1 = {4, 3, 2, 1};
    private static final byte[] KEY_2 = {121};
    private static final String COLUMN_NAME_2 = "bar";
    private static final byte[] COLUMN_BYTES_2 = PtBytes.toBytes(COLUMN_NAME_2);
    private static final byte[] VALUE_2 = {5, 9, 2, 6};

    @Test
    public void canIdentifyDataManipulationAsSuccessful() {
        CqlResult result = createMockSingleRowSingleColumnCqlResult(
                CassandraTimestampUtils.APPLIED_COLUMN,
                CassandraTimestampUtils.SUCCESSFUL_OPERATION);
        assertThat(CassandraTimestampUtils.wasOperationApplied(result)).isTrue();
    }

    @Test
    public void canIdentifyDataManipulationAsUnsuccessful() {
        byte[] failedOperation = {0};
        CqlResult result = createMockSingleRowSingleColumnCqlResult(
                CassandraTimestampUtils.APPLIED_COLUMN,
                failedOperation);
        assertThat(CassandraTimestampUtils.wasOperationApplied(result)).isFalse();
    }

    @Test
    public void throwsOnDataManipulationIfResultIsMissingAppliedColumn() {
        CqlResult result = createMockSingleRowSingleColumnCqlResult(
                "foo",
                CassandraTimestampUtils.SUCCESSFUL_OPERATION);
        assertThatThrownBy(() -> CassandraTimestampUtils.wasOperationApplied(result))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void canGetPresentLongValueFromApplicationResult() {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(createColumn(CassandraTimestampUtils.APPLIED_COLUMN, CassandraTimestampUtils.SUCCESSFUL_OPERATION))
                .add(createColumn(CassandraTimestampUtils.VALUE_COLUMN, PtBytes.toBytes(LONG_VALUE)))
                .build();
        CqlResult result = createMockSingleRowCqlResult(columns);
        assertThat(CassandraTimestampUtils.getLongValueFromApplicationResult(result))
                .isEqualTo(Optional.of(LONG_VALUE));
    }

    @Test
    public void canGetEmptyLongValueFromApplicationResult() {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(createColumn(CassandraTimestampUtils.APPLIED_COLUMN, CassandraTimestampUtils.SUCCESSFUL_OPERATION))
                .build();
        CqlResult result = createMockSingleRowCqlResult(columns);
        assertThat(CassandraTimestampUtils.getLongValueFromApplicationResult(result))
                .isEqualTo(Optional.empty());
    }

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

    private List<Column> buildKeyValueColumnList(byte[] key, byte[] columnName, byte[] value) {
        return ImmutableList.<Column>builder()
                .add(createColumn("key", key))
                .add(createColumn(CassandraTimestampUtils.COLUMN_NAME_COLUMN, columnName))
                .add(createColumn(CassandraTimestampUtils.VALUE_COLUMN, value))
                .build();
    }

    private static CqlResult createMockSingleRowSingleColumnCqlResult(String name, byte[] value) {
        return createMockSingleRowCqlResult(ImmutableList.of(createColumn(name, value)));
    }

    private static CqlResult createMockSingleRowCqlResult(List<Column> columns) {
        CqlResult result = mock(CqlResult.class);
        CqlRow mockRow = createMockCqlRow(columns);
        when(result.getRows()).thenReturn(ImmutableList.of(mockRow));
        return result;
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
