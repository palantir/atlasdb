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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;

public class CassandraTimestampUtilsTest {
    @Test
    public void canIdentifyDataManipulationAsSuccessful() {
        CqlResult result = createMockSingleColumnCqlResult(
                CassandraTimestampUtils.APPLIED_COLUMN,
                CassandraTimestampUtils.SUCCESSFUL_OPERATION);
        assertThat(CassandraTimestampUtils.wasOperationApplied(result)).isTrue();
    }

    @Test
    public void canIdentifyDataManipulationAsUnsuccessful() {
        byte[] failedOperation = {0};
        CqlResult result = createMockSingleColumnCqlResult(
                CassandraTimestampUtils.APPLIED_COLUMN,
                failedOperation);
        assertThat(CassandraTimestampUtils.wasOperationApplied(result)).isFalse();
    }

    @Test
    public void throwsOnDataManipulationIfResultIsMissingAppliedColumn() {
        CqlResult result = createMockSingleColumnCqlResult(
                "foo",
                CassandraTimestampUtils.SUCCESSFUL_OPERATION);
        assertThatThrownBy(() -> CassandraTimestampUtils.wasOperationApplied(result))
                .isInstanceOf(NoSuchElementException.class);
    }

    private static CqlResult createMockSingleColumnCqlResult(String name, byte[] value) {
        return createMockCqlResult(ImmutableList.of(createColumn(name, value)));
    }

    private static CqlResult createMockCqlResult(List<Column> columns) {
        CqlRow row = mock(CqlRow.class);
        when(row.getColumns()).thenReturn(columns);
        CqlResult result = mock(CqlResult.class);
        when(result.getRows()).thenReturn(ImmutableList.of(row));
        return result;
    }

    private static Column createColumn(String name, byte[] value) {
        Column column = new Column();
        column.setName(PtBytes.toBytes(name));
        column.setValue(value);
        return column;
    }
}
