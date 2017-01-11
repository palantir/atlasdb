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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticResultRow;

public class PostgresTableNameMapperTest extends TableNameMapperTest {
    @Override
    TableNameMapper getTableNameMapper() {
        return new PostgresTableNameMapper();
    }


    @Test
    public void shouldModifyNamespaceNameIfLong() {
        when(resultSet.size()).thenReturn(0);

        TableReference tableRef = TableReference.create(Namespace.create("reallyLongNamespaceName"), "short");
        String shortPrefixedTableName = tableNameMapper
                .getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertThat(shortPrefixedTableName, is("a_reallyLong__short_00000"));
    }

    @Override
    String getTableNameWithNumber(int tableNum) {
        return String.format("a_test_names__ThisIsAVeryLongTableNameThatWillExceedEvenT_%05d", tableNum);
    }

    @Test
    public void shouldNumericallyRemapOtherwiseOverlappingTablenames() {
        when(resultSet.size()).thenReturn(1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getString(eq("short_table_name"))).thenReturn(getTableNameWithNumber(10));
        when(resultSet.get(eq(0))).thenReturn(row);

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName = tableNameMapper.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX,
                tableRef);
        assertThat(shortPrefixedTableName, is("a_test_names__ThisIsAVeryLongTableNameThatWillExceedEvenT_00011"));
    }

}
