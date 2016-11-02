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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public class OracleTableNameMapperTest {
    private static ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
    private static SqlConnection sqlConnection = mock(SqlConnection.class);

    private static final String TEST_PREFIX = "a_";
    private static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    private static final String LONG_TABLE_NAME = "ThisIsAVeryLongTableNameThatWillExceed";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowIfTable99999Exists() {
        when(connectionSupplier.get()).thenReturn(sqlConnection);
        OracleTableNameMapper oracleTableNameMapper = new OracleTableNameMapper(connectionSupplier);

        AgnosticResultSet resultSet = mock(AgnosticResultSet.class);
        when(resultSet.size()).thenReturn(1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getString(eq("short_table_name"))).thenReturn(getTableNameWithNumber(99999));
        when(resultSet.get(eq(0))).thenReturn(row);

        when(sqlConnection
                .selectResultSetUnregisteredQuery(startsWith("SELECT short_table_name FROM atlasdb_table_names")))
                .thenReturn(resultSet);

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Cannot create any more tables with name starting with a_te__ThisIsAVeryLongTab");
        oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);
    }

    private String getTableNameWithNumber(int tableNum) {
        return String.format("a_te__ThisIsAVeryLongTab_%05d", tableNum);
    }
}
