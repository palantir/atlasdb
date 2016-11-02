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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyObject;
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

public class OracleTableNameUnmapperTest {

    private static final String TEST_PREFIX = "a_";
    private static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    private static final String LONG_TABLE_NAME = "ThisIsAVeryLongTableNameThatWillExceed";
    private static final String SHORT_TABLE_NAME = "testShort";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowIfTableMappingDoesNotExist() throws TableMappingNotFoundException {
        ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
        OracleTableNameUnmapper oracleTableNameUnmapper = new OracleTableNameUnmapper(connectionSupplier);

        SqlConnection sqlConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(sqlConnection);

        setupResultSetForSqlConnection(sqlConnection, 0);

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        expectedException.expect(TableMappingNotFoundException.class);
        expectedException.expectMessage("The table a_test_namespace__ThisIsAVeryLongTableNameThatWillExceed");
        oracleTableNameUnmapper.getShortTableNameFromMappingTable(TEST_PREFIX, tableRef);
    }

    @Test
    public void shouldReturnIfTableMappingExists() throws TableMappingNotFoundException {
        ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
        OracleTableNameUnmapper oracleTableNameUnmapper = new OracleTableNameUnmapper(connectionSupplier);

        SqlConnection sqlConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(sqlConnection);

        AgnosticResultSet resultSet = setupResultSetForSqlConnection(sqlConnection, 1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(resultSet.get(eq(0))).thenReturn(row);
        when(row.getString(eq("short_table_name"))).thenReturn(SHORT_TABLE_NAME);

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortName = oracleTableNameUnmapper.getShortTableNameFromMappingTable(TEST_PREFIX, tableRef);
        assertThat(shortName, is(SHORT_TABLE_NAME));
    }

    private AgnosticResultSet setupResultSetForSqlConnection(SqlConnection sqlConnection, int resultSize) {
        AgnosticResultSet resultSet = mock(AgnosticResultSet.class);
        when(sqlConnection
                .selectResultSetUnregisteredQuery(
                        startsWith("SELECT short_table_name FROM atlasdb_table_names WHERE table_name"), anyObject()))
                .thenReturn(resultSet);
        when(resultSet.size()).thenReturn(resultSize);
        return resultSet;
    }
}
