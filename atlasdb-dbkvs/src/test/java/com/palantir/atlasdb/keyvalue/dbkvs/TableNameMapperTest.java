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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public abstract class TableNameMapperTest {
    static final String TEST_PREFIX = "a_";
    static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    static final String LONG_TABLE_NAME =
            "ThisIsAVeryLongTableNameThatWillExceedEvenTheRelativelyLongerPostgresLimit";

    TableNameMapper tableNameMapper;
    AgnosticResultSet resultSet;
    ConnectionSupplier connectionSupplier;

    abstract TableNameMapper getTableNameMapper();
    abstract String getTableNameWithNumber(int tableNum);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        connectionSupplier = mock(ConnectionSupplier.class);
        tableNameMapper = getTableNameMapper();
        SqlConnection sqlConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(sqlConnection);
        resultSet = mock(AgnosticResultSet.class);
        when(sqlConnection
                .selectResultSetUnregisteredQuery(
                        startsWith("SELECT short_table_name FROM atlasdb_table_names WHERE LOWER(short_table_name)"),
                        anyObject()))
                .thenReturn(resultSet);
    }


    @Test
    public void shouldModifyTableNameForShortTableName() {
        when(resultSet.size()).thenReturn(0);

        TableReference tableRef = TableReference.create(Namespace.create("ns"), "short");
        String shortPrefixedTableName = tableNameMapper
                .getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertThat(shortPrefixedTableName, is("a_ns__short_00000"));
    }

    @Test
    public void shouldThrowIfTable99999Exists() {
        when(resultSet.size()).thenReturn(1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getString(eq("short_table_name"))).thenReturn(getTableNameWithNumber(99999));
        when(resultSet.get(eq(0))).thenReturn(row);

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Cannot create any more tables with name starting with");
        tableNameMapper.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
    }



}
