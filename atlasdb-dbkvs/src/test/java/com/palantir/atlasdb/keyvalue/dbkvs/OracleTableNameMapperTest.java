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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.PrimaryKeyConstraintNames;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OracleTableNameMapperTest {
    private static final String TEST_PREFIX = "a_";
    private static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    private static final String LONG_TABLE_NAME = "ThisIsAVeryLongTableNameThatWillExceed";

    private OracleTableNameMapper oracleTableNameMapper;
    private AgnosticResultSet resultSet;
    private ConnectionSupplier connectionSupplier;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        connectionSupplier = mock(ConnectionSupplier.class);
        oracleTableNameMapper = new OracleTableNameMapper();
        SqlConnection sqlConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(sqlConnection);
        resultSet = mock(AgnosticResultSet.class);
        when(sqlConnection.selectResultSetUnregisteredQuery(
                        startsWith("SELECT short_table_name FROM atlasdb_table_names WHERE LOWER(short_table_name)"),
                        anyObject()))
                .thenReturn(resultSet);
    }

    @Test
    public void shouldModifyTableNameForShortTableName() {
        when(resultSet.size()).thenReturn(0);

        TableReference tableRef = TableReference.create(Namespace.create("ns1"), "short");
        String shortPrefixedTableName =
                oracleTableNameMapper.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertThat(shortPrefixedTableName).isEqualTo("a_ns__short_0000");
    }

    @Test
    public void shouldThrowIfTable9999Exists() {
        when(resultSet.size()).thenReturn(1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getString(eq("short_table_name"))).thenReturn(getTableNameWithNumber(9999));
        when(resultSet.get(eq(0))).thenReturn(row);

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot create any more tables with name starting with a_te__ThisIsAVeryLongT");
        oracleTableNameMapper.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
    }

    @Test
    public void shouldGetRightPrimaryKeyConstraintForTableNamesWithinLimits() {
        String tableName = "table";
        String pkConstraintName = PrimaryKeyConstraintNames.get(tableName);
        assertThat(pkConstraintName).isEqualTo(AtlasDbConstants.PRIMARY_KEY_CONSTRAINT_PREFIX + tableName);
    }

    private String getTableNameWithNumber(int tableNum) {
        return String.format("a_te__ThisIsAVeryLongT_%0" + OracleTableNameMapper.SUFFIX_NUMBER_LENGTH + "d", tableNum);
    }
}
