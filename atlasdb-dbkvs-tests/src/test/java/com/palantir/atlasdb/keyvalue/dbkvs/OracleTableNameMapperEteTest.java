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
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;

public class OracleTableNameMapperEteTest {
    private final OracleTableNameMapper oracleTableNameMapper =
            new OracleTableNameMapper(DbkvsOracleTestSuite.getConnectionSupplier());
    private final ConnectionManagerAwareDbKvs kvs = ConnectionManagerAwareDbKvs.create(
            DbkvsOracleTestSuite.getKvsConfig());

    private static final int ORACLE_TABLE_NAME_LENGTH_LIMIT = 30;
    private static final String TEST_PREFIX = "a_";
    private static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    private static final String LONG_TABLE_NAME = "ThisIsAVeryLongTableNameThatWillExceed";

    @After
    public void tearDown() {
        kvs.dropTables(kvs.getAllTableNames());
    }

    @Test
    public void shouldNotModifyTableNameLessThanTwentyFourCharactersLong() {
        TableReference tableRef = TableReference.createFromFullyQualifiedName("ns.test_table");
        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);

        assertThat(shortPrefixedTableName.length(), lessThan(ORACLE_TABLE_NAME_LENGTH_LIMIT));
        String expectedName = "a_ns__test_table";
        assertThat(shortPrefixedTableName, is(expectedName));
    }

    @Test
    public void shouldNotModifyWhenPrefixedTableNameIsTwentyFourCharactersLong() {
        String tableName = LONG_TABLE_NAME.substring(0, 18); // 6 characters "a_ns__" will be prefixed
        TableReference tableRef = TableReference.createFromFullyQualifiedName("ns." + tableName);
        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);

        assertThat(shortPrefixedTableName.length(), is(24));
        String expectedName = "a_ns__" + tableName;
        assertThat(shortPrefixedTableName, is(expectedName));
    }

    @Test
    public void shouldModifyWhenPrefixedTableNameIsTwentyFiveCharactersLong() {
        String tableName = LONG_TABLE_NAME.substring(0, 19); // 6 characters "a_te__" will be prefixed
        TableReference tableRef = TableReference.createFromFullyQualifiedName("te." + tableName);
        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);

        assertLengthAndValueOfName(shortPrefixedTableName, 0);
    }

    @Test
    public void shouldReturnValidTableNameWhenNoTablesExist() {
        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);

        assertLengthAndValueOfName(shortPrefixedTableName, 0);
    }

    @Test
    public void shouldReturnValidTableNameWhenOneTableExists() {
        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        kvs.createTable(tableRef, AtlasDbConstants.GENERIC_TABLE_METADATA);

        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 1);
    }

    @Test
    public void shouldReturnValidTableNameWhenMultipleTablesExists() {
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"), AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"), AtlasDbConstants.GENERIC_TABLE_METADATA);
        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v3");

        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 2);
    }

    @Test
    public void shouldReturnLatestTableNameWhenOneTableExistsButOutOfOrder() {
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"), AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"), AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v3"), AtlasDbConstants.GENERIC_TABLE_METADATA);

        kvs.dropTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"));
        kvs.dropTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"));

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 3);
    }

    @Test
    public void shouldReturnLatestTableNameWhenMultipleTablesExistsButOutOfOrder() {
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"), AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"), AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v3"), AtlasDbConstants.GENERIC_TABLE_METADATA);

        kvs.dropTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"));

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName = oracleTableNameMapper.getShortPrefixedTableName(TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 3);
    }

    private void assertLengthAndValueOfName(String shortPrefixedTableName, int tableNum) {
        assertThat(shortPrefixedTableName.length(), is(ORACLE_TABLE_NAME_LENGTH_LIMIT));
        String expectedName = TEST_PREFIX + getTableRefWithNumber(tableNum).getTablename();
        assertThat(shortPrefixedTableName, is(expectedName));
    }

    private TableReference getTableRefWithNumber(int tableNum) {
        return TableReference.createWithEmptyNamespace(String.format("te__ThisIsAVeryLongTab_%05d", tableNum));
    }
}
