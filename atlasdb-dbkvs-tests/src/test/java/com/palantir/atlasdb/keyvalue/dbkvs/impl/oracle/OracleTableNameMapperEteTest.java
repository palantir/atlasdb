/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameMapper;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OracleTableNameMapperEteTest {
    @ClassRule
    public static final TestResourceManager TRM =
            new TestResourceManager(() -> ConnectionManagerAwareDbKvs.create(DbKvsOracleTestSuite.getKvsConfig()));

    private KeyValueService kvs;
    private ConnectionSupplier connectionSupplier;
    private static final OracleTableNameMapper ORACLE_TABLE_NAME_MAPPER = new OracleTableNameMapper();
    private static final String TEST_PREFIX = "a_";

    private static final int POST_NAMESPACE_UNDERSCORES = 2;
    private static final int PRE_SUFFIX_NUMBER_UNDERSCORE = 1;
    private static final int TABLE_NAME_LENGTH_WITHOUT_PREFIXES_OR_SUFFIXES =
            AtlasDbConstants.ATLASDB_ORACLE_TABLE_NAME_LIMIT
                    - (TEST_PREFIX.length() + OracleTableNameMapper.MAX_NAMESPACE_LENGTH + POST_NAMESPACE_UNDERSCORES)
                    - (PRE_SUFFIX_NUMBER_UNDERSCORE + OracleTableNameMapper.SUFFIX_NUMBER_LENGTH);
    private static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    private static final String LONG_TABLE_NAME = "ThisIsAVeryLongTableNameThatWillExceed";

    @Before
    public void setup() {
        kvs = TRM.getDefaultKvs();
        connectionSupplier = DbKvsOracleTestSuite.getConnectionSupplier(kvs);
    }

    @After
    public void tearDown() {
        kvs.dropTables(kvs.getAllTableNames());
        connectionSupplier.close();
    }

    @Test
    public void shouldModifyShortTableName() {
        TableReference tableRef = TableReference.createFromFullyQualifiedName("ns.test_table");
        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);

        assertThat(shortPrefixedTableName.length(), lessThan(AtlasDbConstants.ATLASDB_ORACLE_TABLE_NAME_LIMIT));
        String expectedName = "a_ns__test_table_0000";
        assertThat(shortPrefixedTableName, is(expectedName));
    }

    @Test
    public void shouldReturnCorrectTableNameWhenNoTruncationIsNeeded() {
        String tableName = LONG_TABLE_NAME.substring(0, TABLE_NAME_LENGTH_WITHOUT_PREFIXES_OR_SUFFIXES);
        TableReference tableRef = TableReference.createFromFullyQualifiedName("ns." + tableName);
        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);

        assertThat(shortPrefixedTableName.length(), is(AtlasDbConstants.ATLASDB_ORACLE_TABLE_NAME_LIMIT));
        String expectedName = "a_ns__" + tableName + "_0000";
        assertThat(shortPrefixedTableName, is(expectedName));
    }

    @Test
    public void shouldReturnCorrectTableNameWhenTruncationIsNeededForOnlyOneCharacter() {
        String tableName = LONG_TABLE_NAME.substring(0, TABLE_NAME_LENGTH_WITHOUT_PREFIXES_OR_SUFFIXES + 1);
        TableReference tableRef = TableReference.createFromFullyQualifiedName("te." + tableName);
        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);

        assertLengthAndValueOfName(shortPrefixedTableName, 0);
    }

    @Test
    public void shouldReturnValidTableNameWhenNoTablesExist() {
        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);

        assertLengthAndValueOfName(shortPrefixedTableName, 0);
    }

    @Test
    public void shouldReturnValidTableNameWhenOneTableExists() {
        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        kvs.createTable(tableRef, AtlasDbConstants.GENERIC_TABLE_METADATA);

        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 1);
    }

    @Test
    public void shouldReturnValidTableNameWhenMultipleTablesExists() {
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v3");

        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 2);
    }

    @Test
    public void shouldReturnLatestTableNameWhenOneTableExistsButOutOfOrder() {
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v3"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);

        kvs.dropTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"));
        kvs.dropTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"));

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 3);
    }

    @Test
    public void shouldReturnLatestTableNameWhenMultipleTablesExistsButOutOfOrder() {
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v1"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(
                TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v3"),
                AtlasDbConstants.GENERIC_TABLE_METADATA);

        kvs.dropTable(TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "_v2"));

        TableReference tableRef = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
        String shortPrefixedTableName =
                ORACLE_TABLE_NAME_MAPPER.getShortPrefixedTableName(connectionSupplier, TEST_PREFIX, tableRef);
        assertLengthAndValueOfName(shortPrefixedTableName, 3);
    }

    private void assertLengthAndValueOfName(String shortPrefixedTableName, int tableNum) {
        assertThat(shortPrefixedTableName.length(), is(AtlasDbConstants.ATLASDB_ORACLE_TABLE_NAME_LIMIT));
        String expectedName = TEST_PREFIX + getTableRefWithNumber(tableNum).getTablename();
        assertThat(shortPrefixedTableName, is(expectedName));
    }

    private TableReference getTableRefWithNumber(int tableNum) {
        return TableReference.createWithEmptyNamespace(
                String.format("te__ThisIsAVeryLongT_%0" + OracleTableNameMapper.SUFFIX_NUMBER_LENGTH + "d", tableNum));
    }
}
