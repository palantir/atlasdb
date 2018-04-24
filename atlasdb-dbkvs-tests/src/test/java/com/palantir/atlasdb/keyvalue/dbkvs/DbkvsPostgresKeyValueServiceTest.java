/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresDdlTable;

public class DbkvsPostgresKeyValueServiceTest extends AbstractDbKvsKeyValueServiceTest {
    private static final Namespace TEST_NAMESPACE = Namespace.create("ns");
    private static final String TEST_LONG_TABLE_NAME =
            "ThisShouldAlwaysBeAVeryLongTableNameThatExceedsPostgresLengthLimit";
    private static final int TWO_UNDERSCORES = 2;

    @Override
    protected KeyValueService getKeyValueService() {
        KeyValueService kvs = ConnectionManagerAwareDbKvs.create(DbkvsPostgresTestSuite.getKvsConfig());
        kvs.getAllTableNames().stream().filter(table -> !table.getQualifiedName().equals("_metadata")).forEach(
                kvs::dropTable);
        return kvs;
    }

    @Test(expected = RuntimeException.class)
    public void throwWhenCreatingDifferentLongTablesWithSameFirstCharactersUntilTheTableNameLimit() {
        String tableNameForFirstSixtyCharactersToBeSame = StringUtils.left(TEST_LONG_TABLE_NAME,
                PostgresDdlTable.ATLASDB_POSTGRES_TABLE_NAME_LIMIT - TEST_NAMESPACE.getName().length()
                        - TWO_UNDERSCORES);
        createTwoTablesWithSamePrefix(tableNameForFirstSixtyCharactersToBeSame);
    }

    @Test
    public void shouldNotThrowWhenCreatingDifferentLongTablesWithSameFirstCharactersUntilOneLessThanTableNameLimit() {
        String tableNameForFirstFiftyNineCharactersToBeSame = StringUtils.left(TEST_LONG_TABLE_NAME,
                PostgresDdlTable.ATLASDB_POSTGRES_TABLE_NAME_LIMIT - TEST_NAMESPACE.getName().length()
                        - TWO_UNDERSCORES - 1);
        createTwoTablesWithSamePrefix(tableNameForFirstFiftyNineCharactersToBeSame);
    }

    @Test
    public void shouldNotThrowWhenCreatingDifferentLongTablesWithDifferentFirstCharactersUntilTheTableNameLimit() {
        TableReference longTableName1 = TableReference.create(TEST_NAMESPACE, "a" + TEST_LONG_TABLE_NAME);
        TableReference longTableName2 = TableReference.create(TEST_NAMESPACE, "b" + TEST_LONG_TABLE_NAME);

        keyValueService.createTable(longTableName1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(longTableName2, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.dropTable(longTableName1);
        keyValueService.dropTable(longTableName2);
    }

    @Test(expected = RuntimeException.class)
    public void throwWhenCreatingDifferentLongTablesWithSameFirstCharactersAfterTheTableNameLimit() throws Exception {
        createTwoTablesWithSamePrefix(TEST_LONG_TABLE_NAME);
    }


    private void createTwoTablesWithSamePrefix(String tableNamePrefix) {
        TableReference longTableName1 = TableReference.create(TEST_NAMESPACE, tableNamePrefix + "1");
        TableReference longTableName2 = TableReference.create(TEST_NAMESPACE, tableNamePrefix + "2");
        try {
            keyValueService.createTable(longTableName1, AtlasDbConstants.GENERIC_TABLE_METADATA);
            keyValueService.createTable(longTableName2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        } finally {
            keyValueService.dropTable(longTableName1);
            keyValueService.dropTable(longTableName2);
        }
    }
}
