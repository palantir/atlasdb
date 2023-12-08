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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.AbstractDbKvsKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(DbKvsPostgresExtension.class)
public class DbKvsPostgresKeyValueServiceTest extends AbstractDbKvsKeyValueServiceTest {
    @RegisterExtension
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsPostgresExtension::createKvs);

    private static final Namespace TEST_NAMESPACE = Namespace.create("ns");
    private static final String TEST_LONG_TABLE_NAME =
            "ThisShouldAlwaysBeAVeryLongTableNameThatExceedsPostgresLengthLimit";
    private static final int TWO_UNDERSCORES = 2;

    public DbKvsPostgresKeyValueServiceTest() {
        super(TRM);
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        keyValueService.getAllTableNames().stream()
                .filter(table -> !table.getQualifiedName().equals("_metadata"))
                .forEach(keyValueService::dropTable);
    }

    @Test
    public void throwWhenCreatingDifferentLongTablesWithSameFirstCharactersUntilTheTableNameLimit() {
        String tableNameForFirstSixtyCharactersToBeSame = StringUtils.left(
                TEST_LONG_TABLE_NAME,
                PostgresDdlTable.ATLASDB_POSTGRES_TABLE_NAME_LIMIT
                        - TEST_NAMESPACE.getName().length()
                        - TWO_UNDERSCORES);
        assertThatThrownBy(() -> createTwoTablesWithSamePrefix(tableNameForFirstSixtyCharactersToBeSame))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void shouldNotThrowWhenCreatingDifferentLongTablesWithSameFirstCharactersUntilOneLessThanTableNameLimit() {
        String tableNameForFirstFiftyNineCharactersToBeSame = StringUtils.left(
                TEST_LONG_TABLE_NAME,
                PostgresDdlTable.ATLASDB_POSTGRES_TABLE_NAME_LIMIT
                        - TEST_NAMESPACE.getName().length()
                        - TWO_UNDERSCORES
                        - 1);
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

    @Test
    public void throwWhenCreatingDifferentLongTablesWithSameFirstCharactersAfterTheTableNameLimit() {
        assertThatThrownBy(() -> createTwoTablesWithSamePrefix(TEST_LONG_TABLE_NAME))
                .isInstanceOf(RuntimeException.class);
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
