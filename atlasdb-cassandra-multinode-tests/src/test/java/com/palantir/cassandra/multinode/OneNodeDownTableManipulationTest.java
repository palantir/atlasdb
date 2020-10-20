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
package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import org.junit.Test;

public class OneNodeDownTableManipulationTest extends AbstractDegradedClusterTest {
    private static final TableReference TABLE_TO_DROP = TableReference.createWithEmptyNamespace("table_to_drop");
    private static final TableReference TABLE_TO_DROP_2 = TableReference.createWithEmptyNamespace("table_to_drop_2");

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TABLE_TO_DROP, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TABLE_TO_DROP_2, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void canCreateTable() {
        TableReference tableToCreate = TableReference.createWithEmptyNamespace("new_table");
        getTestKvs().createTable(tableToCreate, AtlasDbConstants.GENERIC_TABLE_METADATA);

        assertThat(getTestKvs().getAllTableNames()).contains(tableToCreate);
        assertKvsReturnsGenericMetadata(tableToCreate);
        assertCassandraSchemaChanged();
    }

    @Test
    public void canCreateTables() {
        TableReference tableToCreate = TableReference.createWithEmptyNamespace("new_table2");
        getTestKvs().createTables(ImmutableMap.of(tableToCreate, AtlasDbConstants.GENERIC_TABLE_METADATA));

        assertThat(getTestKvs().getAllTableNames()).contains(tableToCreate);
        assertKvsReturnsGenericMetadata(tableToCreate);
        assertCassandraSchemaChanged();
    }

    @Test
    public void cannotDropTable() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().dropTable(TABLE_TO_DROP));
    }

    @Test
    public void cannotDropTables() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().dropTables(ImmutableSet.of(TABLE_TO_DROP_2)));
    }

    @Test
    public void truncateTableThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().truncateTable(TEST_TABLE));
    }

    @Test
    public void truncateTablesThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().truncateTables(ImmutableSet.of(TEST_TABLE)));
    }
}
