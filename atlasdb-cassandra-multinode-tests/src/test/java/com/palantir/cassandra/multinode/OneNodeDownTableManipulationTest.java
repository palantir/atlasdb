/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraSchemaLockCleaner;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLockTables;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.common.exception.AtlasDbDependencyException;

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
    }

    @Test
    public void canCreateTables() {
        TableReference tableToCreate = TableReference.createWithEmptyNamespace("new_table2");
        getTestKvs().createTables(ImmutableMap.of(tableToCreate, AtlasDbConstants.GENERIC_TABLE_METADATA));

        assertThat(getTestKvs().getAllTableNames()).contains(tableToCreate);
    }

    @Test
    public void dropTableThrows() {
        assertThatThrownBy(() -> getTestKvs().dropTable(TABLE_TO_DROP))
                .isInstanceOf(AtlasDbDependencyException.class);
        // This documents and verifies the current behaviour, dropping the table in spite of the exception
        // Seems to be inconsistent with the API
        assertThat(getTestKvs().getAllTableNames()).doesNotContain(TABLE_TO_DROP);
    }

    @Test
    public void dropTablesThrows() {
        assertThatThrownBy(() -> getTestKvs().dropTables(ImmutableSet.of(TABLE_TO_DROP_2)))
                .isInstanceOf(AtlasDbDependencyException.class);
        // This documents and verifies the current behaviour, dropping the table in spite of the exception
        // Seems to be inconsistent with the API
        assertThat(getTestKvs().getAllTableNames()).doesNotContain(TABLE_TO_DROP_2);
    }

    @Test
    public void canCleanUpSchemaMutationLockTablesState() throws Exception {
        CassandraKeyValueServiceConfig config = OneNodeDownTestSuite.getConfig(getClass());
        CassandraClientPool clientPool = getTestKvs().getClientPool();
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, config);
        TracingQueryRunner queryRunner = new TracingQueryRunner(LoggerFactory.getLogger(TracingQueryRunner.class),
                new TracingPrefsConfig());
        CassandraSchemaLockCleaner cleaner = CassandraSchemaLockCleaner.create(config, clientPool, lockTables,
                queryRunner);

        cleaner.cleanLocksState();
    }

    @Test
    public void truncateTableThrows() {
        assertThatThrownBy(() -> getTestKvs().truncateTable(TEST_TABLE))
                .isInstanceOf(AtlasDbDependencyException.class);

    }

    @Test
    public void truncateTablesThrows() {
        assertThatThrownBy(() -> getTestKvs().truncateTables(ImmutableSet.of(TEST_TABLE)))
                .isInstanceOf(AtlasDbDependencyException.class);
    }
}
