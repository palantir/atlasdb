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
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraSchemaLockCleaner;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLockTables;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.common.exception.AtlasDbDependencyException;

public class OneNodeDownTableManipulationTest {
    private static final TableReference NEW_TABLE = TableReference.createWithEmptyNamespace("new_table");
    private static final TableReference NEW_TABLE2 = TableReference.createWithEmptyNamespace("new_table2");

    @Test
    public void canCreateTable() {
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).doesNotContain(NEW_TABLE);
        OneNodeDownTestSuite.kvs.createTable(NEW_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        // This documents and verifies the current behaviour, creating the table in spite of the exception
        // Seems to be inconsistent with the API
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).contains(NEW_TABLE);
    }

    @Test
    public void canCreateTables() {
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).doesNotContain(NEW_TABLE2);
        OneNodeDownTestSuite.kvs.createTables(ImmutableMap.of(NEW_TABLE2, AtlasDbConstants.GENERIC_TABLE_METADATA));

        // This documents and verifies the current behaviour, creating the table in spite of the exception
        // Seems to be inconsistent with the API
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).contains(NEW_TABLE2);
    }

    @Test
    public void dropTableThrows() {
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).contains(OneNodeDownTestSuite.TEST_TABLE_TO_DROP);
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.dropTable(OneNodeDownTestSuite.TEST_TABLE_TO_DROP))
                .isExactlyInstanceOf(AtlasDbDependencyException.class)
                .hasCauseInstanceOf(UncheckedExecutionException.class);
        // This documents and verifies the current behaviour, dropping the table in spite of the exception
        // Seems to be inconsistent with the API
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).doesNotContain(OneNodeDownTestSuite.TEST_TABLE_TO_DROP);
    }

    @Test
    public void dropTablesThrows() {
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).contains(OneNodeDownTestSuite.TEST_TABLE_TO_DROP_2);
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.dropTables(
                ImmutableSet.of(OneNodeDownTestSuite.TEST_TABLE_TO_DROP_2)))
                .isExactlyInstanceOf(AtlasDbDependencyException.class)
                .hasCauseInstanceOf(UncheckedExecutionException.class);
        // This documents and verifies the current behaviour, dropping the table in spite of the exception
        // Seems to be inconsistent with the API
        assertThat(OneNodeDownTestSuite.kvs.getAllTableNames())
                .doesNotContain(OneNodeDownTestSuite.TEST_TABLE_TO_DROP_2);
    }

    @Test
    public void canCompactInternally() {
        OneNodeDownTestSuite.kvs.compactInternally(OneNodeDownTestSuite.TEST_TABLE);
    }

    @Test
    public void canCleanUpSchemaMutationLockTablesState() throws Exception {
        ImmutableCassandraKeyValueServiceConfig config = OneNodeDownTestSuite.CONFIG;
        CassandraClientPool clientPool = OneNodeDownTestSuite.kvs.getClientPool();
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, config);
        TracingQueryRunner queryRunner = new TracingQueryRunner(LoggerFactory.getLogger(TracingQueryRunner.class),
                new TracingPrefsConfig());
        CassandraSchemaLockCleaner cleaner = CassandraSchemaLockCleaner.create(config, clientPool, lockTables,
                queryRunner);

        cleaner.cleanLocksState();
    }

    @Test
    public void truncateTableThrows() {
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.truncateTable(OneNodeDownTestSuite.TEST_TABLE))
                .isExactlyInstanceOf(InsufficientConsistencyException.class)
                .hasMessage("Truncating tables requires all Cassandra nodes to be up and available.");
    }

    @Test
    public void truncateTablesThrows() {
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.truncateTables(
                ImmutableSet.of(OneNodeDownTestSuite.TEST_TABLE)))
                .isExactlyInstanceOf(InsufficientConsistencyException.class)
                .hasMessage("Truncating tables requires all Cassandra nodes to be up and available.");
    }
}
