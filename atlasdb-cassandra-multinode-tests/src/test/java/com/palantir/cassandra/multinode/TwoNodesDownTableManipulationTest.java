/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.LoggerFactory;

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

public class TwoNodesDownTableManipulationTest extends AbstractDegradedClusterTest {

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void createTableThrowsAndDoesNotChangeCassandraSchema() throws TException {
        TableReference tableToCreate = TableReference.createWithEmptyNamespace("new_table");
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().createTable(tableToCreate, AtlasDbConstants.GENERIC_TABLE_METADATA));

    }

    @Test
    public void dropTableThrowsAndDoesNotChangeCassandraSchema() throws TException {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().dropTable(TEST_TABLE));
    }

    @Test
    public void dropTablesThrowsAndDoesNotChangeCassandraSchema() throws TException {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().dropTables(ImmutableSet.of(TEST_TABLE)));
    }

    @Test
    public void cleanUpSchemaMutationLockTablesStateThrowsAndDoesNotChangeCassandraSchema() throws TException {
        CassandraKeyValueServiceConfig config = OneNodeDownTestSuite.getConfig(getClass());
        CassandraClientPool clientPool = getTestKvs().getClientPool();
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, config);
        TracingQueryRunner queryRunner = new TracingQueryRunner(LoggerFactory.getLogger(TracingQueryRunner.class),
                new TracingPrefsConfig());
        CassandraSchemaLockCleaner cleaner = CassandraSchemaLockCleaner.create(config, clientPool, lockTables,
                queryRunner);

        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(cleaner::cleanLocksState);
    }

    @Test
    public void truncateTableThrowsAndDoesNotChangeCassandraSchema() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().truncateTable(TEST_TABLE));

    }

    @Test
    public void truncateTablesThrowsAndDoesNotChangeCassandraSchema() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().truncateTables(ImmutableSet.of(TEST_TABLE)));
    }
}
