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

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraSchemaLockCleaner;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLockTables;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;

public class TwoNodesDownCleanCassLockStateTest extends AbstractDegradedClusterTest {

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        try {
            kvs.createTable(
                    TableReference.createWithEmptyNamespace(SchemaMutationLockTables.LOCK_TABLE_PREFIX + "test"),
                    AtlasDbConstants.GENERIC_TABLE_METADATA);
        } catch (UncheckedExecutionException e) {
            // expected since we cause ourselves to throw, but the creation succeeds nonetheless
        }
    }

    @Test
    public void cleanUpSchemaMutationLockTablesStateThrows() {
        CassandraKeyValueServiceConfig config = TwoNodesDownTestSuite.getConfig(getClass());
        CassandraClientPool clientPool = getTestKvs().getClientPool();
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, config);
        TracingQueryRunner queryRunner = new TracingQueryRunner(LoggerFactory.getLogger(TracingQueryRunner.class),
                new TracingPrefsConfig());
        CassandraSchemaLockCleaner cleaner = CassandraSchemaLockCleaner.create(config, clientPool, lockTables,
                queryRunner);


        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(cleaner::cleanLocksState);
    }
}
