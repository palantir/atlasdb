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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SchemaMutationLockTablesTest {
    private ExecutorService executorService;
    CassandraKeyValueServiceConfigManager configManager = CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.CASSANDRA_KVS_CONFIG);
    private CassandraClientPool clientPool = new CassandraClientPool(CassandraTestSuite.CASSANDRA_KVS_CONFIG);

    @Before
    public void setupKVS() {
        executorService = Executors.newFixedThreadPool(4);
    }

    @After
    public void cleanUp() {
        executorService.shutdown();
    }

    @Test
    public void shouldReturnALockTableIfNoneExist() {
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, configManager);

        assertThat(lockTables.getOnlyTable(), isA(TableReference.class));
    }

    @Test
    public void shouldReturnTheSameLockTableOnMultipleCalls() {
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, configManager);

        assertThat(lockTables.getOnlyTable(), is(lockTables.getOnlyTable()));
    }
}
