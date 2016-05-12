/**
 * Copyright 2015 Palantir Technologies
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

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;

public class CassandraDbLockTest {
    private CassandraKeyValueService kv;

    @Before
    public void setUp() {
        kv = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(
                        ImmutableCassandraKeyValueServiceConfig.builder()
                                .addServers(new InetSocketAddress("localhost", 9160))
                                .poolSize(20)
                                .keyspace("atlasdb")
                                .ssl(false)
                                .replicationFactor(1)
                                .mutationBatchCount(10000)
                                .mutationBatchSizeBytes(10000000)
                                .fetchBatchCount(1000)
                                .safetyDisabled(true)
                                .autoRefreshNodes(true)
                                .build()));
        kv.initializeFromFreshInstance();
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @After
    public void tearDown() {
        kv.teardown();
    }

    @Test
    public void testLockAndUnlockWithoutContention() {
        long ourId = kv.waitForSchemaMutationLock();
        kv.schemaMutationUnlock(ourId);
    }

    @Test (expected = IllegalStateException.class)
    public void testBadUnlockFails() {
        kv.schemaMutationUnlock(CassandraConstants.GLOBAL_DDL_LOCK_NEVER_ALLOCATED_VALUE);
    }
}
