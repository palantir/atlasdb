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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;

public class HeartbeatServiceIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatServiceIntegrationTest.class);

    private static final int heartbeatTimePeriodMillis = 100;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(HeartbeatServiceIntegrationTest.class)
            .with(new CassandraContainer());

    private HeartbeatService heartbeatService;
    private TracingQueryRunner queryRunner;
    private CassandraClientPool clientPool;
    private ConsistencyLevel writeConsistency;
    private UniqueSchemaMutationLockTable lockTable;
    private SchemaMutationLockTestTools lockTestTools;

    private final long lockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws TException {
        CassandraKeyValueServiceConfigManager simpleManager = CassandraKeyValueServiceConfigManager.createSimpleManager(
                CassandraContainer.KVS_CONFIG);
        queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());

        writeConsistency = ConsistencyLevel.EACH_QUORUM;
        clientPool = CassandraClientPoolImpl.create(simpleManager.getConfig());
        lockTable = new UniqueSchemaMutationLockTable(
                new SchemaMutationLockTables(clientPool, CassandraContainer.KVS_CONFIG),
                LockLeader.I_AM_THE_LOCK_LEADER);
        heartbeatService = new HeartbeatService(clientPool,
                                                queryRunner,
                                                heartbeatTimePeriodMillis,
                                                lockTable.getOnlyTable(),
                                                writeConsistency);
        lockTestTools = new SchemaMutationLockTestTools(clientPool, lockTable);
        lockTestTools.setLocksTableValue(lockId, 0);
    }

    @After
    public void cleanUp() throws TException {
        heartbeatService.stopBeating();
        lockTestTools.truncateLocksTable();
    }

    @Test
    public void testNormalStartStopBeatingSequence() throws TException, InterruptedException {
        assertThat(lockTestTools.readHeartbeatCountFromLocksTable(), is(0L));
        heartbeatService.startBeatingForLock(lockId);
        Thread.sleep(10 * heartbeatTimePeriodMillis);
        heartbeatService.stopBeating();
        assertThat(lockTestTools.readHeartbeatCountFromLocksTable(), is(not(0L)));
    }

    @Test
    public void testStartBeatingWithoutStopping() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(HeartbeatService.START_BEATING_ERR_MSG);

        heartbeatService.startBeatingForLock(lockId);

        // try starting another heartbeat without stopping an existing heartbeat
        heartbeatService.startBeatingForLock(lockId - 10);
    }

    @Test
    public void testSingleHeartbeat() throws TException {
        Heartbeat heartbeat = new Heartbeat(clientPool, queryRunner,
                lockTable.getOnlyTable(), writeConsistency, lockId);
        heartbeat.run();
        assertThat(lockTestTools.readHeartbeatCountFromLocksTable(), is(1L));
    }

    @Test
    public void testHeartbeatWithInvalidLock() throws TException {
        long invalidLockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);
        Heartbeat heartbeat = new Heartbeat(clientPool, queryRunner, lockTable.getOnlyTable(),
                writeConsistency, invalidLockId);
        heartbeat.run();
        // value should not be updated because an IllegalStateException will be thrown and caught
        assertThat(lockTestTools.readHeartbeatCountFromLocksTable(), is(0L));
    }
}
