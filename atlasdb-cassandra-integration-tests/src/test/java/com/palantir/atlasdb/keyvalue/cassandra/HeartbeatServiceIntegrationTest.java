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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;

public class HeartbeatServiceIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SchemaMutationLockIntegrationTest.class);

    private HeartbeatService heartbeatService;
    private TracingQueryRunner queryRunner;
    private CassandraClientPool clientPool;
    private ConsistencyLevel writeConsistency;
    private UniqueSchemaMutationLockTable lockTable;

    private final int heartbeatTimePeriodMillis = 100;
    private final long lockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        ImmutableCassandraKeyValueServiceConfig quickHeartbeatConfig = CassandraTestSuite.getKvsConfig()
                .withHeartbeatTimePeriodMillis(heartbeatTimePeriodMillis);
        CassandraKeyValueServiceConfigManager simpleManager = CassandraKeyValueServiceConfigManager.createSimpleManager(
                quickHeartbeatConfig);
        queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());

        writeConsistency= ConsistencyLevel.EACH_QUORUM;
        clientPool = new CassandraClientPool(simpleManager.getConfig());
        lockTable = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, quickHeartbeatConfig),
                                                      LockLeader.I_AM_THE_LOCK_LEADER);
        heartbeatService = new HeartbeatService(clientPool,
                                                queryRunner,
                                                quickHeartbeatConfig.heartbeatTimePeriodMillis(),
                                                lockTable.getOnlyTable(),
                                                writeConsistency);
        clientPool.runWithRetry(this::createLockEntry);
    }

    private CqlResult createLockEntry(Cassandra.Client client) throws TException {
        String lockValue= CassandraKeyValueServices.encodeAsHex(
                SchemaMutationLock.lockValueFromIdAndHeartbeat(lockId, 0).getBytes(StandardCharsets.UTF_8));
        String lockRowName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME.getBytes(StandardCharsets.UTF_8));
        String lockColName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes(StandardCharsets.UTF_8));
        String createCql = String.format(
                "UPDATE \"%s\" SET value = %s WHERE key = %s AND column1 = %s AND column2 = -1;",
                lockTable.getOnlyTable().getQualifiedName(), lockValue, lockRowName, lockColName);
        ByteBuffer queryBuffer = ByteBuffer.wrap(createCql.getBytes(StandardCharsets.UTF_8));
        return client.execute_cql3_query(queryBuffer, Compression.NONE, writeConsistency);
    }



    @After
    public void cleanUp() throws Exception {
        heartbeatService.stopBeating();
        clientPool.runWithRetry(this::truncateLocks);
    }

    private CqlResult truncateLocks(Cassandra.Client client) throws TException {
        String truncateCql = String.format("TRUNCATE \"%s\";", lockTable.getOnlyTable().getQualifiedName());
        ByteBuffer queryBuffer = ByteBuffer.wrap(truncateCql.getBytes(StandardCharsets.UTF_8));
        return client.execute_cql3_query(queryBuffer, Compression.NONE, writeConsistency);
    }

    @Test
    public void testNormalStartStopBeatingSequence() throws InterruptedException {
        Assert.assertEquals(0, heartbeatService.getCurrentHeartbeatCount());
        heartbeatService.startBeatingForLock(lockId);
        Thread.sleep(10 * heartbeatTimePeriodMillis);
        heartbeatService.stopBeating();
        Assert.assertNotEquals(0, heartbeatService.getCurrentHeartbeatCount());
    }

    @Test
    public void testStartBeatingWithoutStopping() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(HeartbeatService.startBeatingErr);

        heartbeatService.startBeatingForLock(lockId);

        // try starting another heartbeat without stopping an existing heartbeat
        heartbeatService.startBeatingForLock(lockId - 10);
    }

    @Test
    public void testSingleHeartbeat() {
        AtomicInteger heartbeatCount = new AtomicInteger(0);
        Heartbeat heartbeat = new Heartbeat(clientPool, queryRunner, heartbeatCount,
                lockTable.getOnlyTable(), writeConsistency, lockId);
        heartbeat.run();
        Assert.assertEquals(1, heartbeatCount.get());
    }

    @Test
    public void testHeartbeatWithInvalidLock() {
        long invalidLockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);
        AtomicInteger heartbeatCount = new AtomicInteger(0);
        Heartbeat heartbeat = new Heartbeat(clientPool, queryRunner, heartbeatCount,
                lockTable.getOnlyTable(), writeConsistency, invalidLockId);
        heartbeat.run();
        // value should not be updated because an IllegalStateException will be thrown and caught
        Assert.assertEquals(0, heartbeatCount.get());
    }
}
