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

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.LockLeader;

public class HeartbeatServiceIntegrationTest {
    private HeartbeatService heartbeatService;
    private ImmutableCassandraKeyValueServiceConfig quickHeartbeatConfig;
    private CassandraKeyValueServiceConfigManager simpleManager;
    private ConsistencyLevel writeConsistency;
    private CassandraClientPool clientPool;
    private UniqueSchemaMutationLockTable lockTable;
    private final int heartbeatTimePeriodMillis = 100;
    private final long lockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        quickHeartbeatConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG.withHeartbeatTimePeriodMillis(heartbeatTimePeriodMillis);
        simpleManager = CassandraKeyValueServiceConfigManager.createSimpleManager(quickHeartbeatConfig);
        writeConsistency = ConsistencyLevel.EACH_QUORUM;
        clientPool = new CassandraClientPool(simpleManager.getConfig());
        lockTable = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, quickHeartbeatConfig),
                                                      LockLeader.I_AM_THE_LOCK_LEADER);
        heartbeatService = new HeartbeatService(clientPool,
                                                quickHeartbeatConfig.heartbeatTimePeriodMillis(),
                                                lockTable.getOnlyTable().getQualifiedName(),
                                                writeConsistency);
        clientPool.runWithRetry(this::createLockEntry);
    }

    private CqlResult createLockEntry(Cassandra.Client client) throws TException {
        String lockValue= CassandraKeyValueServices.encodeAsHex(
                SchemaMutationLock.lockValueFromIdAndHeartbeat(lockId, 0).getBytes(StandardCharsets.UTF_8));
        String lockRowName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK.getBytes(StandardCharsets.UTF_8));
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
        Thread.sleep(2 * heartbeatTimePeriodMillis);
        heartbeatService.stopBeating();
        Assert.assertNotEquals(0, heartbeatService.getCurrentHeartbeatCount());
    }

    @Test
    public void testStopBeatingWithoutStarting() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(HeartbeatService.stopBeatingError);
        heartbeatService.stopBeating();
    }

    @Test
    public void testStartBeatingWithoutStopping() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(HeartbeatService.startBeatingError);

        heartbeatService.startBeatingForLock(lockId);

        // try starting another heartbeat without stopping an existing heartbeat
        heartbeatService.startBeatingForLock(lockId - 10);

        heartbeatService.stopBeating();
    }
}
