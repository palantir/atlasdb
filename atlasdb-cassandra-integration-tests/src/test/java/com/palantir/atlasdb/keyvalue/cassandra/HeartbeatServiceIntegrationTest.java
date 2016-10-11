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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;

public class HeartbeatServiceIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SchemaMutationLockIntegrationTest.class);

    private static final int heartbeatTimePeriodMillis = 100;

    private HeartbeatService heartbeatService;
    private TracingQueryRunner queryRunner;
    private CassandraClientPool clientPool;
    private ConsistencyLevel writeConsistency;
    private UniqueSchemaMutationLockTable lockTable;

    private final long lockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws TException {
        CassandraKeyValueServiceConfigManager simpleManager = CassandraKeyValueServiceConfigManager.createSimpleManager(
                CassandraTestSuite.cassandraKvsConfig);
        queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());

        writeConsistency = ConsistencyLevel.EACH_QUORUM;
        clientPool = new CassandraClientPool(simpleManager.getConfig());
        lockTable = new UniqueSchemaMutationLockTable(
                new SchemaMutationLockTables(clientPool, CassandraTestSuite.cassandraKvsConfig),
                LockLeader.I_AM_THE_LOCK_LEADER);
        heartbeatService = new HeartbeatService(clientPool,
                                                queryRunner,
                                                heartbeatTimePeriodMillis,
                                                lockTable.getOnlyTable(),
                                                writeConsistency);
        clientPool.runWithRetry(this::createLockEntry);
    }

    @After
    public void cleanUp() throws TException {
        heartbeatService.stopBeating();
        clientPool.runWithRetry(this::truncateLocks);
    }

    private CqlResult truncateLocks(Cassandra.Client client) throws TException {
        String truncateCql = String.format("TRUNCATE \"%s\";", lockTable.getOnlyTable().getQualifiedName());
        ByteBuffer queryBuffer = ByteBuffer.wrap(truncateCql.getBytes(StandardCharsets.UTF_8));
        return client.execute_cql3_query(queryBuffer, Compression.NONE, writeConsistency);
    }

    @Test
    public void testNormalStartStopBeatingSequence() throws TException, InterruptedException {
        assertEquals(0, getCurrentHeartbeat());
        heartbeatService.startBeatingForLock(lockId);
        Thread.sleep(10 * heartbeatTimePeriodMillis);
        heartbeatService.stopBeating();
        assertNotEquals(0, getCurrentHeartbeat());
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
        assertEquals(1, getCurrentHeartbeat());
    }

    @Test
    public void testHeartbeatWithInvalidLock() throws TException {
        long invalidLockId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);
        Heartbeat heartbeat = new Heartbeat(clientPool, queryRunner, lockTable.getOnlyTable(),
                writeConsistency, invalidLockId);
        heartbeat.run();
        // value should not be updated because an IllegalStateException will be thrown and caught
        assertEquals(0, getCurrentHeartbeat());
    }

    private CqlResult createLockEntry(Cassandra.Client client) throws TException {
        String lockValue = CassandraKeyValueServices.encodeAsHex(
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

    private long getCurrentHeartbeat() throws TException {
        List<CqlRow> resultBeforeHeartbeat = clientPool.runWithRetry(this::readLockEntry).getRows();
        assertEquals(1, resultBeforeHeartbeat.size());
        List<Column> resultColumnsBeforeHeartbeat = resultBeforeHeartbeat.get(0).getColumns();
        assertEquals(1, resultColumnsBeforeHeartbeat.size());
        return SchemaMutationLock.getHeartbeatCountFromColumn(resultColumnsBeforeHeartbeat.get(0));
    }

    private CqlResult readLockEntry(Cassandra.Client client) throws TException {
        String lockRowName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME.getBytes(StandardCharsets.UTF_8));
        String lockColName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes(StandardCharsets.UTF_8));
        String createCql = String.format(
                "SELECT \"value\" FROM \"%s\" WHERE key = %s AND column1 = %s AND column2 = -1;",
                lockTable.getOnlyTable().getQualifiedName(), lockRowName, lockColName);
        ByteBuffer queryBuffer = ByteBuffer.wrap(createCql.getBytes(StandardCharsets.UTF_8));
        return client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.LOCAL_QUORUM);
    }
}
