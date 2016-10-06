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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.common.exception.PalantirRuntimeException;

@RunWith(Parameterized.class)
public class SchemaMutationLockIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SchemaMutationLockIntegrationTest.class);
    private static final SchemaMutationLock.Action DO_NOTHING = () -> { };

    private SchemaMutationLock schemaMutationLock;
    private HeartbeatService heartbeatService;
    private ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig;
    private ConsistencyLevel writeConsistency;
    private CassandraClientPool clientPool;
    private UniqueSchemaMutationLockTable lockTable;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @SuppressWarnings({"WeakerAccess", "DefaultAnnotationParam"}) // test parameter
    @Parameterized.Parameter(value = 0)
    public boolean casEnabled;

    @SuppressWarnings("WeakerAccess") // test parameter
    @Parameterized.Parameter(value = 1)
    public String expectedTimeoutErrorMessage;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                { true, "We have timed out waiting on the current schema mutation lock holder." },
                { false, "unable to get a lock on Cassandra system schema mutations" }});
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        setUpWithCasSupportSetTo(casEnabled);
    }

    private void setUpWithCasSupportSetTo(boolean supportsCas) throws Exception {
        quickTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(500);
        CassandraKeyValueServiceConfigManager simpleManager =
                CassandraKeyValueServiceConfigManager.createSimpleManager(quickTimeoutConfig);
        TracingQueryRunner queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());
        writeConsistency = ConsistencyLevel.EACH_QUORUM;
        clientPool = new CassandraClientPool(simpleManager.getConfig());
        lockTable = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, quickTimeoutConfig),
                LockLeader.I_AM_THE_LOCK_LEADER);
        heartbeatService = new HeartbeatService(clientPool, queryRunner,
                quickTimeoutConfig.heartbeatTimePeriodMillis(), lockTable.getOnlyTable(), writeConsistency);
        schemaMutationLock = new SchemaMutationLock(supportsCas, simpleManager, clientPool, queryRunner,
                writeConsistency, lockTable, heartbeatService);
    }

    @Test
    public void testLockAndUnlockWithoutContention() {
        schemaMutationLock.runWithLock(DO_NOTHING);
    }

    @Test
    public void doesNotPerformAnActionIfTheLockIsAlreadyHeld() {
        schemaMutationLock.runWithLock(() -> {
            Future getLockAgain = CassandraTestTools.async(
                    executorService,
                    () -> schemaMutationLock.runWithLock(DO_NOTHING));

            Thread.sleep(3 * 1000);

            CassandraTestTools.assertThatFutureDidNotSucceedYet(getLockAgain);
        });
    }

    @Test(timeout = 10 * 1000)
    public void canRunAnotherActionOnceTheFirstHasBeenCompleted() {
        AtomicInteger counter = new AtomicInteger();
        SchemaMutationLock.Action increment = counter::incrementAndGet;

        schemaMutationLock.runWithLock(increment);
        schemaMutationLock.runWithLock(increment);

        assertThat(counter.get(), is(2));
    }

    @Test
    public void shouldWrapCheckedExceptionsInARuntimeException() {
        Exception error = new Exception();

        expectedException.expect(PalantirRuntimeException.class);
        expectedException.expectCause(is(error));

        schemaMutationLock.runWithLock(() -> {
            throw error;
        });
    }

    @Test
    public void testExceptionWithDeadHeartbeat() throws InterruptedException, ExecutionException {
        // only run this test with cas
        Assume.assumeTrue(casEnabled);

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(instanceOf(RuntimeException.class));
        expectedException.expectMessage("The current lock holder has failed to update its heartbeat.");

        Future initialLockHolder = CassandraTestTools.async(executorService, () ->
                schemaMutationLock.runWithLock(() -> {
                    // Wait for few heartbeats
                    Thread.sleep(quickTimeoutConfig.heartbeatTimePeriodMillis() * 2);

                    heartbeatService.stopBeating();

                    // Try acquiring lock with dead heartbeat
                    Future lockGrabber = CassandraTestTools.async(executorService,
                            () -> schemaMutationLock.runWithLock(DO_NOTHING));

                    // verify that lock is not acquired
                    lockGrabber.get();
                    assertThat("Schema lock was grabbed with dead heartbeat.",
                            lockGrabber.isDone(), is(false));
                }));
        initialLockHolder.get();
    }

    @Test
    public void testLocksTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        schemaMutationLock.runWithLock(() -> {
            expectedException.expect(PalantirRuntimeException.class);
            expectedException.expectMessage(expectedTimeoutErrorMessage);

            Future async = CassandraTestTools.async(
                    executorService,
                    () -> schemaMutationLock.runWithLock(DO_NOTHING));
            async.get(10, TimeUnit.SECONDS);
        });
    }

    @Test
    public void testNonHeartbeatClearedLockPostMigration() throws TException {
        // only run this test with cas
        Assume.assumeTrue(casEnabled);
        setUpWithNonHeartbeatClearedLock();

        schemaMutationLock.runWithLock(DO_NOTHING);
    }

    private void setUpWithNonHeartbeatClearedLock() throws TException {
        clientPool.runWithRetry(this::createNonHeartbeatClearedLockEntry);
    }

    private CqlResult createNonHeartbeatClearedLockEntry(Cassandra.Client client) throws TException {
        String lockValue = CassandraKeyValueServices.encodeAsHex(Longs.toByteArray(Long.MAX_VALUE));
        String lockRowName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME.getBytes(StandardCharsets.UTF_8));
        String lockColName = CassandraKeyValueServices.encodeAsHex(
                CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes(StandardCharsets.UTF_8));
        String createCql = String.format(
                "UPDATE \"%s\" SET value = %s WHERE key = %s AND column1 = %s AND column2 = -1;",
                lockTable.getOnlyTable().getQualifiedName(),
                lockValue,
                lockRowName,
                lockColName);
        ByteBuffer queryBuffer = ByteBuffer.wrap(createCql.getBytes(StandardCharsets.UTF_8));
        return client.execute_cql3_query(queryBuffer, Compression.NONE, writeConsistency);
    }
}
