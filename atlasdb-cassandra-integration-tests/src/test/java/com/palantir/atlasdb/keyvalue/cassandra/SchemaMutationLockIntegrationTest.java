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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.common.exception.PalantirRuntimeException;

@RunWith(Parameterized.class)
public class SchemaMutationLockIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SchemaMutationLockIntegrationTest.class);
    private static final SchemaMutationLock.Action DO_NOTHING = () -> { };

    @ClassRule
    public static final Containers CONTAINERS = new Containers(SchemaMutationLockIntegrationTest.class)
            .with(new CassandraContainer());

    private SchemaMutationLock schemaMutationLock;
    private SchemaMutationLock secondSchemaMutationLock;
    private HeartbeatService heartbeatService;
    private ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig;
    private ConsistencyLevel writeConsistency;
    private CassandraClientPool clientPool;
    private UniqueSchemaMutationLockTable lockTable;
    private SchemaMutationLockTestTools lockTestTools;
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
        quickTimeoutConfig = ImmutableCassandraKeyValueServiceConfig.copyOf(CassandraContainer.KVS_CONFIG)
                .withSchemaMutationTimeoutMillis(500);
        TracingQueryRunner queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());
        writeConsistency = ConsistencyLevel.EACH_QUORUM;
        clientPool = CassandraClientPoolImpl.create(quickTimeoutConfig);
        lockTable = new UniqueSchemaMutationLockTable(
                new SchemaMutationLockTables(clientPool, quickTimeoutConfig),
                LockLeader.I_AM_THE_LOCK_LEADER);
        heartbeatService = new HeartbeatService(
                clientPool,
                queryRunner,
                HeartbeatService.DEFAULT_HEARTBEAT_TIME_PERIOD_MILLIS,
                lockTable.getOnlyTable(),
                writeConsistency);
        schemaMutationLock = getSchemaMutationLock(supportsCas, quickTimeoutConfig, queryRunner,
                SchemaMutationLock.DEFAULT_DEAD_HEARTBEAT_TIMEOUT_THRESHOLD_MILLIS);
        secondSchemaMutationLock = getSchemaMutationLock(supportsCas, quickTimeoutConfig, queryRunner,
                SchemaMutationLock.DEFAULT_DEAD_HEARTBEAT_TIMEOUT_THRESHOLD_MILLIS);
        lockTestTools = new SchemaMutationLockTestTools(clientPool, lockTable);
    }

    @Test
    public void testLockAndUnlockWithoutContention() {
        schemaMutationLock.runWithLock(DO_NOTHING);
    }

    @Test
    public void doesNotPerformAnActionIfTheLockIsAlreadyHeld() throws InterruptedException {
        Assume.assumeTrue(casEnabled);

        Semaphore blockingLock = blockSchemaMutationLock();

        Future getLockAgain = CassandraTestTools.async(
                executorService,
                () -> secondSchemaMutationLock.runWithLock(DO_NOTHING));

        Thread.sleep(3 * 1000);

        CassandraTestTools.assertThatFutureDidNotSucceedYet(getLockAgain);
        blockingLock.release();
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
        Assume.assumeTrue(casEnabled);

        Semaphore blockingLock = blockSchemaMutationLockAndKillHeartbeat();

        SchemaMutationLock quickHeartbeatTimeoutLock = createQuickHeartbeatTimeoutLock();

        try {
            quickHeartbeatTimeoutLock.runWithLock(DO_NOTHING);
            fail("Should have failed to acquire the lock");
        } catch (Exception exception) {
            assertTrue(exception instanceof RuntimeException);
            assertThat(exception.getMessage(),
                    containsString("The current lock holder has failed to update its heartbeat."));
        } finally {
            blockingLock.release();
        }
    }

    @Test
    public void runWithLockShouldTimeoutIfLockIsTaken() throws InterruptedException, ExecutionException,
            TimeoutException {
        Assume.assumeTrue(casEnabled);

        Semaphore blockingLock = blockSchemaMutationLock();

        try {
            secondSchemaMutationLock.runWithLock(() ->
                    fail("The schema mutation lock should have been acquired by the first thread"));
            fail("Should have thrown an exception");
        } catch (Exception exception) {
            assertTrue(exception instanceof PalantirRuntimeException);
            assertThat(exception.getMessage(), containsString(expectedTimeoutErrorMessage));
        } finally {
            blockingLock.release();
        }
    }

    @Test
    public void testCleanLockState() throws TException {
        Assume.assumeTrue(casEnabled);
        lockTestTools.setLocksTableValue(123456789L, 0);
        schemaMutationLock.cleanLockState();
        assertThat(lockTestTools.readLockIdFromLocksTable(), is(SchemaMutationLock.GLOBAL_DDL_LOCK_CLEARED_ID));
    }

    @Test
    public void testLegacyClearedLockPostMigration() throws TException {
        Assume.assumeTrue(casEnabled);

        lockTestTools.setLegacyClearedLocksTableValue();

        schemaMutationLock.runWithLock(DO_NOTHING);
    }

    private Semaphore blockSchemaMutationLock() throws InterruptedException {
        Semaphore blockingLock = new Semaphore(0);
        Semaphore outerLock = new Semaphore(0);
        executorService.submit(() -> schemaMutationLock.runWithLock(() -> {
            outerLock.release();
            blockingLock.acquire();
        }));

        outerLock.acquire();
        return blockingLock;
    }

    private Semaphore blockSchemaMutationLockAndKillHeartbeat() throws InterruptedException {
        Semaphore blockingLock = new Semaphore(0);
        Semaphore outerLock = new Semaphore(0);

        executorService.submit(() -> schemaMutationLock.runWithLock(() -> {
            Thread.sleep(HeartbeatService.DEFAULT_HEARTBEAT_TIME_PERIOD_MILLIS * 2);
            heartbeatService.stopBeating();

            outerLock.release();
            blockingLock.acquire();
        }));

        outerLock.acquire();

        return blockingLock;
    }

    private SchemaMutationLock createQuickHeartbeatTimeoutLock() {
        TracingQueryRunner queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());
        return getSchemaMutationLock(true, CassandraContainer.KVS_CONFIG, queryRunner, 2000);
    }

    private SchemaMutationLock getSchemaMutationLock(boolean supportsCas,
            CassandraKeyValueServiceConfig config, TracingQueryRunner queryRunner,
            int defaultDeadHeartbeatTimeoutThresholdMillis) {
        return new SchemaMutationLock(
                supportsCas,
                config,
                clientPool,
                queryRunner,
                writeConsistency,
                lockTable,
                heartbeatService,
                defaultDeadHeartbeatTimeoutThresholdMillis);
    }
}
