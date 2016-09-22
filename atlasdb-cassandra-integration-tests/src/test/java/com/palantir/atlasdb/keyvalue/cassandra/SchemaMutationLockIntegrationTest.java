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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.common.exception.PalantirRuntimeException;

@RunWith(Parameterized.class)
public class SchemaMutationLockIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SchemaMutationLockIntegrationTest.class);
    private static final SchemaMutationLock.Action DO_NOTHING = () -> {};

    private SchemaMutationLock schemaMutationLock;
    private HeartbeatService heartbeatService;
    private ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig;
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
        ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
        CassandraClientPool clientPool = new CassandraClientPool(simpleManager.getConfig());

        UniqueSchemaMutationLockTable lockTable = new UniqueSchemaMutationLockTable(
                new SchemaMutationLockTables(clientPool, quickTimeoutConfig), LockLeader.I_AM_THE_LOCK_LEADER);
        TracingQueryRunner queryRunner = new TracingQueryRunner(log, TracingPrefsConfig.create());
        heartbeatService = new HeartbeatService(clientPool, queryRunner,
                quickTimeoutConfig.heartbeatTimePeriodMillis(), lockTable.getOnlyTable(), writeConsistency);
        schemaMutationLock = new SchemaMutationLock(supportsCas, simpleManager, clientPool, queryRunner,
                writeConsistency, lockTable, heartbeatService);
    }

    @Test
    public void testLockAndUnlockWithoutContention() {
        schemaMutationLock.runWithLock(() -> {});
    }

    @Test
    public void doesNotPerformAnActionIfTheLockIsAlreadyHeld() {
        schemaMutationLock.runWithLock(() -> {
            Future getLockAgain = CassandraTestTools.async(
                    executorService,
                    () -> schemaMutationLock.runWithLock(DO_NOTHING));

            Thread.sleep(3*1000);

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

        schemaMutationLock.runWithLock(() -> { throw error; });
    }

    @Test
    public void canGrabLockWhenNoHeartbeat() throws InterruptedException, ExecutionException {
        // only run this test with cas
        Assume.assumeTrue(casEnabled);

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(instanceOf(IllegalStateException.class));
        expectedException.expectMessage("Another process cleared our schema mutation lock from underneath us.");

        Future initialLockHolder = CassandraTestTools.async(executorService, () ->
                schemaMutationLock.runWithLock(() -> {
                    // Wait for few heartbeats
                    Thread.sleep(quickTimeoutConfig.heartbeatTimePeriodMillis() * 2);

                    heartbeatService.stopBeating();

                    // Try grabbing lock with dead heartbeat
                    Future lockGrabber = CassandraTestTools.async(executorService,
                            () -> schemaMutationLock.runWithLock(DO_NOTHING));

                    // check if able to successfully acquire lock
                    try {
                        lockGrabber.get(5, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        assertThat("Schema lock could not be grabbed despite no heartbeat.",
                                lockGrabber.isDone(), is(true));
                    }
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
}
