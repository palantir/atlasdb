package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.common.exception.PalantirRuntimeException;

public abstract class SchemaMutationLockTest {
    public static final SchemaMutationLock.Action DO_NOTHING = () -> {};
    protected SchemaMutationLock schemaMutationLock;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        setUpWithCasSupportSetTo(true);
    }

    protected void setUpWithCasSupportSetTo(boolean supportsCas) {
        ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(500);
        CassandraKeyValueServiceConfigManager simpleManager = CassandraKeyValueServiceConfigManager.createSimpleManager(quickTimeoutConfig);
        ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
        CassandraClientPool clientPool = new CassandraClientPool(simpleManager.getConfig());

        schemaMutationLock = new SchemaMutationLock(supportsCas, simpleManager, clientPool, writeConsistency);
    }

    @Test
    public void testLockAndUnlockWithoutContention() {
        schemaMutationLock.runWithLock(() -> {});
    }

    @Test
    public void doesNotPerformAnActionIfTheLockIsAlreadyHeld() {
        schemaMutationLock.runWithLock(() -> {
            Future getLockAgain = async(() -> schemaMutationLock.runWithLock(DO_NOTHING));

            Thread.sleep(3*1000);

            assertThatFutureDidNotSucceedYet(getLockAgain);
        });
    }

    @Test(timeout = 10 * 1000)
    public void canRunAnotherActionOnceTheFirstHasBeenCompleted() {
        AtomicInteger counter = new AtomicInteger();
        SchemaMutationLock.Action increment = () -> counter.incrementAndGet();

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

    protected Future async(Runnable callable) {
        return executorService.submit(callable);
    }

    private void assertThatFutureDidNotSucceedYet(Future future) throws InterruptedException {
        if (future.isDone()) {
            try {
                future.get();
                throw new AssertionError("Future task should have failed but finished successfully");
            } catch (ExecutionException e) {
                // if execution is done, we expect it to have failed
            }
        }
    }
}
