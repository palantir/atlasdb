package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;

public abstract class SchemaMutationLockTest {
    public static final SchemaMutationLock.Action DO_NOTHING = () -> {
    };
    protected SchemaMutationLock schemaMutationLock;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

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

    @Test
    public void testUnlockIsSuccessful() throws InterruptedException, TimeoutException, ExecutionException {
        long id = schemaMutationLock.waitForSchemaMutationLock();
        Future future = async(() -> {
            long newId = schemaMutationLock.waitForSchemaMutationLock();
            schemaMutationLock.schemaMutationUnlock(newId);
        });
        Thread.sleep(100);
        Assert.assertFalse(future.isDone());
        schemaMutationLock.schemaMutationUnlock(id);
        future.get(3, TimeUnit.SECONDS);
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
