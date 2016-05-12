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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraDbLockTest {
    private static final long GLOBAL_DDL_LOCK_NEVER_ALLOCATED_VALUE = Long.MAX_VALUE - 1;
    private CassandraKeyValueService kvs;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    public static final TableReference BAD_TABLE = TableReference.createFromFullyQualifiedName("foo.b@r");
    public static final TableReference GOOD_TABLE = TableReference.createFromFullyQualifiedName("foo.bar");

    @Before
    public void setUp() {
        ImmutableCassandraKeyValueServiceConfig cassandraKvsConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(1000);
        kvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(cassandraKvsConfig));
        kvs.initializeFromFreshInstance();
        kvs.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void tearDown() {
        kvs.teardown();
    }

    @Test
    public void testLockAndUnlockWithoutContention() {
        long ourId = kvs.waitForSchemaMutationLock();
        kvs.schemaMutationUnlock(ourId);
    }

    @Test (expected = IllegalStateException.class)
    public void testBadUnlockFails() {
        kvs.schemaMutationUnlock(GLOBAL_DDL_LOCK_NEVER_ALLOCATED_VALUE);
    }

    @Test
    public void testOnlyOneLockCanBeLockedAtATime() throws InterruptedException, ExecutionException, TimeoutException {
        long id = kvs.waitForSchemaMutationLock();
        try {
            Future future = asyncRunner(() -> kvs.waitForSchemaMutationLock());
            exception.expect(TimeoutException.class);
            future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw e;
        } finally {
            kvs.schemaMutationUnlock(id);
        }
    }

    private Future asyncRunner(Runnable callable) {
        return executorService.submit(callable);
    }

    @Test
    public void testIdsAreRequestUnique() {
        long id = kvs.waitForSchemaMutationLock();
        kvs.schemaMutationUnlock(id);
        long newId = kvs.waitForSchemaMutationLock();
        kvs.schemaMutationUnlock(newId);
        Assert.assertNotEquals(id, newId);
    }

    @Test
    public void testUnlockIsSuccessful() throws InterruptedException, TimeoutException, ExecutionException {
        long id = kvs.waitForSchemaMutationLock();
        Future future = asyncRunner(() -> {
            long newId = kvs.waitForSchemaMutationLock();
            kvs.schemaMutationUnlock(newId);
        });
        Thread.sleep(100);
        Assert.assertFalse(future.isDone());
        kvs.schemaMutationUnlock(id);
        future.get(3, TimeUnit.SECONDS);
    }

    @Test (timeout = 10 * 1000)
    public void testTableCreationCanOccurAfterError() {
        try {
            kvs.createTable(BAD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        } catch (Exception e) {
            e.printStackTrace();
        }
        kvs.createTable(GOOD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.dropTable(GOOD_TABLE);
    }

    @Test
    public void testLocksTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        long id = kvs.waitForSchemaMutationLock();
        try {
            Future future = asyncRunner(() -> kvs.waitForSchemaMutationLock());
            exception.expect(ExecutionException.class);
            exception.expectMessage("We have timed out waiting on the current schema mutation lock holder.");
            exception.expectMessage("please contact support.");
            //exception.expectCause(isA(TimeoutException.class));
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw e;
        } finally {
            kvs.schemaMutationUnlock(id);
        }
    }
}
