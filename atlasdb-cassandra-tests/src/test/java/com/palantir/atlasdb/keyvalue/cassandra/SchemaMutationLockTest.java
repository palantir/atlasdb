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
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.exception.PalantirRuntimeException;

@RunWith(Parameterized.class)
public class SchemaMutationLockTest {
    public static final SchemaMutationLock.Action DO_NOTHING = () -> {};
    protected SchemaMutationLock schemaMutationLock;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @Parameterized.Parameter(value = 0)
    public boolean casEnabled;

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

    protected void setUpWithCasSupportSetTo(boolean supportsCas) throws Exception {
        ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(500);
        CassandraKeyValueServiceConfigManager simpleManager = CassandraKeyValueServiceConfigManager.createSimpleManager(quickTimeoutConfig);
        ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
        CassandraClientPool clientPool = new CassandraClientPool(simpleManager.getConfig());
        TableReference locks = TableReference.createWithEmptyNamespace("_locks");
        createTableIfNotExists(clientPool, quickTimeoutConfig.keyspace(), locks);

        UniqueSchemaMutationLockTable lockTable = UniqueSchemaMutationLockTable.create(new SchemaMutationLockTables(clientPool, quickTimeoutConfig));
        schemaMutationLock = new SchemaMutationLock(supportsCas, simpleManager, clientPool, writeConsistency, lockTable);
    }

    private void createTableIfNotExists(CassandraClientPool clientPool, String keyspace, TableReference table) throws Exception {
        clientPool.run((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
            String internalTableName = AbstractKeyValueService.internalTableName(table);
            if (tableDoesNotExist(client, keyspace, internalTableName)) {
                CfDef cf = CassandraConstants.getStandardCfDef(
                        keyspace,
                        internalTableName);
                client.system_add_column_family(cf);
            }
            return null;
        });
    }

    private boolean tableDoesNotExist(Cassandra.Client client, String keyspace, String internalTableName) throws TException {
        return client.describe_keyspace(keyspace).cf_defs.stream().noneMatch(cf1 -> cf1.getName().equalsIgnoreCase(internalTableName));
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
