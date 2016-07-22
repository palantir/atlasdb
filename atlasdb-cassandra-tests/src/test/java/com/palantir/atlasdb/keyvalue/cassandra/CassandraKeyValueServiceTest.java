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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class CassandraKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {
    private KeyValueService keyValueService;
    private ExecutorService executorService;

    @Before
    public void setupKVS() {
        keyValueService = getKeyValueService();
        executorService = Executors.newFixedThreadPool(4);
    }

    @After
    public void cleanUp() {
        executorService.shutdown();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.CASSANDRA_KVS_CONFIG), CassandraTestSuite.LEADER_CONFIG);
    }

    @Override
    protected boolean reverseRangesSupported() {
        return false;
    }

    @Override
    @Ignore
    public void testGetRangeWithHistory() {
        //
    }

    @Override
    @Ignore
    public void testGetAllTableNames() {
        //
    }

    @Test
    public void testCreateTableCaseInsensitive() throws TException {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.tAbLe");
        TableReference table2 = TableReference.createFromFullyQualifiedName("ns.table");
        TableReference table3 = TableReference.createFromFullyQualifiedName("ns.TABle");
        keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(table2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(table3, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Set<TableReference> allTables = keyValueService.getAllTableNames();
        Preconditions.checkArgument(allTables.contains(table1));
        Preconditions.checkArgument(!allTables.contains(table2));
        Preconditions.checkArgument(!allTables.contains(table3));
    }

    @Test(timeout = 3000L)
    public void testNullLockLeaderDefaultsToFirstInLeaderList() {
        ImmutableCassandraKeyValueServiceConfig config = CassandraTestSuite.CASSANDRA_KVS_CONFIG.withKeyspace("nullLockLeader");
        CassandraKeyValueService kvs = createKVS(config);
    }

    @Test
    public void testCreateMultipleLockTables() throws TException, InterruptedException {
        String keyspace = "multipleLockTables";
        ImmutableCassandraKeyValueServiceConfig config = CassandraTestSuite.CASSANDRA_KVS_CONFIG.withKeyspace(keyspace);

        AtomicInteger timesThrown = new AtomicInteger(0);
        for (int numAttempts = 0; numAttempts < 10 && timesThrown.get() == 0; numAttempts++) {
            timesThrown = createMultipleLockTables(config);
        }

        System.out.println("Times thrown: " + timesThrown.get());
        assertThat("Expected to throw, but didn't :-(", timesThrown.get() > 0);
    }

    private AtomicInteger createMultipleLockTables(ImmutableCassandraKeyValueServiceConfig config) throws TException, InterruptedException {
        CassandraKeyValueServiceConfigManager configManager = CassandraKeyValueServiceConfigManager.createSimpleManager(config);
        CassandraTestTools.dropKeyspaceIfExists(config);

        // Create the keyspace before going through the loop - doing so within the loop causes the nodes to get out of sync and we don't catch the bug
        CassandraTestTools.createKeyspace(config);

        AtomicInteger timesThrown = new AtomicInteger(0);

        int threadCount = 3;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);
        ForkJoinPool threadPool = new ForkJoinPool(threadCount);

        threadPool.submit(() -> {
            IntStream.range(0, threadCount).parallel().forEach(i -> {
                try {
                    cyclicBarrier.await();
                    CassandraKeyValueService.create(configManager, CassandraTestSuite.LEADER_CONFIG);
                } catch (BrokenBarrierException | InterruptedException e) {
                    System.out.println("Something went wrong with the cyclic barrier. Exception thrown was: " + e);
                } catch (IllegalStateException e) {
                    timesThrown.incrementAndGet();
                }
            });
        });

        threadPool.awaitTermination(2, TimeUnit.SECONDS);

        CassandraTestTools.dropKeyspaceIfExists(configManager.getConfig());
        return timesThrown;
    }

    private CassandraKeyValueService createKVS(ImmutableCassandraKeyValueServiceConfig config) {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(config),
                CassandraTestSuite.LEADER_CONFIG);
    }
}
