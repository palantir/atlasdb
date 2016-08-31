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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraKeyValueServiceTableCreationTest {
    public static final TableReference GOOD_TABLE = TableReference.createFromFullyQualifiedName("foo.bar");
    public static final TableReference BAD_TABLE = TableReference.createFromFullyQualifiedName("foo.b@r");

    protected CassandraKeyValueService kvs;
    protected CassandraKeyValueService slowTimeoutKvs;

    @Before
    public void setUp() {
        ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(500);
        kvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(quickTimeoutConfig), CassandraTestSuite.LEADER_CONFIG);

        ImmutableCassandraKeyValueServiceConfig slowTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(6 * 1000);
        slowTimeoutKvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(slowTimeoutConfig), CassandraTestSuite.LEADER_CONFIG);

        kvs.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @After
    public void tearDown() {
        kvs.teardown();
    }

    @Test (timeout = 10 * 1000)
    public void testTableCreationCanOccurAfterError() {
        try {
            kvs.createTable(BAD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        } catch (Exception e) {
            // failure expected
        }
        kvs.createTable(GOOD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.dropTable(GOOD_TABLE);
    }

    @Test
    public void testCreatingMultipleTablesAtOnce() throws InterruptedException {
        int threadCount =  16;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ForkJoinPool threadPool = new ForkJoinPool(threadCount);

        threadPool.submit(() -> {
            IntStream.range(0, threadCount).parallel().forEach(i -> {
                try {
                    barrier.await();
                    slowTimeoutKvs.createTable(GOOD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
                } catch (BrokenBarrierException | InterruptedException e) {
                    // Do nothing
                }
            });
        });

        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(60, TimeUnit.SECONDS));

        slowTimeoutKvs.dropTable(GOOD_TABLE);
    }

    @Test
    public void describeVersionBehavesCorrectly() throws Exception {
        kvs.clientPool.runWithRetry(CassandraVerifier.underlyingCassandraClusterSupportsCASOperations);
    }
}
