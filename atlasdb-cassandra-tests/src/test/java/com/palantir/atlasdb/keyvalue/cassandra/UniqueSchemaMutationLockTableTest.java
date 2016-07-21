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
import static org.hamcrest.Matchers.isA;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class UniqueSchemaMutationLockTableTest {
    private ExecutorService executorService;
    private CassandraClientPool clientPool;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setupKVS() throws TException, InterruptedException {
        clientPool = new CassandraClientPool(CassandraTestSuite.CASSANDRA_KVS_CONFIG);
        executorService = Executors.newFixedThreadPool(4);
    }

    @After
    public void cleanUp() throws TException, InterruptedException {
        executorService.shutdown();
    }

    @Test
    public void shouldReturnALockTableIfNoneExist() {
        UniqueSchemaMutationLockTable lockTables = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, CassandraTestSuite.CASSANDRA_KVS_CONFIG));

        assertThat(lockTables.getOnlyTable(), isA(TableReference.class));
    }

    @Test
    public void shouldReturnTheSameLockTableOnMultipleCalls() {
        UniqueSchemaMutationLockTable lockTables = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, CassandraTestSuite.CASSANDRA_KVS_CONFIG));

        assertThat(lockTables.getOnlyTable(), is(lockTables.getOnlyTable()));
    }

    @Test
    public void newLockTablesObjectsShouldUseAlreadyCreatedTables() {
        UniqueSchemaMutationLockTable lockTables1 = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, CassandraTestSuite.CASSANDRA_KVS_CONFIG));
        UniqueSchemaMutationLockTable lockTables2 = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, CassandraTestSuite.CASSANDRA_KVS_CONFIG));

        assertThat(lockTables1.getOnlyTable(), is(lockTables2.getOnlyTable()));
    }

    @Test
    public void ensureMultipleLockTablesCannotBeCreated() throws TException {
        String lockTable1 = createRandomLockTable();
        String lockTable2 = createRandomLockTable();

        UniqueSchemaMutationLockTable lockTables = new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, CassandraTestSuite.CASSANDRA_KVS_CONFIG));
        exception.expect(IllegalArgumentException.class);
        try {
            lockTables.getOnlyTable();
        } finally {
            deleteLockTable(lockTable1);
            deleteLockTable(lockTable2);
        }
    }

    @Test
    public void ensureMultipleLockTablesCannotBeCreatedByDifferentSchemaMutationLockTables() {
        List<UniqueSchemaMutationLockTable> lockTables = Lists.newArrayList();

        int threadCount = 3;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);
        ForkJoinPool threadPool = new ForkJoinPool(threadCount);

        threadPool.submit(() -> {
            IntStream.range(0, threadCount).parallel().forEach(i -> {
                try {
                    cyclicBarrier.await();
                    lockTables.set(i, new UniqueSchemaMutationLockTable(new SchemaMutationLockTables(clientPool, CassandraTestSuite.CASSANDRA_KVS_CONFIG)));
                    lockTables.get(i).getOnlyTable();
                } catch (BrokenBarrierException | InterruptedException e) {
                    System.out.println("Something went wrong with the cyclic barrier. Exception thrown was: " + e);
                } catch (IllegalStateException e) {
                    System.out.println(e.getMessage());
                }
            });
        });
    }

    private void deleteLockTable(String lockTable) throws TException {
        clientPool.run(client -> {
            try {
                client.system_drop_column_family(lockTable);
                System.out.println("Deleted table : " + lockTable);
                TableReference lockTableRef = TableReference.createWithEmptyNamespace(lockTable);
                CassandraKeyValueServices.waitForSchemaVersions(client, lockTableRef.getQualifiedName(), CassandraTestSuite.CASSANDRA_KVS_CONFIG.schemaMutationTimeoutMillis());
            } catch (TException e) {
                System.out.println("Failed to delete table : " + lockTable);
            }
            return null;
        });
    }
    
    private String createRandomLockTable() throws TException {
        String lockTableName = (HiddenTables.LOCK_TABLE_PREFIX + UUID.randomUUID()).replace('-','_');
        TableReference lockTable = TableReference.createWithEmptyNamespace(lockTableName);
        clientPool.run(client -> {
            try {
                createTableInternal(client, lockTable);
                System.out.println("Created table : " + lockTableName);
            } catch (TException e) {
                System.out.println("Failed to create table : " + lockTableName);
            }
            return null;
        });
        return lockTableName;
    }

    private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
        CfDef cf = CassandraConstants.getStandardCfDef(CassandraTestSuite.CASSANDRA_KVS_CONFIG.keyspace(), CassandraKeyValueService.internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), CassandraTestSuite.CASSANDRA_KVS_CONFIG.schemaMutationTimeoutMillis());
    }
}
