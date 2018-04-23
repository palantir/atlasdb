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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SchemaMutationLockTablesIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(SchemaMutationLockTablesIntegrationTest.class)
            .with(new CassandraContainer());

    private SchemaMutationLockTables lockTables;
    private CassandraKeyValueServiceConfig config;
    private CassandraClientPoolImpl clientPool;

    @Before
    public void setupKvs() throws TException, InterruptedException {
        config = ImmutableCassandraKeyValueServiceConfig.copyOf(CassandraContainer.KVS_CONFIG)
                .withKeyspace(UUID.randomUUID().toString().replace('-', '_')); // Hyphens not allowed in C* schema
        clientPool = CassandraClientPoolImpl.createImplForTest(config,
                CassandraClientPoolImpl.StartupChecks.RUN,
                new Blacklist(config));
        clientPool.runOneTimeStartupChecks();
        lockTables = new SchemaMutationLockTables(clientPool, config);
    }

    @Test
    public void startsWithNoTables() throws TException {
        assertThat(lockTables.getAllLockTables(), is(empty()));
    }

    @Test
    public void tableShouldExistAfterCreation() throws Exception {
        lockTables.createLockTable();
        assertThat(lockTables.getAllLockTables(), hasSize(1));
    }

    @Test
    public void secondTableCreationSucceedsButOnlyOneTableExistsAfterwards() throws Exception {
        lockTables.createLockTable();

        lockTables.createLockTable();

        assertThat(lockTables.getAllLockTables(), hasSize(1));
    }

    @Test
    public void multipleSchemaMutationLockTablesObjectsShouldReturnSameLockTables() throws Exception {
        SchemaMutationLockTables lockTables2 = new SchemaMutationLockTables(clientPool, config);
        lockTables.createLockTable();
        assertThat(lockTables.getAllLockTables(), is(lockTables2.getAllLockTables()));
    }

    @Test
    public void shouldCreateLockTablesWithCorrectName() throws TException {
        lockTables.createLockTable();

        assertThat(Iterables.getOnlyElement(lockTables.getAllLockTables()).getTablename(), equalTo("_locks"));
    }

    @Test
    public void whenTablesAreCreatedConcurrentlyThereIsStillOnlyOneTable() {
        CyclicBarrier barrier = new CyclicBarrier(2);

        Set<TableReference> lockTablesSeen = Collections.synchronizedSet(new HashSet<>());

        IntStream.range(0, 2).parallel()
                .forEach(ignoringExceptions(() -> {
                    barrier.await();
                    lockTables.createLockTable();
                    lockTablesSeen.addAll(lockTables.getAllLockTables());
                    return null;
                }));

        assertThat("Only one table should have been seen by both creation threads", lockTablesSeen, hasSize(1));
    }

    private IntConsumer ignoringExceptions(Callable function) {
        return (iterationCount) -> {
            try {
                function.call();
            } catch (InvalidRequestException ire) {
                // ignore - expected if tables aren't quite created at the same time.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
