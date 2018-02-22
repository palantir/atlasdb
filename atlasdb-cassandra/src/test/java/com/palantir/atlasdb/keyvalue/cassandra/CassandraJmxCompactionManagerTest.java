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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionClient;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;

/**
 * All tests are Jmx disabled.
 */
public class CassandraJmxCompactionManagerTest {
    private ExecutorService exec;
    ImmutableSet<CassandraJmxCompactionClient> mockedClients;

    private static final String TEST_KEY_SPACE = "testKeySpace";
    private static final TableReference TEST_TABLE_NAME = TableReference.createWithEmptyNamespace("testTableName");

    @Before
    public void before() {
        CassandraJmxCompactionClient jmxCompactionClient = mock(CassandraJmxCompactionClient.class);
        mockedClients = ImmutableSet.of(jmxCompactionClient);
        exec = Executors.newFixedThreadPool(
                1, new ThreadFactoryBuilder().setNameFormat("test-Cassandra-Compaction-ThreadPool-%d").build());
    }

    @After
    public void tearDown() {
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    @Test
    public void createShouldWorkWithEmptyClients() {
        CassandraJmxCompactionManager.create(ImmutableSet.<CassandraJmxCompactionClient>of(), exec);
    }

    @Test(expected = NullPointerException.class)
    public void createShouldFailWithNullParameter() {
        CassandraJmxCompactionManager.create(null, exec);
    }

    @Test
    public void verifyTombstoneCompactionTask() throws InterruptedException, TimeoutException, ExecutionException {
        CassandraJmxCompactionManager clientManager = CassandraJmxCompactionManager.create(mockedClients, exec);
        clientManager.performTombstoneCompaction(10, TEST_KEY_SPACE, TEST_TABLE_NAME);
        CassandraJmxCompactionClient client = Iterables.get(mockedClients, 0);

        verify(client).deleteLocalHints();
        verify(client).forceTableFlush(TEST_KEY_SPACE, TEST_TABLE_NAME);
        verify(client).forceTableCompaction(TEST_KEY_SPACE, TEST_TABLE_NAME);
    }
}
