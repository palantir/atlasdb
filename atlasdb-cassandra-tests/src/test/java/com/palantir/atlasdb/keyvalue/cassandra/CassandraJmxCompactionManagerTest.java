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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionClient;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;

/**
 * All tests are Jmx disabled.
 */
public class CassandraJmxCompactionManagerTest {

    private CassandraJmxCompactionManager clientsManager;
    private ExecutorService exec = Executors.newFixedThreadPool(
            1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("test-Cassandra-Compaction-ThreadPool-%d").build());

    private static final String TEST_KEY_SPACE = "testKeySpace";
    private static final String TEST_TABLE_NAME = "testTableName";

    private Set<CassandraJmxCompactionClient> getCompactionClients() {
        CassandraJmxCompactionClient client = mock(CassandraJmxCompactionClient.class);
        Set<CassandraJmxCompactionClient> clients = Sets.newHashSet();
        clients.add(client);
        return clients;
    }

    @Test
    public void newInstanceWithEmptyClientsShouldWork() {
        Set<CassandraJmxCompactionClient> emptyClients = ImmutableSet.of();
        clientsManager = CassandraJmxCompactionManager.newInstance(emptyClients, exec);
    }

    @Test (expected = NullPointerException.class)
    public void newInstanceWithNullClientsShouldFail() {
        clientsManager = CassandraJmxCompactionManager.newInstance(null, exec);
    }


    @Test
    public void getCompactionClientsShouldReturnTheSameClients() {
        Set<CassandraJmxCompactionClient> clients = getCompactionClients();
        clientsManager = CassandraJmxCompactionManager.newInstance(clients, exec);
        assertEquals(clientsManager.getCompactionClients(), clients);
    }

    @Test (expected = UnsupportedOperationException.class)
    public void getCompactionClientsShouldReturnUnmodifiedView() {
        Set<CassandraJmxCompactionClient> clients = getCompactionClients();
        clientsManager = CassandraJmxCompactionManager.newInstance(clients, exec);
        clientsManager.getCompactionClients().clear();
    }

    @Test (expected = TimeoutException.class)
    public void tombStoneCompactionGetTimeout() throws TimeoutException {
        Set<CassandraJmxCompactionClient> clients = getCompactionClients();
        clientsManager = CassandraJmxCompactionManager.newInstance(clients, exec);
        try {
            clientsManager.performTombstoneCompaction(0, TEST_KEY_SPACE, TEST_TABLE_NAME);
        } catch (InterruptedException e) {
            fail("should not be here.");
        }
    }

    @Test (expected = InterruptedException.class)
    public void tombStoneCompactionGetInterrupted() throws InterruptedException {
        Set<CassandraJmxCompactionClient> clients = getCompactionClients();
        clientsManager = CassandraJmxCompactionManager.newInstance(clients, exec);
        try {
            Thread.currentThread().interrupt();
            clientsManager.performTombstoneCompaction(20, TEST_KEY_SPACE, TEST_TABLE_NAME);
        } catch (TimeoutException e) {
            fail("should not be here.");
        }
    }

    @Test
    public void tombStoneCompactionWithEmptyClients() throws InterruptedException, TimeoutException {
        Set<CassandraJmxCompactionClient> emptyClients = ImmutableSet.of();
        clientsManager = CassandraJmxCompactionManager.newInstance(emptyClients, exec);
        clientsManager.performTombstoneCompaction(20, TEST_KEY_SPACE, TEST_TABLE_NAME);
    }

    @Test
    public void verifyTombStoneCompaction() throws InterruptedException, TimeoutException {
        Set<CassandraJmxCompactionClient> clients = getCompactionClients();
        clientsManager = CassandraJmxCompactionManager.newInstance(clients, exec);
        clientsManager.performTombstoneCompaction(100, TEST_KEY_SPACE, TEST_TABLE_NAME);
        CassandraJmxCompactionClient client = clients.iterator().next();

        verify(client).deleteLocalHints();
        verify(client).forceTableFlush(TEST_KEY_SPACE, TEST_TABLE_NAME);
        verify(client).forceTableCompaction(TEST_KEY_SPACE, TEST_TABLE_NAME);
    }

}
