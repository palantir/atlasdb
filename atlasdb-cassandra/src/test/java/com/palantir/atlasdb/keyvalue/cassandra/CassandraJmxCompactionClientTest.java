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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.management.remote.JMXConnector;

import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionClient;

public class CassandraJmxCompactionClientTest {
    private JMXConnector jmxConnector;
    private StorageServiceMBean storageServiceProxy;
    private HintedHandOffManagerMBean hintedHandoffProxy;
    private CompactionManagerMBean compactionManagerProxy;
    private CassandraJmxCompactionClient jmxClient;

    private static final String FAKE_HOST = "localhost";
    private static final int FAKE_JMX_PORT = 7199;
    private static final String TEST_KEY_SPACE = "testKeySpace";
    private static final TableReference TEST_TABLE_NAME = TableReference.createWithEmptyNamespace("testTableName");

    @Before
    public void before() {
        jmxConnector = mock(JMXConnector.class);
        storageServiceProxy = mock(StorageServiceMBean.class);
        hintedHandoffProxy = mock(HintedHandOffManagerMBean.class);
        compactionManagerProxy = mock(CompactionManagerMBean.class);
        jmxClient = CassandraJmxCompactionClient.create(FAKE_HOST, FAKE_JMX_PORT,
                jmxConnector, storageServiceProxy, hintedHandoffProxy, compactionManagerProxy);
    }

    @Test
    public void verifyDeleteLocalHints() {
        jmxClient.deleteLocalHints();
        verify(hintedHandoffProxy).deleteHintsForEndpoint(FAKE_HOST);
    }

    @Test
    public void verifyForceTableFlush() throws InterruptedException, ExecutionException, IOException {
        jmxClient.forceTableFlush(TEST_KEY_SPACE, TEST_TABLE_NAME);
        verify(storageServiceProxy).forceKeyspaceFlush(TEST_KEY_SPACE, TEST_TABLE_NAME.getQualifiedName());
    }

    @Test
    public void verifyForceTableCompaction() throws InterruptedException, ExecutionException, IOException {
        jmxClient.forceTableCompaction(TEST_KEY_SPACE, TEST_TABLE_NAME);
        verify(storageServiceProxy).forceKeyspaceCompaction(true, TEST_KEY_SPACE, TEST_TABLE_NAME.getQualifiedName());
    }

    @Test
    public void verifyGetCompactionStatus() {
        jmxClient.getCompactionStatus();
        verify(compactionManagerProxy).getCompactionSummary();
    }

    @Test
    public void verifyClose() throws IOException {
        jmxClient.close();
        verify(jmxConnector).close();
    }
}
