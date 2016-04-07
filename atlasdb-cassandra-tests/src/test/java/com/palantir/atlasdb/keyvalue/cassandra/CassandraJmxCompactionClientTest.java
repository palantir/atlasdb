/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
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
    private JMXConnector mockedJmxConnector;
    private StorageServiceMBean mockedSSProxy;
    private HintedHandOffManagerMBean mockedHHProxy;
    private CompactionManagerMBean mockedCMProxy;
    private CassandraJmxCompactionClient jmxClient;

    private static String FAKE_HOST = "localhost";
    private static int FAKE_JMX_PORT = 7199;
    private static final String TEST_KEY_SPACE = "testKeySpace";
    private static final TableReference TEST_TABLE_NAME = TableReference.createWithEmptyNamespace("testTableName");

    @Before
    public void setUp() {
        mockedJmxConnector = mock(JMXConnector.class);
        mockedSSProxy = mock(StorageServiceMBean.class);
        mockedHHProxy = mock(HintedHandOffManagerMBean.class);
        mockedCMProxy = mock(CompactionManagerMBean.class);
        jmxClient = CassandraJmxCompactionClient.create(FAKE_HOST, FAKE_JMX_PORT,
                mockedJmxConnector, mockedSSProxy, mockedHHProxy, mockedCMProxy);
    }

    @Test
    public void verifyDeleteLocalHints() {
        jmxClient.deleteLocalHints();
        verify(mockedHHProxy).deleteHintsForEndpoint(FAKE_HOST);
    }

    @Test
    public void verifyForceTableFlush() throws InterruptedException, ExecutionException, IOException {
        jmxClient.forceTableFlush(TEST_KEY_SPACE, TEST_TABLE_NAME);
        verify(mockedSSProxy).forceKeyspaceFlush(TEST_KEY_SPACE, TEST_TABLE_NAME.getQualifiedName());
    }

    @Test
    public void verifyForceTableCompaction() throws InterruptedException, ExecutionException, IOException {
        jmxClient.forceTableCompaction(TEST_KEY_SPACE, TEST_TABLE_NAME);
        verify(mockedSSProxy).forceKeyspaceCompaction(true, TEST_KEY_SPACE, TEST_TABLE_NAME.getQualifiedName());
    }

    @Test
    public void verifyGetCompactionStatus() {
        jmxClient.getCompactionStatus();
        verify(mockedCMProxy).getCompactionSummary();
    }

    @Test
    public void verifyClose() throws IOException {
        jmxClient.close();
        verify(mockedJmxConnector).close();
    }
}
