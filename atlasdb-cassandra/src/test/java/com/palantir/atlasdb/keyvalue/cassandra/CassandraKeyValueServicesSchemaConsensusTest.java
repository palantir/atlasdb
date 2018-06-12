/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

@RunWith(Parameterized.class)
public class CassandraKeyValueServicesSchemaConsensusTest {
    private static CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
    private static CassandraKeyValueServiceConfig waitingConfig = mock(CassandraKeyValueServiceConfig.class);
    private static CassandraClient client = mock(CassandraClient.class);

    private static final Set<InetSocketAddress> FIVE_SERVERS = ImmutableSet.of(
            new InetSocketAddress("1", 0),
            new InetSocketAddress("2", 0),
            new InetSocketAddress("3", 0),
            new InetSocketAddress("4", 0),
            new InetSocketAddress("5", 0));
    private static final String TABLE = "table";
    private static final String VERSION_1 = "v1";
    private static final String VERSION_2 = "v2";
    private static final String VERSION_UNREACHABLE = "UNREACHABLE";
    private static final List<String> QUORUM_OF_NODES = ImmutableList.of("1", "2", "3");
    private static final List<String> REST_OF_NODES = ImmutableList.of("4", "5");
    private static final List<String> ALL_NODES = ImmutableList.of("1", "2", "3", "4", "5");

    @SuppressWarnings("WeakerAccess") // test parameter
    @Parameterized.Parameter
    public boolean requiresQuorum;

    @SuppressWarnings("WeakerAccess") // test parameter
    @Parameterized.Parameter(value = 1)
    public int expectedAttempts;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                { true, 3 },
                { false, 4 }});
    }

    @BeforeClass
    public static void initializeMocks() {
        when(config.schemaMutationTimeoutMillis()).thenReturn(0);
        when(config.servers()).thenReturn(FIVE_SERVERS);
        when(waitingConfig.schemaMutationTimeoutMillis()).thenReturn(10_000);
        when(waitingConfig.servers()).thenReturn(FIVE_SERVERS);
    }

    @Test
    public void waitSucceedsForSameSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, ALL_NODES));
        CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, requiresQuorum);
    }

    @Test
    public void waitThrowsForAllUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of());
        assertWaitForSchemaVersionsThrowsAndContainsConfigNodesInformation();
    }

    @Test
    public void waitThrowsForAllUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_UNREACHABLE, ALL_NODES));
        assertWaitForSchemaVersionsThrowsAndContainsConfigNodesInformation();
    }

    @Test
    public void waitThrowsForFewerThanQuorumOnSameVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, REST_OF_NODES));
        assertWaitForSchemaVersionsThrowsAndContainsConfigNodesInformation();
    }

    @Test
    public void waitThrowsForQuorumOfUnreachableNodes() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_UNREACHABLE, QUORUM_OF_NODES, VERSION_1, REST_OF_NODES));
        assertWaitForSchemaVersionsThrowsAndContainsConfigNodesInformation();
    }

    @Test
    public void waitSucceedsForQuorumOnlyWithUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES));
        assertWaitForSchemaVersionsThrowsOnlyIfRequiresAll();
    }

    @Test
    public void waitSucceedsForQuorumOnlyWithUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, REST_OF_NODES));
        assertWaitForSchemaVersionsThrowsOnlyIfRequiresAll();
    }

    @Test
    public void waitThrowsForDifferentSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_2, REST_OF_NODES));
        assertWaitForSchemaVersionsThrowsAndContainsConfigNodesInformation();
    }

    @Test
    public void waitSucceedsForQuorumOnlyWithUnknownAndUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(
                ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, ImmutableList.of("5")));
        assertWaitForSchemaVersionsThrowsOnlyIfRequiresAll();
    }

    @Test
    public void waitWaitsForSchemaVersions() throws TException {
        CassandraClient waitingClient = mock(CassandraClient.class);
        when(waitingClient.describe_schema_versions()).thenReturn(
                ImmutableMap.of(),
                ImmutableMap.of(VERSION_UNREACHABLE, QUORUM_OF_NODES, VERSION_1, REST_OF_NODES),
                ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, REST_OF_NODES),
                ImmutableMap.of(VERSION_1, ALL_NODES));

        CassandraKeyValueServices.waitForSchemaVersions(waitingConfig, waitingClient, TABLE, requiresQuorum);
        verify(waitingClient, times(expectedAttempts)).describe_schema_versions();
    }

    private void assertWaitForSchemaVersionsThrowsAndContainsConfigNodesInformation() {
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, requiresQuorum))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(FIVE_SERVERS.iterator().next().getHostName());
    }

    private void assertWaitForSchemaVersionsThrowsOnlyIfRequiresAll() throws TException {
        try {
            CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, requiresQuorum);
        } catch (IllegalStateException e) {
            Assert.assertFalse(requiresQuorum);
        }
    }
}
