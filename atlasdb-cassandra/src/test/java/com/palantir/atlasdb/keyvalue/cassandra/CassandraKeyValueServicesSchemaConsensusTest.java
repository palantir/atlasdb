/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import java.net.InetSocketAddress;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraKeyValueServicesSchemaConsensusTest {
    private static CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
    private static CassandraKeyValueServiceConfig waitingConfig = mock(CassandraKeyValueServiceConfig.class);
    private static CassandraClient client = mock(CassandraClient.class);

    private static final ImmutableSet<InetSocketAddress> FIVE_SERVERS = ImmutableSet.of(
            new InetSocketAddress("1", 1),
            new InetSocketAddress("2", 1),
            new InetSocketAddress("3", 1),
            new InetSocketAddress("4", 1),
            new InetSocketAddress("5", 1));
    private static final String TABLE = "table";
    private static final String VERSION_1 = "v1";
    private static final String VERSION_2 = "v2";
    private static final String VERSION_UNREACHABLE = "UNREACHABLE";
    private static final ImmutableList<String> QUORUM_OF_NODES = ImmutableList.of("1", "2", "3");
    private static final ImmutableList<String> REST_OF_NODES = ImmutableList.of("4", "5");
    private static final ImmutableList<String> ALL_NODES = ImmutableList.of("1", "2", "3", "4", "5");

    @BeforeClass
    public static void initializeMocks() {
        when(config.schemaMutationTimeoutMillis()).thenReturn(0);
        when(config.servers())
                .thenReturn(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(FIVE_SERVERS)
                        .build());
        when(waitingConfig.schemaMutationTimeoutMillis()).thenReturn(10_000);
        when(waitingConfig.servers())
                .thenReturn(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(FIVE_SERVERS)
                        .build());
    }

    @Test
    public void waitSucceedsForSameSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, ALL_NODES));
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitThrowsForAllUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of());
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitThrowsForAllUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_UNREACHABLE, ALL_NODES));
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitSucceedsWithClusterHavingDownsizedAtRuntime() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, REST_OF_NODES));
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitFailsOnMinorityOnSameVersionAndRestUnreachable() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_UNREACHABLE, QUORUM_OF_NODES, VERSION_1, REST_OF_NODES));
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitSucceedsForQuorumOnlyWithUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES));
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitSucceedsForQuorumOnlyWithUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, REST_OF_NODES));
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitThrowsForDifferentSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_2, REST_OF_NODES));
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitSucceedsForQuorumOnlyWithUnknownAndUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, ImmutableList.of("5")));
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitWaitsForSchemaVersions() throws TException {
        CassandraClient waitingClient = mock(CassandraClient.class);
        when(waitingClient.describe_schema_versions())
                .thenReturn(
                        ImmutableMap.of(),
                        ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_2, REST_OF_NODES),
                        ImmutableMap.of(VERSION_1, REST_OF_NODES, VERSION_UNREACHABLE, QUORUM_OF_NODES),
                        ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, REST_OF_NODES),
                        ImmutableMap.of(VERSION_1, ALL_NODES));

        CassandraKeyValueServices.waitForSchemaVersions(
                waitingConfig.schemaMutationTimeoutMillis(), waitingClient, TABLE);
        verify(waitingClient, times(4)).describe_schema_versions();
    }

    private void assertWaitForSchemaVersionsThrows() {
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(
                        config.schemaMutationTimeoutMillis(), client, TABLE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cassandra cluster cannot come to agreement on schema versions");
    }

    private void assertWaitForSchemaVersionsDoesNotThrow() throws TException {
        CassandraKeyValueServices.waitForSchemaVersions(config.schemaMutationTimeoutMillis(), client, TABLE);
    }
}
