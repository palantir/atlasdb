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
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;

import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;


public class CassandraKeyValueServicesTest {
    private static CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
    private static Cassandra.Client client = mock(Cassandra.Client.class);
    private static final Set<InetSocketAddress> FIVE_SERVERS = ImmutableSet.of(
            new InetSocketAddress("1", 0),
            new InetSocketAddress("2", 0),
            new InetSocketAddress("3", 0),
            new InetSocketAddress("4", 0),
            new InetSocketAddress("5", 0));
    private static final String TABLE = "table";

    private static final String VERSION_1 = "v1";
    private static final String VERSION_2 = "v2";
    private static final String VERSION_UNREACHABLE = CassandraKeyValueServices.VERSION_UNREACHABLE;
    private static final List<String> QUORUM_OF_NODES = ImmutableList.of("1", "2", "3");
    private static final List<String> REST_OF_NODES = ImmutableList.of("4", "5");
    private static final List<String> ALL_NODES = ImmutableList.of("1", "2", "3", "4", "5");

    @BeforeClass
    public static void initializeMocks() {
        when(config.schemaMutationTimeoutMillis()).thenReturn(0);
        when(config.servers()).thenReturn(FIVE_SERVERS);
    }

    @Test
    public void waitForSchemaVersionsAllSucceedsForSameSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, ALL_NODES));
        CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE);
    }

    @Test
    public void waitForSchemaVersionsQuorumFailsForAllUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of());
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, true))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void waitForSchemaVersionsQuorumFailsForAllUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_UNREACHABLE, ALL_NODES));
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, true))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void waitForSchemaVersionsAllFailsForUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES));
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void waitForSchemaVersionsQuorumSucceedsForUnknownSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES));
        CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, true);
    }

    @Test
    public void waitForSchemaVersionsAllFailsForUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, REST_OF_NODES));
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void waitForSchemaVersionsQuorumSucceedsForUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, REST_OF_NODES));
        CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, true);
    }

    @Test
    public void waitForSchemaVersionsQuorumFailsForDifferentSchemaVersion() throws TException {
        when(client.describe_schema_versions())
                .thenReturn(ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_2, REST_OF_NODES));
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, true))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void waitForSchemaVersionsQuorumSucceedsForUnknownAndUnreachableSchemaVersion() throws TException {
        when(client.describe_schema_versions()).thenReturn(
                ImmutableMap.of(VERSION_1, QUORUM_OF_NODES, VERSION_UNREACHABLE, ImmutableList.of("5")));
        CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE, true);
    }
}
