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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraClientPoolIntegrationTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    // private int modifiedReplicationFactor;
    private Blacklist blacklist;
    // private CassandraClientPoolImpl clientPool;

    @Before
    public void setUp() {
        blacklist = new Blacklist(CASSANDRA.getConfig());
    }

    @Test
    public void testRefreshLocalHosts() {
        HostLocation localLocation = HostLocation.of("dc1", "rack1");
        HostLocation remoteLocation = HostLocation.of("dc1", "rack4");

        assertThat(getLocalHostsUsingLocation(localLocation)).isNotEmpty();
        assertThat(getLocalHostsUsingLocation(remoteLocation)).isEmpty();
    }

    private Set<InetSocketAddress> getLocalHostsUsingLocation(HostLocation hostLocation) {
        CassandraKeyValueServiceConfig configHostWithLocation = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA.getConfig())
                .overrideHostLocation(Optional.of(hostLocation))
                .build();

        CassandraClientPoolImpl clientPoolWithLocation = CassandraClientPoolImpl.createImplForTest(
                metricsManager, configHostWithLocation, CassandraClientPoolImpl.StartupChecks.RUN, blacklist);

        return clientPoolWithLocation.getLocalHosts();
    }
}
