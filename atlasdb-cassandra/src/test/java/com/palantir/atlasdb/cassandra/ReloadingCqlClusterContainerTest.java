/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.ReloadingCqlClusterContainer.CqlClusterFactory;
import com.palantir.atlasdb.cassandra.backup.CqlCluster;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory.CassandraClusterConfig;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReloadingCqlClusterContainerTest {
    private static final Namespace NAMESPACE = Namespace.of("TEST");
    private static final CassandraServersConfig INITIAL_SERVERS_CONFIG = ImmutableDefaultConfig.of();
    private static final CassandraServersConfig UPDATED_SERVERS_CONFIG = ImmutableDefaultConfig.builder()
            .addThriftHosts(InetSocketAddress.createUnresolved("foo", 42))
            .build();

    @Mock
    private CassandraClusterConfig cassandraClusterConfig;

    @Mock
    private CqlClusterFactory cqlClusterFactory;

    private CqlCluster initialCqlClusterMock;

    private CqlCluster refreshedCqlClusterMock;

    private SettableRefreshable<CassandraServersConfig> refreshableCassandraServersConfig;

    private ReloadingCqlClusterContainer reloadingCqlClusterContainer;

    @Before
    public void setUp() {
        initialCqlClusterMock = mockFactoryForServersConfig(INITIAL_SERVERS_CONFIG);
        refreshedCqlClusterMock = mockFactoryForServersConfig(UPDATED_SERVERS_CONFIG);
        refreshableCassandraServersConfig = Refreshable.create(INITIAL_SERVERS_CONFIG);
        reloadingCqlClusterContainer = ReloadingCqlClusterContainer.of(
                cassandraClusterConfig, refreshableCassandraServersConfig, NAMESPACE, cqlClusterFactory);
    }

    @Test
    public void lastCqlClusterClosedAfterClose() throws IOException {
        CqlCluster cqlCluster = reloadingCqlClusterContainer.get();
        reloadingCqlClusterContainer.close();
        verify(cqlCluster).close();
    }

    @Test
    public void previousCqlClusterIsClosedAfterRefresh() throws IOException {
        CqlCluster cqlCluster = reloadingCqlClusterContainer.get();
        refreshableCassandraServersConfig.update(UPDATED_SERVERS_CONFIG);
        verify(cqlCluster).close();
    }

    @Test
    public void newCqlClusterCreatedWithNewServerListAfterRefresh() throws IOException {
        CqlCluster firstCluster = reloadingCqlClusterContainer.get();

        refreshableCassandraServersConfig.update(UPDATED_SERVERS_CONFIG);

        CqlCluster secondCluster = reloadingCqlClusterContainer.get();

        assertThat(firstCluster).isEqualTo(initialCqlClusterMock);
        assertThat(secondCluster).isEqualTo(refreshedCqlClusterMock);
        verify(secondCluster, never()).close();
    }

    @Test
    public void noNewClustersAfterClose() throws IOException {
        reloadingCqlClusterContainer.close();
        refreshableCassandraServersConfig.update(UPDATED_SERVERS_CONFIG);
        assertThat(reloadingCqlClusterContainer.get()).isEqualTo(initialCqlClusterMock);
        verify(cqlClusterFactory, never()).create(cassandraClusterConfig, UPDATED_SERVERS_CONFIG, NAMESPACE);
    }


    private CqlCluster mockFactoryForServersConfig(CassandraServersConfig cassandraServersConfig) {
        CqlCluster cqlCluster = mock(CqlCluster.class);
        when(cqlClusterFactory.create(cassandraClusterConfig, cassandraServersConfig, NAMESPACE))
                .thenReturn(cqlCluster);
        return cqlCluster;
    }
}
