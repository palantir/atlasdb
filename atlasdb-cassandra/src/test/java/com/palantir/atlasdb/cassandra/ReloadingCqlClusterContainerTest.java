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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReloadingCqlClusterContainerTest {
    private static final Namespace NAMESPACE = Namespace.of("TEST");
    private static final CassandraServersConfig EMPTY_SERVERS_CONFIG = ImmutableDefaultConfig.of();
    private static final CassandraServersConfig SERVERS_CONFIG = ImmutableDefaultConfig.builder()
            .addThriftHosts(InetSocketAddress.createUnresolved("foo", 42))
            .build();

    @Mock
    private CassandraClusterConfig cassandraClusterConfig;

    @Mock
    private CqlClusterFactory cqlClusterFactory;

    @Test
    public void lastCqlClusterClosedAfterClose() throws IOException {
        when(cqlClusterFactory.create(cassandraClusterConfig, EMPTY_SERVERS_CONFIG, NAMESPACE))
                .thenReturn(mock(CqlCluster.class));

        Refreshable<CassandraServersConfig> cassandraServersConfigRefreshable = Refreshable.only(EMPTY_SERVERS_CONFIG);
        ReloadingCqlClusterContainer reloadingCqlClusterContainer = ReloadingCqlClusterContainer.of(
                cassandraClusterConfig, cassandraServersConfigRefreshable, NAMESPACE, cqlClusterFactory);

        CqlCluster cqlCluster = reloadingCqlClusterContainer.get();
        reloadingCqlClusterContainer.close();
        verify(cqlCluster).close();
    }

    @Test
    public void previousCqlClusterIsClosedAfterRefresh() throws IOException {
        CqlCluster firstMock = mock(CqlCluster.class);
        CqlCluster secondMock = mock(CqlCluster.class);
        when(cqlClusterFactory.create(cassandraClusterConfig, EMPTY_SERVERS_CONFIG, NAMESPACE))
                .thenReturn(firstMock);
        when(cqlClusterFactory.create(cassandraClusterConfig, SERVERS_CONFIG, NAMESPACE))
                .thenReturn(secondMock);

        SettableRefreshable<CassandraServersConfig> cassandraServersConfigRefreshable =
                Refreshable.create(EMPTY_SERVERS_CONFIG);
        ReloadingCqlClusterContainer reloadingCqlClusterContainer = ReloadingCqlClusterContainer.of(
                cassandraClusterConfig, cassandraServersConfigRefreshable, NAMESPACE, cqlClusterFactory);
        CqlCluster cqlCluster = reloadingCqlClusterContainer.get();
        cassandraServersConfigRefreshable.update(SERVERS_CONFIG);
        verify(cqlCluster).close();
    }

    @Test
    public void newCqlClusterCreatedWithNewServerListAfterRefresh() throws IOException {
        CqlCluster firstMock = mock(CqlCluster.class);
        CqlCluster secondMock = mock(CqlCluster.class);
        when(cqlClusterFactory.create(cassandraClusterConfig, EMPTY_SERVERS_CONFIG, NAMESPACE))
                .thenReturn(firstMock);
        when(cqlClusterFactory.create(cassandraClusterConfig, SERVERS_CONFIG, NAMESPACE))
                .thenReturn(secondMock);
        SettableRefreshable<CassandraServersConfig> cassandraServersConfigRefreshable =
                Refreshable.create(ImmutableDefaultConfig.of());
        ReloadingCqlClusterContainer reloadingCqlClusterContainer = ReloadingCqlClusterContainer.of(
                cassandraClusterConfig, cassandraServersConfigRefreshable, NAMESPACE, cqlClusterFactory);
        CqlCluster firstCluster = reloadingCqlClusterContainer.get();
        cassandraServersConfigRefreshable.update(SERVERS_CONFIG);
        CqlCluster secondCluster = reloadingCqlClusterContainer.get();
        assertThat(firstCluster).isEqualTo(firstMock);
        assertThat(secondCluster).isEqualTo(secondMock);
        verify(secondCluster, never()).close();
    }

    @Test
    public void noNewClustersAfterClose() throws IOException {
        CqlCluster cqlCluster = mock(CqlCluster.class);
        when(cqlClusterFactory.create(cassandraClusterConfig, EMPTY_SERVERS_CONFIG, NAMESPACE))
                .thenReturn(cqlCluster);

        SettableRefreshable<CassandraServersConfig> cassandraServersConfigRefreshable =
                Refreshable.create(ImmutableDefaultConfig.of());
        ReloadingCqlClusterContainer reloadingCqlClusterContainer = ReloadingCqlClusterContainer.of(
                cassandraClusterConfig, cassandraServersConfigRefreshable, NAMESPACE, cqlClusterFactory);
        reloadingCqlClusterContainer.close();
        cassandraServersConfigRefreshable.update(SERVERS_CONFIG);
        assertThat(reloadingCqlClusterContainer.get()).isEqualTo(cqlCluster);
        verify(cqlClusterFactory, never()).create(cassandraClusterConfig, SERVERS_CONFIG, NAMESPACE);
    }
}
