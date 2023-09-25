/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.CassandraTopologyValidationMetrics;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolImpl.StartupChecks;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.InitializeableScheduledExecutorServiceSupplier;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

public class CassandraClientPoolCloudMigrationIntegrationTest {
    private static final InetSocketAddress CLOUD_1_HOST_1 = InetSocketAddress.createUnresolved("one", 1234);
    private static final InetSocketAddress CLOUD_1_HOST_2 = InetSocketAddress.createUnresolved("two", 1234);
    private static final InetSocketAddress CLOUD_1_HOST_3 = InetSocketAddress.createUnresolved("three", 1234);
    private static final Set<InetSocketAddress> CLOUD_1_HOSTS =
            ImmutableSet.of(CLOUD_1_HOST_1, CLOUD_1_HOST_2, CLOUD_1_HOST_3);
    private static final Set<CassandraServer> CLOUD_1_SERVERS =
            CLOUD_1_HOSTS.stream().map(CassandraServer::of).collect(Collectors.toSet());

    private static final InetSocketAddress CLOUD_2_HOST_1 = InetSocketAddress.createUnresolved("eins", 1234);
    private static final InetSocketAddress CLOUD_2_HOST_2 = InetSocketAddress.createUnresolved("zwei", 1234);
    private static final InetSocketAddress CLOUD_2_HOST_3 = InetSocketAddress.createUnresolved("drei", 1234);
    private static final Set<InetSocketAddress> CLOUD_2_HOSTS =
            ImmutableSet.of(CLOUD_2_HOST_1, CLOUD_2_HOST_2, CLOUD_2_HOST_3);
    private static final Set<CassandraServer> CLOUD_2_SERVERS =
            CLOUD_2_HOSTS.stream().map(CassandraServer::of).collect(Collectors.toSet());

    private static final CassandraKeyValueServiceConfig CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .keyspace("keyspace") // Will not actually be filled in by a higher-level Atlas construct.
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username("u")
                    .password("p")
                    .build())
            .build();

    private final DeterministicScheduler deterministicScheduler = new DeterministicScheduler();

    @Test
    public void clusterCanInitialiseFromNoQuorumStateAfterSecondCloudConnected() {
        SettableRefreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable =
                initialiseConfigurationTo(CLOUD_1_HOSTS);
        FakeCassandraService fakeCassandraService = FakeCassandraService.create(
                runtimeConfigRefreshable,
                Stream.of(CLOUD_1_HOSTS, CLOUD_2_HOSTS)
                        .flatMap(Set::stream)
                        .map(CassandraServer::of)
                        .collect(Collectors.toSet()));
        connectSecondCloudDatacenter(fakeCassandraService);
        enableServiceDiscoveryForSecondCloudNodes(runtimeConfigRefreshable);

        FakeCassandraClientPool clientPool =
                FakeCassandraClientPool.create(fakeCassandraService, runtimeConfigRefreshable, deterministicScheduler);
        fakeCassandraService.simulateTokenRingFailure();
        fakeCassandraService.clearPools();

        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("we should still be able to accept nodes from the first cloud, because of the no quorum handler")
                .containsOnlyKeys(CLOUD_1_SERVERS);

        enableClientInterfacesOnSecondCloud(fakeCassandraService);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("we should accept nodes from the second cloud, once they come online")
                .containsOnlyKeys(Sets.union(CLOUD_1_SERVERS, CLOUD_2_SERVERS));
    }

    @Test
    public void clusterCanInitialiseFromNoQuorumStateAfterFirstCloudDisconnected() {
        SettableRefreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable =
                initialiseConfigurationTo(CLOUD_2_HOSTS);
        FakeCassandraService fakeCassandraService = FakeCassandraService.create(
                runtimeConfigRefreshable,
                Stream.of(CLOUD_1_HOSTS, CLOUD_2_HOSTS)
                        .flatMap(Set::stream)
                        .map(CassandraServer::of)
                        .collect(Collectors.toSet()));
        FakeCassandraClientPool clientPool =
                FakeCassandraClientPool.create(fakeCassandraService, runtimeConfigRefreshable, deterministicScheduler);
        fakeCassandraService.simulateTokenRingFailure();
        fakeCassandraService.clearPools();

        disableClientInterfacesOnFirstCloud(fakeCassandraService);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the client pool can be initialised post-disabling original cloud interfaces")
                .containsOnlyKeys(CLOUD_2_SERVERS);
    }

    @Test
    public void happyPathMigrationRunsWithoutDowntime() {
        SettableRefreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable =
                initialiseConfigurationTo(CLOUD_1_HOSTS);
        FakeCassandraService fakeCassandraService = FakeCassandraService.create(
                runtimeConfigRefreshable,
                CLOUD_1_HOSTS.stream().map(CassandraServer::of).collect(Collectors.toSet()));

        FakeCassandraClientPool clientPool =
                FakeCassandraClientPool.create(fakeCassandraService, runtimeConfigRefreshable, deterministicScheduler);

        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the client pool is initialised to the correct size")
                .containsOnlyKeys(CLOUD_1_SERVERS);

        connectSecondCloudDatacenter(fakeCassandraService);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the new nodes should be discoverable in Cassandra, but should not be added to the cluster")
                .containsOnlyKeys(CLOUD_1_SERVERS);

        enableServiceDiscoveryForSecondCloudNodes(runtimeConfigRefreshable);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the new nodes should be discoverable in config, but should not yet be added to the cluster as"
                        + " they are still unreachable")
                .containsOnlyKeys(CLOUD_1_SERVERS);

        enableClientInterfacesOnSecondCloud(fakeCassandraService);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the new nodes should present a topology consistent with the existing nodes, so we can add them")
                .containsOnlyKeys(Sets.union(CLOUD_1_SERVERS, CLOUD_2_SERVERS));

        disableServiceDiscoveryForFirstCloudNodes(runtimeConfigRefreshable);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("we should still know about the nodes in the first cloud")
                .containsOnlyKeys(Sets.union(CLOUD_1_SERVERS, CLOUD_2_SERVERS));

        disableClientInterfacesOnFirstCloud(fakeCassandraService);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the nodes in the first cloud are still a part of the ring; we don't need to decomm them yet")
                .containsOnlyKeys(Sets.union(CLOUD_1_SERVERS, CLOUD_2_SERVERS));

        disconnectFirstCloudDatacenter(fakeCassandraService);
        refreshClientPool();
        assertThat(clientPool.getCassandraClientPool().getCurrentPools())
                .as("the nodes in the first cloud are no longer a part of the ring, and should be removed")
                .containsOnlyKeys(CLOUD_2_SERVERS);
    }

    private static void disconnectFirstCloudDatacenter(FakeCassandraService fakeCassandraService) {
        fakeCassandraService.setServersDiscoverableInTokenRing(CLOUD_2_SERVERS);
    }

    private static void disableServiceDiscoveryForFirstCloudNodes(
            SettableRefreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable) {
        runtimeConfigRefreshable.update(ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                .servers(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(CLOUD_2_HOSTS)
                        .build())
                .build());
    }

    private static void enableClientInterfacesOnSecondCloud(FakeCassandraService fakeCassandraService) {
        fakeCassandraService.setServersReachable(CLOUD_2_SERVERS);
    }

    private static void disableClientInterfacesOnFirstCloud(FakeCassandraService fakeCassandraService) {
        fakeCassandraService.setServersUnreachable(CLOUD_1_SERVERS);
    }

    private static void enableServiceDiscoveryForSecondCloudNodes(
            SettableRefreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable) {
        runtimeConfigRefreshable.update(ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                .servers(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(CLOUD_1_HOSTS)
                        .addAllThriftHosts(CLOUD_2_HOSTS)
                        .build())
                .build());
    }

    private static void connectSecondCloudDatacenter(FakeCassandraService fakeCassandraService) {
        fakeCassandraService.setServersDiscoverableInTokenRing(Sets.union(CLOUD_1_SERVERS, CLOUD_2_SERVERS));
        fakeCassandraService.setServersUnreachable(CLOUD_2_SERVERS);
    }

    @NotNull
    private static SettableRefreshable<CassandraKeyValueServiceRuntimeConfig> initialiseConfigurationTo(
            Set<InetSocketAddress> hosts) {
        CassandraKeyValueServiceRuntimeConfig runtimeConfig = ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                .servers(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(hosts)
                        .build())
                .build();
        return Refreshable.create(runtimeConfig);
    }

    private void refreshClientPool() {
        deterministicScheduler.tick(CONFIG.poolRefreshIntervalSeconds(), TimeUnit.SECONDS);
    }

    private static Set<String> getHostIds(Set<CassandraServer> cassandraServers) {
        return cassandraServers.stream().map(CassandraServer::cassandraHostName).collect(Collectors.toSet());
    }

    private static final class FakeCassandraClientPool {
        private final CassandraClientPool cassandraClientPool;

        private FakeCassandraClientPool(CassandraClientPool cassandraClientPool) {
            this.cassandraClientPool = cassandraClientPool;
        }

        public static FakeCassandraClientPool create(
                FakeCassandraService fakeCassandraService,
                Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable,
                DeterministicScheduler deterministicScheduler) {
            MetricsManager metricsManager = MetricsManagers.createForTests();
            CassandraTopologyValidator validator = new CassandraTopologyValidator(
                    CassandraTopologyValidationMetrics.of(metricsManager.getTaggedRegistry()),
                    new K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategy(
                            runtimeConfigRefreshable.map(CassandraKeyValueServiceRuntimeConfig::servers)));
            CassandraClientPool pool = CassandraClientPoolImpl.createImplForTest(
                    metricsManager,
                    CONFIG,
                    runtimeConfigRefreshable,
                    StartupChecks.DO_NOT_RUN,
                    InitializeableScheduledExecutorServiceSupplier.createForTests(deterministicScheduler),
                    new Blacklist(CONFIG, Refreshable.only(5)),
                    fakeCassandraService.getCassandraServiceInterface(),
                    validator,
                    new CassandraAbsentHostTracker(CONFIG.consecutiveAbsencesBeforePoolRemoval()));
            return new FakeCassandraClientPool(pool);
        }

        public CassandraClientPool getCassandraClientPool() {
            return cassandraClientPool;
        }
    }

    private static final class FakeCassandraService {
        private final Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable;
        private final CassandraService cassandraService = mock(CassandraService.class);
        private final Set<CassandraServer> serversDiscoverableInTokenRing = new HashSet<>();
        private final Map<CassandraServer, CassandraClientPoolingContainer> containers = new HashMap<>();
        private final Set<CassandraServer> unreachableServers = new HashSet<>();
        private final Set<CassandraServer> knownServers = new HashSet<>();

        public FakeCassandraService(
                Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable,
                Set<CassandraServer> hosts) {
            this.runtimeConfigRefreshable = runtimeConfigRefreshable;
            knownServers.addAll(hosts);
            hosts.forEach(host -> {
                CassandraClientPoolingContainer container = mock(CassandraClientPoolingContainer.class);
                try {
                    setupCassandraContainer(host, container);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                containers.put(host, container);
            });
        }

        public static FakeCassandraService create(
                Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable,
                Set<CassandraServer> initialHosts) {
            FakeCassandraService fakeCassandraService =
                    new FakeCassandraService(runtimeConfigRefreshable, initialHosts);
            fakeCassandraService.setupDefaultCassandraServiceBehaviour();
            fakeCassandraService.setServersDiscoverableInTokenRing(initialHosts);
            return fakeCassandraService;
        }

        public CassandraService getCassandraServiceInterface() {
            return cassandraService;
        }

        public void setupDefaultCassandraServiceBehaviour() {
            when(cassandraService.getPools()).thenReturn(containers);
            when(cassandraService.createPool(any())).thenAnswer(invocation -> {
                CassandraClientPoolingContainer container = mock(CassandraClientPoolingContainer.class);
                setupCassandraContainer(invocation.getArgument(0), container);
                return container;
            });
            doAnswer(invocation -> {
                        containers.put(invocation.getArgument(0), invocation.getArgument(1));
                        return null;
                    })
                    .when(cassandraService)
                    .addPool(any(), any());
            doAnswer(invocation -> containers.remove((CassandraServer) invocation.getArgument(0)))
                    .when(cassandraService)
                    .removePool(any(CassandraServer.class));
        }

        public void setServersDiscoverableInTokenRing(Set<CassandraServer> servers) {
            serversDiscoverableInTokenRing.clear();
            serversDiscoverableInTokenRing.addAll(servers);

            Map<CassandraServer, CassandraServerOrigin> hostsToTokenRangeOrigin =
                    serversDiscoverableInTokenRing.stream()
                            .collect(Collectors.toMap(
                                    Function.identity(), _unused -> CassandraServerOrigin.TOKEN_RANGE));
            when(cassandraService.refreshTokenRangesAndGetServers())
                    .thenReturn(ImmutableMap.copyOf(hostsToTokenRangeOrigin));

            knownServers.clear();
            knownServers.addAll(servers);
            containers.forEach(this::setupCassandraContainer);
        }

        public void setServersUnreachable(Set<CassandraServer> servers) {
            unreachableServers.addAll(servers);
        }

        public void clearPools() {
            containers.clear();
        }

        public void setServersReachable(Set<CassandraServer> servers) {
            unreachableServers.removeAll(servers);
        }

        public void simulateTokenRingFailure() {
            CassandraServersConfig configServers =
                    runtimeConfigRefreshable.get().servers();
            Set<InetSocketAddress> configAddresses = configServers.accept(ThriftHostsExtractingVisitor.INSTANCE);
            Set<CassandraServer> configCassandraServers =
                    configAddresses.stream().map(CassandraServer::of).collect(Collectors.toSet());
            Map<CassandraServer, CassandraServerOrigin> hostsToTokenRangeOrigin =
                    serversDiscoverableInTokenRing.stream()
                            .collect(Collectors.toMap(
                                    Function.identity(),
                                    server -> configCassandraServers.contains(server)
                                            ? CassandraServerOrigin.CONFIG
                                            : CassandraServerOrigin.LAST_KNOWN));
            when(cassandraService.refreshTokenRangesAndGetServers())
                    .thenReturn(ImmutableMap.copyOf(hostsToTokenRangeOrigin));
        }

        private void setupCassandraContainer(CassandraServer host, CassandraClientPoolingContainer container) {
            try {
                when(container.<HostIdResult, Exception>runWithPooledResource(any()))
                        .thenAnswer(invocation -> {
                            if (unreachableServers.contains(host)) {
                                return HostIdResult.hardFailure();
                            } else {
                                return HostIdResult.success(getHostIds(knownServers));
                            }
                        });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
