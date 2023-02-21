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
package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class CassandraServiceTest {
    private static final int DEFAULT_PORT = 5000;
    private static final int OTHER_PORT = 6000;
    private static final String HOSTNAME_1 = "1.0.0.0";
    private static final String HOSTNAME_2 = "2.0.0.0";
    private static final String HOSTNAME_3 = "3.0.0.0";
    private static final InetSocketAddress HOST_1 = InetSocketAddress.createUnresolved(HOSTNAME_1, DEFAULT_PORT);
    private static final InetSocketAddress HOST_2 = InetSocketAddress.createUnresolved(HOSTNAME_2, DEFAULT_PORT);
    private static final InetSocketAddress HOST_3 = InetSocketAddress.createUnresolved(HOSTNAME_3, DEFAULT_PORT);

    private static final CassandraServer SERVER_1 = CassandraServer.of(HOST_1);
    private static final CassandraServer SERVER_2 = CassandraServer.of(HOST_2);
    private static final CassandraServer SERVER_3 = CassandraServer.of(HOST_3);

    private static final String DC_1 = "london";
    private static final String DC_2 = "singapore";
    private static final String DC_3 = "zurich";

    private CassandraKeyValueServiceConfig config;
    private Blacklist blacklist;

    @Test
    public void shouldOnlyReturnLocalHosts() {
        ImmutableSet<CassandraServer> hosts = ImmutableSet.of(SERVER_1, SERVER_2);
        ImmutableSet<CassandraServer> localHosts = ImmutableSet.of(SERVER_1);

        CassandraService cassandra = clientPoolWithServersAndParams(hosts, 1.0);

        cassandra.setLocalHosts(localHosts);

        assertThat(cassandra.maybeFilterLocalHosts(hosts)).containsExactlyInAnyOrderElementsOf(localHosts);
    }

    @Test
    public void shouldReturnAllHostsBySkippingFilter() {
        ImmutableSet<CassandraServer> hosts = ImmutableSet.of(SERVER_1, SERVER_2);
        ImmutableSet<CassandraServer> localHosts = ImmutableSet.of(SERVER_1);

        CassandraService cassandra = clientPoolWithServersAndParams(hosts, 0.0);

        cassandra.setLocalHosts(localHosts);

        assertThat(cassandra.maybeFilterLocalHosts(hosts)).containsExactlyInAnyOrderElementsOf(hosts);
    }

    @Test
    public void shouldReturnAllHostsAsNoIntersection() {
        ImmutableSet<CassandraServer> hosts = ImmutableSet.of(SERVER_1, SERVER_2);
        ImmutableSet<CassandraServer> localHosts = ImmutableSet.of();

        CassandraService cassandra = clientPoolWithServersAndParams(hosts, 0.0);

        cassandra.setLocalHosts(localHosts);

        assertThat(cassandra.maybeFilterLocalHosts(hosts)).containsExactlyInAnyOrderElementsOf(hosts);
    }

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1));

        CassandraServer resolvedHost = cassandra.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost.proxy().getHostString()).isEqualTo(HOSTNAME_1);
        assertThat(resolvedHost.proxy().getPort()).isEqualTo(DEFAULT_PORT);
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1));

        CassandraServer resolvedHost = cassandra.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost.proxy().getHostString()).isEqualTo(HOSTNAME_1);
        assertThat(resolvedHost.proxy().getPort()).isEqualTo(DEFAULT_PORT);
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2));

        CassandraServer resolvedHost = cassandra.getAddressForHost(HOSTNAME_3);

        assertThat(resolvedHost.proxy().getHostString()).isEqualTo(HOSTNAME_3);
        assertThat(resolvedHost.proxy().getPort()).isEqualTo(DEFAULT_PORT);
    }

    @Test
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() {
        CassandraServer server2 = CassandraServer.of(InetSocketAddress.createUnresolved(HOSTNAME_2, OTHER_PORT));

        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, server2));

        assertThatThrownBy(() -> cassandra.getAddressForHost(HOSTNAME_3)).isInstanceOf(UnknownHostException.class);
    }

    @Test
    public void shouldReturnAbsentIfPredicateMatchesNoServers() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1));

        Optional<CassandraClientPoolingContainer> container = cassandra.getRandomGoodHostForPredicate(address -> false);
        assertThat(container).isNotPresent();
    }

    @Test
    public void shouldOnlyReturnHostsMatchingPredicate() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2));

        int numTrials = 50;
        for (int i = 0; i < numTrials; i++) {
            Optional<CassandraClientPoolingContainer> container =
                    cassandra.getRandomGoodHostForPredicate(address -> address.equals(SERVER_1));
            assertContainerHasHostOne(container);
        }
    }

    @Test
    public void shouldNotReturnHostsNotMatchingPredicateEvenWithNodeFailure() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2));
        blacklist.add(SERVER_1);
        Optional<CassandraClientPoolingContainer> container =
                cassandra.getRandomGoodHostForPredicate(address -> address.equals(SERVER_1));
        assertContainerHasHostOne(container);
    }

    @Test
    public void selectsHostsInAnotherDatacenter() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_2)), SERVER_1);
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_1)), SERVER_2);
    }

    @Test
    public void choosesTheHostInTheLeastAttemptedDatacenter() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2, SERVER_3, DC_1));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3)),
                SERVER_2);
    }

    @Test
    public void distributesAttemptsWhenMultipleDatacentersAreLeastAttempted() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2, SERVER_3, DC_3));
        Set<CassandraServer> suggestedHosts =
                getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of(SERVER_1));
        assertThat(suggestedHosts).containsExactlyInAnyOrder(SERVER_2, SERVER_3);
    }

    @Test
    public void selectsAnyHostIfAllDatacentersAlreadyTried() {
        ImmutableSet<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        Set<CassandraServer> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, allHosts);
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void selectsAnyHostIfNoDatacentersAlreadyTried() {
        ImmutableSet<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        Set<CassandraServer> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of());
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void selectsHostMatchingPredicateEvenIfRelatedHostsAlreadyTried() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2, SERVER_3, DC_1));

        assertThat(cassandra
                        .getRandomGoodHostForPredicate(address -> address.equals(SERVER_1), ImmutableSet.of(SERVER_1))
                        .map(CassandraClientPoolingContainer::getCassandraServer))
                .as("obeys the predicate even if this host was already tried")
                .hasValue(SERVER_1);
        assertThat(cassandra
                        .getRandomGoodHostForPredicate(address -> address.equals(SERVER_1), ImmutableSet.of(SERVER_3))
                        .map(CassandraClientPoolingContainer::getCassandraServer))
                .as("obeys the predicate even if another host in this datacenter was already tried")
                .hasValue(SERVER_1);
    }

    @Test
    public void selectsHostsWithUnknownDatacenterMappingIfAllKnownDatacentersTried() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_1, SERVER_2)),
                SERVER_3);
    }

    @Test
    public void selectsFromAllHostsIfDatacenterMappingNotAvailable() {
        Set<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of());
        Set<CassandraServer> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of());
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void getRandomHostByActiveConnectionsReturnsDesiredHost() {
        ImmutableSet<CassandraServer> servers = IntStream.range(0, 24)
                .mapToObj(i1 -> CassandraServer.of(InetSocketAddress.createUnresolved("10.0.0." + i1, DEFAULT_PORT)))
                .collect(ImmutableSet.toImmutableSet());
        try (CassandraService service = clientPoolWithParams(servers, servers, 1.0)) {
            service.setLocalHosts(servers.stream().limit(8).collect(ImmutableSet.toImmutableSet()));
            for (int i = 0; i < 500_000; i++) {
                // select some random nodes
                ImmutableSet<CassandraServer> desired = IntStream.generate(
                                () -> ThreadLocalRandom.current().nextInt(servers.size()))
                        .limit(3)
                        .mapToObj(i1 -> servers.asList().get(i1))
                        .collect(ImmutableSet.toImmutableSet());
                assertThat(service.getRandomHostByActiveConnections(desired))
                        .describedAs("Iteration %i - Expecting a node selected from desired: %s", i, desired)
                        .isPresent()
                        .get()
                        .satisfies(server -> assertThat(desired).contains(server));
            }
        }
    }

    @Test
    public void ports() throws Exception {
        assertThat(CassandraService.onlyPort(List.of(1, 1, 1).iterator())).isEqualTo(1);
        assertThat(CassandraService.onlyPort(
                        IntStream.generate(() -> 4224).boxed().limit(15).iterator()))
                .isEqualTo(4224);
        assertThatThrownBy(() -> CassandraService.onlyPort(List.of(1, 2, 1).iterator()))
                .isInstanceOf(UnknownHostException.class)
                .hasMessageContaining("No single known port");
    }

    private Set<CassandraServer> getRecommendedHostsFromAThousandTrials(
            CassandraService cassandra, Set<CassandraServer> hosts) {
        return IntStream.range(0, 1_000)
                .mapToObj(attempt -> cassandra.getRandomGoodHostForPredicate(address -> true, hosts))
                .flatMap(Optional::stream)
                .map(CassandraClientPoolingContainer::getCassandraServer)
                .collect(Collectors.toSet());
    }

    private void assertContainerHasHostOne(Optional<CassandraClientPoolingContainer> container) {
        assertContainerHasHost(container, SERVER_1);
    }

    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "ConstantConditions"})
    private void assertContainerHasHost(Optional<CassandraClientPoolingContainer> container, CassandraServer host) {
        assertThat(container).isPresent();
        assertThat(container.get().getCassandraServer()).isEqualTo(host);
    }

    private CassandraService clientPoolWithServers(Set<CassandraServer> servers) {
        return clientPoolWith(servers, servers);
    }

    private CassandraService clientPoolWithServersAndParams(Set<CassandraServer> servers, double weighting) {
        return clientPoolWithParams(servers, servers, weighting);
    }

    private CassandraService clientPoolWith(Set<CassandraServer> servers, Set<CassandraServer> serversInPool) {
        return clientPoolWithParams(servers, serversInPool, 0.0);
    }

    private CassandraService clientPoolWithParams(
            Set<CassandraServer> servers, Set<CassandraServer> serversInPool, double weighting) {
        config = ImmutableCassandraKeyValueServiceConfig.builder()
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username("username")
                        .password("password")
                        .build())
                .localHostWeighting(weighting)
                .consecutiveAbsencesBeforePoolRemoval(1)
                .keyspace("ks")
                .build();
        Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig =
                Refreshable.only(ImmutableCassandraKeyValueServiceRuntimeConfig.builder()
                        .servers(ImmutableDefaultConfig.builder()
                                .addAllThriftHosts(servers.stream()
                                        .map(CassandraServer::proxy)
                                        .collect(Collectors.toSet()))
                                .build())
                        .replicationFactor(3)
                        .build());

        blacklist = new Blacklist(
                config, runtimeConfig.map(CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds));

        MetricsManager metricsManager = MetricsManagers.createForTests();
        CassandraService service = new CassandraService(
                metricsManager, config, runtimeConfig, blacklist, new CassandraClientPoolMetrics(metricsManager));

        service.cacheInitialCassandraHosts();
        serversInPool.forEach(service::addPool);

        return service;
    }
}
