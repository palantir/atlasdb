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
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Set;
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
    private static final String DC_1 = "london";
    private static final String DC_2 = "singapore";
    private static final String DC_3 = "zurich";

    private CassandraKeyValueServiceConfig config;
    private Blacklist blacklist;

    @Test
    public void shouldOnlyReturnLocalHosts() {
        ImmutableSet<InetSocketAddress> hosts = ImmutableSet.of(HOST_1, HOST_2);
        ImmutableSet<InetSocketAddress> localHosts = ImmutableSet.of(HOST_1);

        CassandraService cassandra = clientPoolWithServersAndParams(hosts, 1.0);

        cassandra.setLocalHosts(localHosts);

        assertThat(cassandra.maybeFilterLocalHosts(hosts)).isEqualTo(localHosts);
    }

    @Test
    public void shouldReturnAllHostsBySkippingFilter() {
        ImmutableSet<InetSocketAddress> hosts = ImmutableSet.of(HOST_1, HOST_2);
        ImmutableSet<InetSocketAddress> localHosts = ImmutableSet.of(HOST_1);

        CassandraService cassandra = clientPoolWithServersAndParams(hosts, 0.0);

        cassandra.setLocalHosts(localHosts);

        assertThat(cassandra.maybeFilterLocalHosts(hosts)).isEqualTo(hosts);
    }

    @Test
    public void shouldReturnAllHostsAsNoIntersection() {
        ImmutableSet<InetSocketAddress> hosts = ImmutableSet.of(HOST_1, HOST_2);
        ImmutableSet<InetSocketAddress> localHosts = ImmutableSet.of();

        CassandraService cassandra = clientPoolWithServersAndParams(hosts, 0.0);

        cassandra.setLocalHosts(localHosts);

        assertThat(cassandra.maybeFilterLocalHosts(hosts)).isEqualTo(hosts);
    }

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1));

        InetSocketAddress resolvedHost = cassandra.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost.getHostString()).isEqualTo(HOSTNAME_1);
        assertThat(resolvedHost.getPort()).isEqualTo(DEFAULT_PORT);
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1));

        InetSocketAddress resolvedHost = cassandra.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost.getHostString()).isEqualTo(HOSTNAME_1);
        assertThat(resolvedHost.getPort()).isEqualTo(DEFAULT_PORT);
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2));

        InetSocketAddress resolvedHost = cassandra.getAddressForHost(HOSTNAME_3);

        assertThat(resolvedHost.getHostString()).isEqualTo(HOSTNAME_3);
        assertThat(resolvedHost.getPort()).isEqualTo(DEFAULT_PORT);
    }

    @Test
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() throws UnknownHostException {
        InetSocketAddress host2 = InetSocketAddress.createUnresolved(HOSTNAME_2, OTHER_PORT);

        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, host2));

        assertThatThrownBy(() -> cassandra.getAddressForHost(HOSTNAME_3)).isInstanceOf(UnknownHostException.class);
    }

    @Test
    public void shouldReturnAbsentIfPredicateMatchesNoServers() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1));

        Optional<CassandraClientPoolingContainer> container = cassandra.getRandomGoodHostForPredicate(address -> false);
        assertThat(container).isNotPresent();
    }

    @Test
    public void shouldOnlyReturnHostsMatchingPredicate() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2));

        int numTrials = 50;
        for (int i = 0; i < numTrials; i++) {
            Optional<CassandraClientPoolingContainer> container =
                    cassandra.getRandomGoodHostForPredicate(address -> address.equals(HOST_1));
            assertContainerHasHostOne(container);
        }
    }

    @Test
    public void shouldNotReturnHostsNotMatchingPredicateEvenWithNodeFailure() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2));
        blacklist.add(HOST_1);
        Optional<CassandraClientPoolingContainer> container =
                cassandra.getRandomGoodHostForPredicate(address -> address.equals(HOST_1));
        assertContainerHasHostOne(container);
    }

    @Test
    public void selectsHostsInAnotherDatacenter() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(HOST_2)), HOST_1);
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(HOST_1)), HOST_2);
    }

    @Test
    public void choosesTheHostInTheLeastAttemptedDatacenter() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2, HOST_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2, HOST_3, DC_1));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(HOST_1, HOST_2, HOST_3)),
                HOST_2);
    }

    @Test
    public void distributesAttemptsWhenMultipleDatacentersAreLeastAttempted() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2, HOST_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2, HOST_3, DC_3));
        Set<InetSocketAddress> suggestedHosts =
                getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of(HOST_1));
        assertThat(suggestedHosts).containsExactlyInAnyOrder(HOST_2, HOST_3);
    }

    @Test
    public void selectsAnyHostIfAllDatacentersAlreadyTried() {
        ImmutableSet<InetSocketAddress> allHosts = ImmutableSet.of(HOST_1, HOST_2);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2));
        Set<InetSocketAddress> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, allHosts);
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void selectsAnyHostIfNoDatacentersAlreadyTried() {
        ImmutableSet<InetSocketAddress> allHosts = ImmutableSet.of(HOST_1, HOST_2);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2));
        Set<InetSocketAddress> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of());
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void selectsHostMatchingPredicateEvenIfRelatedHostsAlreadyTried() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2, HOST_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2, HOST_3, DC_1));

        assertThat(cassandra
                        .getRandomGoodHostForPredicate(address -> address.equals(HOST_1), ImmutableSet.of(HOST_1))
                        .map(CassandraClientPoolingContainer::getHost))
                .as("obeys the predicate even if this host was already tried")
                .hasValue(HOST_1);
        assertThat(cassandra
                        .getRandomGoodHostForPredicate(address -> address.equals(HOST_1), ImmutableSet.of(HOST_3))
                        .map(CassandraClientPoolingContainer::getHost))
                .as("obeys the predicate even if another host in this datacenter was already tried")
                .hasValue(HOST_1);
    }

    @Test
    public void selectsHostsWithUnknownDatacenterMappingIfAllKnownDatacentersTried() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2, HOST_3));
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of(HOST_1, DC_1, HOST_2, DC_2));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(HOST_1, HOST_2)), HOST_3);
    }

    @Test
    public void selectsFromAllHostsIfDatacenterMappingNotAvailable() {
        ImmutableSet<InetSocketAddress> allHosts = ImmutableSet.of(HOST_1, HOST_2, HOST_3);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.overrideHostToDatacenterMapping(ImmutableMap.of());
        Set<InetSocketAddress> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of());
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    private Set<InetSocketAddress> getRecommendedHostsFromAThousandTrials(
            CassandraService cassandra, Set<InetSocketAddress> hosts) {
        return IntStream.range(0, 1_000)
                .mapToObj(attempt -> cassandra.getRandomGoodHostForPredicate(address -> true, hosts))
                .flatMap(Optional::stream)
                .map(CassandraClientPoolingContainer::getHost)
                .collect(Collectors.toSet());
    }

    private void assertContainerHasHostOne(Optional<CassandraClientPoolingContainer> container) {
        assertContainerHasHost(container, HOST_1);
    }

    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "ConstantConditions"})
    private void assertContainerHasHost(Optional<CassandraClientPoolingContainer> container, InetSocketAddress host) {
        assertThat(container).isPresent();
        assertThat(container.get().getHost()).isEqualTo(host);
    }

    private CassandraService clientPoolWithServers(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(servers, servers);
    }

    private CassandraService clientPoolWithServersAndParams(ImmutableSet<InetSocketAddress> servers, double weighting) {
        return clientPoolWithParams(servers, servers, weighting);
    }

    private CassandraService clientPoolWith(
            ImmutableSet<InetSocketAddress> servers, ImmutableSet<InetSocketAddress> serversInPool) {
        return clientPoolWithParams(servers, serversInPool, 0.0);
    }

    private CassandraService clientPoolWithParams(
            ImmutableSet<InetSocketAddress> servers, ImmutableSet<InetSocketAddress> serversInPool, double weighting) {
        config = ImmutableCassandraKeyValueServiceConfig.builder()
                .replicationFactor(3)
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username("username")
                        .password("password")
                        .build())
                .servers(ImmutableDefaultConfig.builder()
                        .addAllThriftHosts(servers)
                        .build())
                .localHostWeighting(weighting)
                .build();

        blacklist = new Blacklist(config);

        MetricsManager metricsManager = MetricsManagers.createForTests();
        CassandraService service =
                new CassandraService(metricsManager, config, blacklist, new CassandraClientPoolMetrics(metricsManager));

        service.cacheInitialCassandraHosts();
        serversInPool.forEach(service::addPool);

        return service;
    }
}
