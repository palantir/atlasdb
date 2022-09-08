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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;
import org.immutables.value.Value;
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
    private static final String RACK_0 = "rack0";
    private static final String RACK_1 = "rack1";
    private static final String RACK_2 = "rack2";

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
        cassandra.blacklist().add(SERVER_1);
        Optional<CassandraClientPoolingContainer> container =
                cassandra.getRandomGoodHostForPredicate(address -> address.equals(SERVER_1));
        assertContainerHasHostOne(container);
    }

    @Test
    public void selectsHostsInAnotherDatacenter() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2));
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_2)), SERVER_1);
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_1)), SERVER_2);
    }

    @Test
    public void choosesTheHostInTheLeastAttemptedDatacenter() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2, SERVER_3, DC_1));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3)),
                SERVER_2);
    }

    @Test
    public void distributesAttemptsWhenMultipleDatacentersAreLeastAttempted() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2, SERVER_3, DC_3));
        Set<CassandraServer> suggestedHosts =
                getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of(SERVER_1));
        assertThat(suggestedHosts).containsExactlyInAnyOrder(SERVER_2, SERVER_3);
    }

    @Test
    public void selectsAnyHostIfAllDatacentersAlreadyTried() {
        ImmutableSet<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        Set<CassandraServer> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, allHosts);
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void selectsAnyHostIfNoDatacentersAlreadyTried() {
        ImmutableSet<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        Set<CassandraServer> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of());
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void selectsHostMatchingPredicateEvenIfRelatedHostsAlreadyTried() {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3));
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2, SERVER_3, DC_1));

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
        cassandra.setHostToDatacenterMapping(ImmutableMap.of(SERVER_1, DC_1, SERVER_2, DC_2));
        assertContainerHasHost(
                cassandra.getRandomGoodHostForPredicate(address -> true, ImmutableSet.of(SERVER_1, SERVER_2)),
                SERVER_3);
    }

    @Test
    public void selectsFromAllHostsIfDatacenterMappingNotAvailable() {
        Set<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        cassandra.setHostToDatacenterMapping(ImmutableMap.of());
        Set<CassandraServer> suggestedHosts = getRecommendedHostsFromAThousandTrials(cassandra, ImmutableSet.of());
        assertThat(suggestedHosts).containsExactlyInAnyOrderElementsOf(allHosts);
    }

    @Test
    public void getRandomHostByActiveConnectionsReturnsDesiredHost() {
        ImmutableSet<CassandraServer> servers = IntStream.range(0, 24)
                .mapToObj(i1 -> CassandraServer.of(InetSocketAddress.createUnresolved("10.0.0." + i1, DEFAULT_PORT)))
                .collect(ImmutableSet.toImmutableSet());
        CassandraService service = clientPoolWithParams(servers, servers, 1.0);
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

    @Test
    public void testSingleServerGetCassandraServers() throws Exception {
        Set<CassandraServer> allHosts = ImmutableSet.of(SERVER_1);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        TokenRange tokens = new TokenRange();
        tokens.setEndpoints(
                allHosts.stream().map(CassandraServer::cassandraHostName).collect(Collectors.toList()));
        tokens.setEndpoint_details(ImmutableList.of(endpointDetails(HOSTNAME_1, DC_1, RACK_1)));
        List<TokenRange> tokenRanges = ImmutableList.of(tokens);
        ImmutableSet<CassandraServer> servers = cassandra.getCassandraServers(tokenRanges);
        assertThat(servers).hasSize(1).first().satisfies(cs -> {
            assertThat(cs.cassandraHostName()).isEqualTo(HOSTNAME_1);
            assertThat(cs.reachableProxyIps()).hasSize(1);
        });
    }

    @Test
    public void testMultiServerGetCassandraServers() throws Exception {
        Set<CassandraServer> allHosts = ImmutableSet.of(SERVER_1, SERVER_2, SERVER_3);
        CassandraService cassandra = clientPoolWithServers(allHosts);
        AtomicInteger rack = new AtomicInteger();
        AtomicInteger token = new AtomicInteger();
        List<TokenRange> tokenRanges = allHosts.stream()
                .map(cass -> {
                    String start = BaseEncoding.base16().encode(Ints.toByteArray(token.get()));
                    String end = BaseEncoding.base16().encode(Ints.toByteArray(token.incrementAndGet()));
                    return tokenRange(
                            start,
                            end,
                            endpointDetails(cass.cassandraHostName(), DC_1, "rack" + rack.incrementAndGet()),
                            ImmutableList.of(cass.cassandraHostName()));
                })
                .collect(ImmutableList.toImmutableList());
        assertThat(cassandra.getCassandraServers(tokenRanges)).hasSize(3);
    }

    @Test
    public void testComplexMultiDatacenterMultiRackServerGetCassandraServers() throws Exception {
        int replicas = 3;
        int partitions = 5;
        int nodeCount = replicas * partitions + 1;
        assertThat(nodeCount).isLessThan(255);
        Set<CassandraServer> nodes = IntStream.range(1, nodeCount)
                .mapToObj(i -> "10." + i + ".0.0")
                .map(ip -> InetSocketAddress.createUnresolved(ip, DEFAULT_PORT))
                .map(CassandraServer::of)
                .collect(Collectors.toSet());

        CassandraService cassandra = clientPoolWithServers(nodes);

        Map<CassandraServer, String> nodeToRack = nodes.stream().collect(Collectors.toMap(Function.identity(), node -> {
            String hostname = node.cassandraHostName();
            assertThat(hostname).startsWith("10.").endsWith(".0.0");
            int dotIndex = hostname.indexOf('.', 3);
            assertThat(dotIndex).isGreaterThan(0);
            String substring = hostname.substring(3, dotIndex);
            assertThat(substring).isNotEmpty();
            int id = Integer.parseInt(substring);
            assertThat(id).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(replicas * partitions);
            int rack = id % replicas;
            assertThat(rack).isGreaterThanOrEqualTo(0).isLessThan(replicas);
            return "rack" + rack;
        }));
        assertThat(nodeToRack).containsOnlyKeys(nodes).hasSize(nodes.size());
        assertThat(nodeToRack.values()).hasSize(15).containsOnly(RACK_0, RACK_1, RACK_2);

        AtomicInteger token = new AtomicInteger();
        List<Node> endpointNodes = nodeToRack.entrySet().stream()
                .map(e -> {
                    int min = token.getAndIncrement() % partitions;
                    int max = (min + 1) % partitions;
                    return ImmutableNode.builder()
                            .start(min)
                            .end(max)
                            .server(e.getKey())
                            .rack(e.getValue())
                            .build();
                })
                .collect(Collectors.toList());

        List<TokenRange> tokenRanges = Multimaps.index(endpointNodes, Node::start).asMap().entrySet().stream()
                .map(e -> {
                    Integer start = e.getKey();
                    String begin = BaseEncoding.base16().encode(Ints.toByteArray(start));
                    String end = BaseEncoding.base16().encode(Ints.toByteArray(start + 1));

                    List<EndpointDetails> endpointDetails = e.getValue().stream()
                            .map(n -> endpointDetails(n.server().cassandraHostName(), DC_1, n.rack()))
                            .collect(Collectors.toList());

                    List<String> endpoints = nodes.stream()
                            .map(CassandraServer::cassandraHostName)
                            .collect(Collectors.toList());
                    TokenRange tokens = new TokenRange(begin, end, endpoints);
                    tokens.setEndpoint_details(endpointDetails);
                    return tokens;
                })
                .collect(Collectors.toList());

        assertThat(cassandra.getCassandraServers(tokenRanges)).hasSize(nodes.size());
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(0, 1))).hasSize(replicas);
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(0, 2))).hasSize(2 * replicas);
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(0, 3))).hasSize(3 * replicas);
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(1, 3))).hasSize(2 * replicas);
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(2, 4))).hasSize(2 * replicas);
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(3, 5))).hasSize(2 * replicas);
        assertThat(cassandra.getCassandraServers(tokenRanges.subList(4, 5))).hasSize(replicas);

        for (int i = 0; i < partitions; i++) {
            for (int j = i + 1; j <= partitions; j++) {
                for (int k = 1; k <= replicas; k++) {
                    List<TokenRange> ranges = tokenRanges.subList(i, j);
                    assertThat(cassandra.getCassandraServers(ranges))
                            .as("(%d, %d, %d) servers for ranges %s", i, j, k, ranges)
                            .hasSize((j - i) * replicas);
                }
            }
        }
    }

    @Value.Immutable
    interface Node {
        int start();

        int end();

        CassandraServer server();

        @Value.Default
        default String dc() {
            return DC_1;
        }

        String rack();
    }

    private static TokenRange tokenRange(String start, String end, EndpointDetails details, List<String> endpoints) {
        TokenRange tokens = new TokenRange(start, end, endpoints);
        tokens.setEndpoint_details(ImmutableList.of(details));
        return tokens;
    }

    private static EndpointDetails endpointDetails(String host, String datacenter, String rack) {
        EndpointDetails endpointDetails = new EndpointDetails(host, datacenter);
        endpointDetails.setRack(rack);
        return endpointDetails;
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
        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
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

        Blacklist blacklist = new Blacklist(
                config, runtimeConfig.map(CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds));

        MetricsManager metricsManager = MetricsManagers.createForTests();
        Supplier<Optional<HostLocation>> myLocationSupplier =
                () -> servers.stream().findFirst().map(cs -> HostLocation.of(DC_1, RACK_0));
        Supplier<Map<String, String>> hostnamesByIpSupplier = () -> servers.stream()
                .collect(Collectors.toMap(CassandraServer::cassandraHostName, CassandraServer::cassandraHostName));
        CassandraService service = new CassandraService(
                metricsManager,
                config,
                runtimeConfig,
                blacklist,
                new CassandraClientPoolMetrics(metricsManager),
                myLocationSupplier,
                hostnamesByIpSupplier);

        service.cacheInitialCassandraHosts();
        serversInPool.forEach(service::addPool);

        return service;
    }
}
