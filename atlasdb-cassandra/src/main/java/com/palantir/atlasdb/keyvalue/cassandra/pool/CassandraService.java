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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraLogHelper;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraNodeIdentifier;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraUtils;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;

public class CassandraService implements AutoCloseable {
    // TODO(tboam): keep logging on old class?
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraService.class);
    private static final Interner<RangeMap<LightweightOppToken, List<CassandraNodeIdentifier>>> tokensInterner =
            Interners.newWeakInterner();

    private final MetricsManager metricsManager;
    private final CassandraKeyValueServiceConfig config;
    private final Blacklist blacklist;
    private final CassandraClientPoolMetrics poolMetrics;

    private volatile RangeMap<LightweightOppToken, List<CassandraNodeIdentifier>> tokenMap = ImmutableRangeMap.of();
    private final Map<CassandraNodeIdentifier, CassandraClientPoolingContainer> currentPools =
            new ConcurrentHashMap<>();
    private volatile Map<CassandraNodeIdentifier, String> hostToDatacenter = ImmutableMap.of();

    private List<CassandraNodeIdentifier> cassandraHosts;

    private volatile Set<CassandraNodeIdentifier> localHosts = ImmutableSet.of();
    private final Supplier<Optional<HostLocation>> myLocationSupplier;
    private final Supplier<Map<String, String>> hostnameByIpSupplier;

    private final Random random = new Random();

    public CassandraService(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Blacklist blacklist,
            CassandraClientPoolMetrics poolMetrics) {
        this.metricsManager = metricsManager;
        this.config = config;
        this.myLocationSupplier =
                AsyncSupplier.create(HostLocationSupplier.create(this::getSnitch, config.overrideHostLocation()));
        this.blacklist = blacklist;
        this.poolMetrics = poolMetrics;

        Supplier<Map<String, String>> hostnamesByIpSupplier = new HostnamesByIpSupplier(this::getRandomGoodHost);
        this.hostnameByIpSupplier = Suppliers.memoizeWithExpiration(hostnamesByIpSupplier::get, 2, TimeUnit.MINUTES);
    }

    public static CassandraService createInitialized(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Blacklist blacklist,
            CassandraClientPoolMetrics poolMetrics) {
        CassandraService cassandraService = new CassandraService(metricsManager, config, blacklist, poolMetrics);
        cassandraService.cacheInitialCassandraHosts();
        cassandraService.refreshTokenRangesAndGetServers();
        return cassandraService;
    }

    @Override
    public void close() {}

    // Todo(snanda): why am i changing this method? For `getAddressForHost`
    public Set<CassandraNodeIdentifier> refreshTokenRangesAndGetServers() {
        Set<CassandraNodeIdentifier> servers = new HashSet<>();
        Map<CassandraNodeIdentifier, String> hostToDatacentersThisRefresh = new HashMap<>();

        try {
            ImmutableRangeMap.Builder<LightweightOppToken, List<CassandraNodeIdentifier>> newTokenRing =
                    ImmutableRangeMap.builder();

            // grab latest token ring view from a random node in the cluster and update local hosts
            List<TokenRange> tokenRanges = getTokenRanges();
            localHosts = refreshLocalHosts(tokenRanges);

            // RangeMap needs a little help with weird 1-node, 1-vnode, this-entire-feature-is-useless case
            if (tokenRanges.size() == 1) {
                EndpointDetails onlyEndpoint = Iterables.getOnlyElement(
                        Iterables.getOnlyElement(tokenRanges).getEndpoint_details());
                CassandraNodeIdentifier onlyHost = getAddressForHost(onlyEndpoint.getHost());
                newTokenRing.put(Range.all(), ImmutableList.of(onlyHost));
                servers.add(onlyHost);
                hostToDatacentersThisRefresh.put(onlyHost, onlyEndpoint.getDatacenter());
            } else { // normal case, large cluster with many vnodes
                for (TokenRange tokenRange : tokenRanges) {
                    Map<CassandraNodeIdentifier, String> hostToDatacentersOnThisTokenRange = KeyedStream.of(
                                    tokenRange.getEndpoint_details())
                            .mapKeys(EndpointDetails::getHost)
                            .mapKeys(this::getNodeIdForHostThrowUnchecked)
                            .map(EndpointDetails::getDatacenter)
                            .collectToMap();

                    List<CassandraNodeIdentifier> hosts = new ArrayList<>(hostToDatacentersOnThisTokenRange.keySet());
                    servers.addAll(hosts);
                    hostToDatacentersThisRefresh.putAll(hostToDatacentersOnThisTokenRange);

                    LightweightOppToken startToken = new LightweightOppToken(BaseEncoding.base16()
                            .decode(tokenRange.getStart_token().toUpperCase()));
                    LightweightOppToken endToken = new LightweightOppToken(BaseEncoding.base16()
                            .decode(tokenRange.getEnd_token().toUpperCase()));
                    if (startToken.compareTo(endToken) <= 0) {
                        newTokenRing.put(Range.openClosed(startToken, endToken), hosts);
                    } else {
                        // Handle wrap-around
                        newTokenRing.put(Range.greaterThan(startToken), hosts);
                        newTokenRing.put(Range.atMost(endToken), hosts);
                    }
                }
            }
            tokenMap = tokensInterner.intern(newTokenRing.build());
            logHostToDatacenterMapping(hostToDatacentersThisRefresh);
            hostToDatacenter = hostToDatacentersThisRefresh;
            return servers;
        } catch (Exception e) {
            log.info(
                    "Couldn't grab new token ranges for token aware cassandra mapping. We will retry in {} seconds.",
                    SafeArg.of("poolRefreshIntervalSeconds", config.poolRefreshIntervalSeconds()),
                    e);

            // Attempt to re-resolve addresses from the configuration; this is important owing to certain race
            // conditions where the entire pool becomes invalid between refreshes.
            Set<CassandraNodeIdentifier> resolvedConfigAddresses = getServersFromConfig();

            Set<CassandraNodeIdentifier> lastKnownAddresses = tokenMap.asMapOfRanges().values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            return Sets.union(resolvedConfigAddresses, lastKnownAddresses);
        }
    }

    private void logHostToDatacenterMapping(Map<CassandraNodeIdentifier, String> hostToDatacentersThisRefresh) {
        if (log.isDebugEnabled()) {
            Map<String, String> hostAddressToDatacenter = KeyedStream.stream(hostToDatacentersThisRefresh)
                    .mapKeys(inetSocketAddress ->
                            inetSocketAddress.getLastUsedIpAddress().getHostAddress())
                    .collectToMap();
            log.debug(
                    "Logging host -> datacenter mapping following a refresh",
                    SafeArg.of("hostAddressToDatacenter", hostAddressToDatacenter));
        }
    }

    private List<TokenRange> getTokenRanges() throws Exception {
        return getRandomGoodHost().runWithPooledResource(CassandraUtils.getDescribeRing(config));
    }

    private String getSnitch() {
        try {
            return getRandomGoodHost()
                    .runWithPooledResource((FunctionCheckedException<CassandraClient, String, Exception>)
                            CassandraClient::describe_snitch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Set<CassandraNodeIdentifier> refreshLocalHosts(List<TokenRange> tokenRanges) {
        Optional<HostLocation> myLocation = myLocationSupplier.get();

        if (!myLocation.isPresent()) {
            return ImmutableSet.of();
        }

        Set<CassandraNodeIdentifier> newLocalHosts = tokenRanges.stream()
                .map(TokenRange::getEndpoint_details)
                .flatMap(Collection::stream)
                .filter(details -> isHostLocal(details, myLocation.get()))
                .map(EndpointDetails::getHost)
                .map(this::getNodeIdForHostThrowUnchecked)
                .collect(Collectors.toSet());

        if (newLocalHosts.isEmpty()) {
            log.warn("No local hosts found");
        }

        return newLocalHosts;
    }

    private static boolean isHostLocal(EndpointDetails details, HostLocation myLocation) {
        return details.isSetDatacenter()
                && details.isSetRack()
                && details.isSetHost()
                && myLocation.isProbablySameRackAs(details.getDatacenter(), details.getRack());
    }

    @VisibleForTesting
    void setLocalHosts(Set<CassandraNodeIdentifier> localHosts) {
        this.localHosts = localHosts;
    }

    public Set<CassandraNodeIdentifier> getLocalHosts() {
        return localHosts;
    }

    private CassandraNodeIdentifier getNodeIdForHostThrowUnchecked(String host) {
        try {
            return getAddressForHost(host);
        } catch (UnknownHostException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    // todo(snanda): this is so dicey
    @VisibleForTesting
    // this arg could be either - mostly it is a hostname
    // this is a best effort situation
    CassandraNodeIdentifier getAddressForHost(String inputHost) throws UnknownHostException {
        if (config.addressTranslation().containsKey(inputHost)) {
            return CassandraNodeIdentifier.from(config.addressTranslation().get(inputHost));
        }

        // we use a random host to basically get this mapping i.e. the input host could be an IP address?
        Map<String, String> hostnamesByIp = hostnameByIpSupplier.get();
        String host = hostnamesByIp.getOrDefault(inputHost, inputHost);

        // if two things have equal ip then return that InetSocketAddress
        InetAddress resolvedHost = InetAddress.getByName(host);
        Set<CassandraNodeIdentifier> allKnownHosts = Sets.union(currentPools.keySet(), getServersFromConfig());

        for (CassandraNodeIdentifier nodeIdentifier : allKnownHosts) {
            if (nodeIdentifier.recognizes(resolvedHost)) {
                // todo(snanda): is this a good time to add?
                nodeIdentifier.addAddress(resolvedHost);
                return nodeIdentifier;
            }
        }

        Set<Integer> allKnownPorts =
                allKnownHosts.stream().map(CassandraNodeIdentifier::getPort).collect(Collectors.toSet());

        // Todo(snanda) what this even mean?
        if (allKnownPorts.size() == 1) { // if everyone is on one port, try and use that
            return CassandraNodeIdentifier.create(resolvedHost, Iterables.getOnlyElement(allKnownPorts));
        } else {
            throw new UnknownHostException("Couldn't find the provided host in server list or current servers");
        }
    }

    private List<CassandraNodeIdentifier> getHostsFor(byte[] key) {
        return tokenMap.get(new LightweightOppToken(key));
    }

    public Optional<CassandraClientPoolingContainer> getRandomGoodHostForPredicate(
            Predicate<CassandraNodeIdentifier> predicate) {
        return getRandomGoodHostForPredicate(predicate, ImmutableSet.of());
    }

    // Todo(snanda): this accounts for most recently used InetAddress for a host - `getHost`
    public Optional<CassandraClientPoolingContainer> getRandomGoodHostForPredicate(
            Predicate<CassandraNodeIdentifier> predicate, Set<CassandraNodeIdentifier> triedHosts) {
        Map<CassandraNodeIdentifier, CassandraClientPoolingContainer> pools = getNodeIds();

        Set<CassandraNodeIdentifier> nodesMatchingPredicate =
                pools.keySet().stream().filter(predicate).collect(Collectors.toSet());
        Map<String, Long> triedDatacenters = triedHosts.stream()
                .map(hostToDatacenter::get)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Optional<Long> maximumAttemptsPerDatacenter =
                triedDatacenters.values().stream().max(Long::compareTo);
        Set<String> maximallyAttemptedDatacenters = KeyedStream.stream(triedDatacenters)
                .filter(attempts -> Objects.equals(
                        attempts,
                        maximumAttemptsPerDatacenter.orElseThrow(() -> new SafeIllegalStateException(
                                "Unexpectedly could not find the max attempts per datacenter"))))
                .keys()
                .collect(Collectors.toSet());

        Set<CassandraNodeIdentifier> nodesInPermittedDatacenters = nodesMatchingPredicate.stream()
                .filter(pool -> {
                    String datacenter = hostToDatacenter.get(pool);
                    return datacenter == null || !maximallyAttemptedDatacenters.contains(datacenter);
                })
                .collect(Collectors.toSet());
        Set<CassandraNodeIdentifier> filteredNodes =
                nodesInPermittedDatacenters.isEmpty() ? nodesMatchingPredicate : nodesInPermittedDatacenters;

        if (filteredNodes.isEmpty()) {
            log.info("No hosts match the provided predicate.");
            return Optional.empty();
        }

        Set<CassandraNodeIdentifier> livingNodes = blacklist.filterBlacklistedHostsFrom(filteredNodes);
        if (livingNodes.isEmpty()) {
            log.info("There are no known live hosts in the connection pool matching the predicate. We're choosing"
                    + " one at random in a last-ditch attempt at forward progress.");
            livingNodes = filteredNodes;
        }

        Optional<CassandraNodeIdentifier> randomLivingHost = getRandomHostByActiveConnections(livingNodes);
        return randomLivingHost.map(pools::get);
    }

    public CassandraClientPoolingContainer getRandomGoodHost() {
        return getRandomGoodHostForPredicate(_address -> true)
                .orElseThrow(() -> new SafeIllegalStateException("No hosts available."));
    }

    private String getRingViewDescription() {
        return CassandraLogHelper.tokenMap(tokenMap).toString();
    }

    public RangeMap<LightweightOppToken, List<CassandraNodeIdentifier>> getTokenMap() {
        return tokenMap;
    }

    // todo(snanda): how we feel about concurrency
    public Map<InetSocketAddress, CassandraClientPoolingContainer> getPools() {
        // todo(snanda): is it okay to not expose the CassandraNodeId situation
        return KeyedStream.stream(currentPools)
                .mapKeys(cassandraNodeIdentifier -> new InetSocketAddress(
                        cassandraNodeIdentifier.getLastUsedIpAddress(), cassandraNodeIdentifier.getPort()))
                .collectToMap();
    }

    public Map<CassandraNodeIdentifier, CassandraClientPoolingContainer> getNodeIds() {
        return currentPools;
    }

    @VisibleForTesting
    Set<CassandraNodeIdentifier> maybeFilterLocalHosts(Set<CassandraNodeIdentifier> nodes) {
        if (random.nextDouble() < config.localHostWeighting()) {
            Set<CassandraNodeIdentifier> localFilteredHosts = Sets.intersection(localHosts, nodes);
            if (!localFilteredHosts.isEmpty()) {
                return localFilteredHosts;
            }
        }

        return nodes;
    }

    // Todo(snanda):  we are still considering each node identifier as one and not retrying on its other known addresses
    private Optional<CassandraNodeIdentifier> getRandomHostByActiveConnections(
            Set<CassandraNodeIdentifier> desiredNodes) {
        Set<CassandraNodeIdentifier> localFilteredHosts = maybeFilterLocalHosts(desiredNodes);

        Map<CassandraNodeIdentifier, CassandraClientPoolingContainer> matchingPools = KeyedStream.stream(
                        ImmutableMap.copyOf(currentPools))
                .filterKeys(localFilteredHosts::contains)
                .collectToMap();
        if (matchingPools.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(WeightedHosts.create(matchingPools).getRandomNodeIdentifier());
    }

    public void debugLogStateOfPool() {
        if (log.isDebugEnabled()) {
            StringBuilder currentState = new StringBuilder();
            currentState.append(String.format(
                    "POOL STATUS: Current blacklist = %s,%n current hosts in pool = %s%n",
                    blacklist.describeBlacklistedHosts(), currentPools.keySet().toString()));
            for (Entry<CassandraNodeIdentifier, CassandraClientPoolingContainer> entry : currentPools.entrySet()) {
                int activeCheckouts = entry.getValue().getActiveCheckouts();
                int totalAllowed = entry.getValue().getPoolSize();

                currentState.append(String.format(
                        "\tPOOL STATUS: Pooled host %s has %s out of %s connections checked out.%n",
                        entry.getKey(),
                        activeCheckouts > 0 ? Integer.toString(activeCheckouts) : "(unknown)",
                        totalAllowed > 0 ? Integer.toString(totalAllowed) : "(not bounded)"));
            }
            log.debug("Current pool state: {}", UnsafeArg.of("currentState", currentState.toString()));
        }
    }

    public CassandraNodeIdentifier getRandomNodeIdentifierForKey(byte[] key) {
        List<CassandraNodeIdentifier> hostsForKey = getHostsFor(key);

        if (hostsForKey == null) {
            if (config.autoRefreshNodes()) {
                log.info("We attempted to route your query to a cassandra host that already contains the relevant data."
                        + " However, the mapping of which host contains which data is not available yet."
                        + " We will choose a random host instead.");
            }
            return getRandomGoodHost().getCassandraNodeIdentifier();
        }

        Set<CassandraNodeIdentifier> liveOwnerHosts = blacklist.filterBlacklistedHostsFrom(hostsForKey);

        if (!liveOwnerHosts.isEmpty()) {
            Optional<CassandraNodeIdentifier> activeHost = getRandomHostByActiveConnections(liveOwnerHosts);
            if (activeHost.isPresent()) {
                return activeHost.get();
            }
        }

        log.warn(
                "Perf / cluster stability issue. Token aware query routing has failed because there are no known "
                        + "live hosts that claim ownership of the given range."
                        + " Falling back to choosing a random live node."
                        + " Current host blacklist is {}."
                        + " Current state logged at TRACE",
                SafeArg.of("blacklistedHosts", blacklist.blacklistDetails()));
        log.trace("Current ring view is: {}.", SafeArg.of("tokenMap", getRingViewDescription()));
        return getRandomGoodHost().getCassandraNodeIdentifier();
    }

    public void addPool(CassandraNodeIdentifier server) {
        int currentPoolNumber = cassandraHosts.indexOf(server) + 1;
        addPoolInternal(
                server,
                new CassandraClientPoolingContainer(metricsManager, server, config, currentPoolNumber, poolMetrics));
    }

    public void returnOrCreatePool(
            CassandraNodeIdentifier server, Optional<CassandraClientPoolingContainer> container) {
        if (container.isPresent()) {
            addPoolInternal(server, container.get());
        } else {
            addPool(server);
        }
    }

    private void addPoolInternal(CassandraNodeIdentifier server, CassandraClientPoolingContainer container) {
        currentPools.put(server, container);
    }

    /**
     * Removes the pool from the set of current pools. Note that this shuts down all idle connections, but active ones
     * remain alive until they are returned to the pool, whereby they are destroyed immediately. Threads waiting on the
     * pool will be interrupted.
     */
    public CassandraClientPoolingContainer removePool(CassandraNodeIdentifier removedServerAddress) {
        blacklist.remove(removedServerAddress);
        CassandraClientPoolingContainer removedContainer = currentPools.remove(removedServerAddress);
        return removedContainer;
    }

    public void cacheInitialCassandraHosts() {
        Set<CassandraNodeIdentifier> nodeIdentifiers = getServersFromConfig();

        cassandraHosts = nodeIdentifiers.stream()
                .sorted(Comparator.comparing(CassandraNodeIdentifier::toString))
                .collect(Collectors.toList());
        cassandraHosts.forEach(this::addPool);
    }

    // Todo(Snanda): need to test how this actually works in prod
    public Set<CassandraNodeIdentifier> getServersFromConfig() {
        return config.servers().accept(new ThriftHostsExtractingVisitor()).stream()
                .map(socketAddress ->
                        CassandraNodeIdentifier.create(socketAddress.getAddress(), socketAddress.getPort()))
                .collect(Collectors.toSet());
    }

    public void clearInitialCassandraHosts() {
        cassandraHosts = Collections.emptyList();
    }

    @VisibleForTesting
    void overrideHostToDatacenterMapping(Map<CassandraNodeIdentifier, String> hostToDatacenterOverride) {
        this.hostToDatacenter = hostToDatacenterOverride;
    }
}
