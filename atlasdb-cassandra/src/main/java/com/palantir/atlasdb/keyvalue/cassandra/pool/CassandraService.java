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
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraLogHelper;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;

public class CassandraService implements AutoCloseable {
    // TODO(tboam): keep logging on old class?
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraService.class);
    private static final Interner<RangeMap<LightweightOppToken, List<InetSocketAddress>>> tokensInterner =
            Interners.newWeakInterner();

    private final MetricsManager metricsManager;
    private final CassandraKeyValueServiceConfig config;
    private final Blacklist blacklist;
    private final CassandraClientPoolMetrics poolMetrics;

    private volatile RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap = ImmutableRangeMap.of();
    private final Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools = new ConcurrentHashMap<>();

    private List<InetSocketAddress> cassandraHosts;

    private volatile Set<InetSocketAddress> localHosts = ImmutableSet.of();
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
        this.myLocationSupplier = HostLocationSupplier.create(this::getSnitch, config.overrideHostLocation());
        this.blacklist = blacklist;
        this.poolMetrics = poolMetrics;

        Supplier<Map<String, String>> hostnamesByIpSupplier = new HostnamesByIpSupplier(this::getRandomGoodHost);
        this.hostnameByIpSupplier = Suppliers.memoizeWithExpiration(hostnamesByIpSupplier::get, 2, TimeUnit.MINUTES);
    }

    @Override
    public void close() {}

    public Set<InetSocketAddress> refreshTokenRangesAndGetServers() {
        Set<InetSocketAddress> servers = new HashSet<>();

        try {
            ImmutableRangeMap.Builder<LightweightOppToken, List<InetSocketAddress>> newTokenRing =
                    ImmutableRangeMap.builder();

            // grab latest token ring view from a random node in the cluster and update local hosts
            List<TokenRange> tokenRanges = getTokenRanges();
            localHosts = refreshLocalHosts(tokenRanges);
            log.debug("Got some token ranges", SafeArg.of("numTokenRanges", tokenRanges.size()));

            // RangeMap needs a little help with weird 1-node, 1-vnode, this-entire-feature-is-useless case
            if (tokenRanges.size() == 1) {
                String onlyEndpoint = Iterables.getOnlyElement(
                        Iterables.getOnlyElement(tokenRanges).getEndpoints());
                InetSocketAddress onlyHost = getAddressForHost(onlyEndpoint);
                newTokenRing.put(Range.all(), ImmutableList.of(onlyHost));
                servers.add(onlyHost);
            } else { // normal case, large cluster with many vnodes
                for (TokenRange tokenRange : tokenRanges) {
                    List<InetSocketAddress> hosts = tokenRange.getEndpoints().stream()
                            .map(this::getAddressForHostThrowUnchecked)
                            .collect(Collectors.toList());

                    servers.addAll(hosts);

                    LightweightOppToken startToken = new LightweightOppToken(BaseEncoding.base16()
                            .decode(tokenRange.getStart_token().toUpperCase()));
                    LightweightOppToken endToken = new LightweightOppToken(BaseEncoding.base16()
                            .decode(tokenRange.getEnd_token().toUpperCase()));
                    if (startToken.compareTo(endToken) <= 0) {
                        log.debug("Adding token to ring");
                        newTokenRing.put(Range.openClosed(startToken, endToken), hosts);
                    } else {
                        // Handle wrap-around
                        log.debug("Adding wraparound token to ring");
                        newTokenRing.put(Range.greaterThan(startToken), hosts);
                        newTokenRing.put(Range.atMost(endToken), hosts);
                    }
                }
            }
            log.debug(
                    "Interning token map - size before",
                    SafeArg.of("size", tokenMap.asMapOfRanges().size()));
            tokenMap = tokensInterner.intern(newTokenRing.build());
            log.debug(
                    "Interning token map - size after",
                    SafeArg.of("size", tokenMap.asMapOfRanges().size()));
            return servers;
        } catch (Exception e) {
            log.info(
                    "Couldn't grab new token ranges for token aware cassandra mapping. We will retry in {} seconds.",
                    SafeArg.of("poolRefreshIntervalSeconds", config.poolRefreshIntervalSeconds()),
                    e);

            // Attempt to re-resolve addresses from the configuration; this is important owing to certain race
            // conditions where the entire pool becomes invalid between refreshes.
            Set<InetSocketAddress> resolvedConfigAddresses =
                    config.servers().accept(new CassandraServersConfigs.ThriftHostsExtractingVisitor());

            Set<InetSocketAddress> lastKnownAddresses = tokenMap.asMapOfRanges().values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            return Sets.union(resolvedConfigAddresses, lastKnownAddresses);
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

    private Set<InetSocketAddress> refreshLocalHosts(List<TokenRange> tokenRanges) {
        Optional<HostLocation> myLocation = myLocationSupplier.get();

        if (!myLocation.isPresent()) {
            return ImmutableSet.of();
        }

        Set<InetSocketAddress> newLocalHosts = tokenRanges.stream()
                .map(TokenRange::getEndpoint_details)
                .flatMap(Collection::stream)
                .filter(details -> isHostLocal(details, myLocation.get()))
                .map(EndpointDetails::getHost)
                .map(this::getAddressForHostThrowUnchecked)
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
    void setLocalHosts(Set<InetSocketAddress> localHosts) {
        this.localHosts = localHosts;
    }

    public Set<InetSocketAddress> getLocalHosts() {
        return localHosts;
    }

    private InetSocketAddress getAddressForHostThrowUnchecked(String host) {
        try {
            return getAddressForHost(host);
        } catch (UnknownHostException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @VisibleForTesting
    InetSocketAddress getAddressForHost(String inputHost) throws UnknownHostException {
        if (config.addressTranslation().containsKey(inputHost)) {
            return config.addressTranslation().get(inputHost);
        }

        Map<String, String> hostnamesByIp = hostnameByIpSupplier.get();
        String host = hostnamesByIp.getOrDefault(inputHost, inputHost);

        InetAddress resolvedHost = InetAddress.getByName(host);
        Set<InetSocketAddress> allKnownHosts =
                Sets.union(currentPools.keySet(), config.servers().accept(new ThriftHostsExtractingVisitor()));

        for (InetSocketAddress address : allKnownHosts) {
            if (Objects.equals(address.getAddress(), resolvedHost)) {
                return address;
            }
        }

        Set<Integer> allKnownPorts =
                allKnownHosts.stream().map(InetSocketAddress::getPort).collect(Collectors.toSet());

        if (allKnownPorts.size() == 1) { // if everyone is on one port, try and use that
            return new InetSocketAddress(resolvedHost, Iterables.getOnlyElement(allKnownPorts));
        } else {
            throw new UnknownHostException("Couldn't find the provided host in server list or current servers");
        }
    }

    private List<InetSocketAddress> getHostsFor(byte[] key) {
        return tokenMap.get(new LightweightOppToken(key));
    }

    public Optional<CassandraClientPoolingContainer> getRandomGoodHostForPredicate(
            Predicate<InetSocketAddress> predicate) {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = currentPools;

        Set<InetSocketAddress> filteredHosts =
                pools.keySet().stream().filter(predicate).collect(Collectors.toSet());
        if (filteredHosts.isEmpty()) {
            log.info("No hosts match the provided predicate.");
            return Optional.empty();
        }

        Set<InetSocketAddress> livingHosts = blacklist.filterBlacklistedHostsFrom(filteredHosts);
        if (livingHosts.isEmpty()) {
            log.info("There are no known live hosts in the connection pool matching the predicate. We're choosing"
                    + " one at random in a last-ditch attempt at forward progress.");
            livingHosts = filteredHosts;
        }

        Optional<InetSocketAddress> randomLivingHost = getRandomHostByActiveConnections(livingHosts);
        return randomLivingHost.map(pools::get);
    }

    public CassandraClientPoolingContainer getRandomGoodHost() {
        return getRandomGoodHostForPredicate(address -> true)
                .orElseThrow(() -> new SafeIllegalStateException("No hosts available."));
    }

    private String getRingViewDescription() {
        return CassandraLogHelper.tokenMap(tokenMap).toString();
    }

    public RangeMap<LightweightOppToken, List<InetSocketAddress>> getTokenMap() {
        log.debug("getTokenMap()");
        return tokenMap;
    }

    public Map<InetSocketAddress, CassandraClientPoolingContainer> getPools() {
        return currentPools;
    }

    @VisibleForTesting
    Set<InetSocketAddress> maybeFilterLocalHosts(Set<InetSocketAddress> hosts) {
        if (random.nextDouble() < config.localHostWeighting()) {
            Set<InetSocketAddress> localFilteredHosts = Sets.intersection(localHosts, hosts);
            if (!localFilteredHosts.isEmpty()) {
                return localFilteredHosts;
            }
        }

        return hosts;
    }

    private Optional<InetSocketAddress> getRandomHostByActiveConnections(Set<InetSocketAddress> desiredHosts) {

        Set<InetSocketAddress> localFilteredHosts = maybeFilterLocalHosts(desiredHosts);

        Map<InetSocketAddress, CassandraClientPoolingContainer> matchingPools = KeyedStream.stream(
                        ImmutableMap.copyOf(currentPools))
                .filterKeys(localFilteredHosts::contains)
                .collectToMap();
        if (matchingPools.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(WeightedHosts.create(matchingPools).getRandomHost());
    }

    public void debugLogStateOfPool() {
        if (log.isDebugEnabled()) {
            StringBuilder currentState = new StringBuilder();
            currentState.append(String.format(
                    "POOL STATUS: Current blacklist = %s,%n current hosts in pool = %s%n",
                    blacklist.describeBlacklistedHosts(), currentPools.keySet().toString()));
            for (Map.Entry<InetSocketAddress, CassandraClientPoolingContainer> entry : currentPools.entrySet()) {
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

    public InetSocketAddress getRandomHostForKey(byte[] key) {
        List<InetSocketAddress> hostsForKey = getHostsFor(key);

        if (hostsForKey == null) {
            if (config.autoRefreshNodes()) {
                log.info("We attempted to route your query to a cassandra host that already contains the relevant data."
                        + " However, the mapping of which host contains which data is not available yet."
                        + " We will choose a random host instead.");
            }
            return getRandomGoodHost().getHost();
        }

        Set<InetSocketAddress> liveOwnerHosts = blacklist.filterBlacklistedHostsFrom(hostsForKey);

        if (!liveOwnerHosts.isEmpty()) {
            Optional<InetSocketAddress> activeHost = getRandomHostByActiveConnections(liveOwnerHosts);
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
        return getRandomGoodHost().getHost();
    }

    public void addPool(InetSocketAddress server) {
        int currentPoolNumber = cassandraHosts.indexOf(server) + 1;
        currentPools.put(
                server,
                new CassandraClientPoolingContainer(metricsManager, server, config, currentPoolNumber, poolMetrics));
    }

    public void removePool(InetSocketAddress removedServerAddress) {
        blacklist.remove(removedServerAddress);
        try {
            currentPools.get(removedServerAddress).shutdownPooling();
        } catch (Exception e) {
            log.warn(
                    "While removing a host ({}) from the pool, we were unable to gently cleanup resources.",
                    SafeArg.of("removedServerAddress", CassandraLogHelper.host(removedServerAddress)),
                    e);
        }
        currentPools.remove(removedServerAddress);
    }

    public void cacheInitialCassandraHosts() {
        Set<InetSocketAddress> thriftSocket = config.servers().accept(new ThriftHostsExtractingVisitor());

        cassandraHosts = thriftSocket.stream()
                .sorted(Comparator.comparing(InetSocketAddress::toString))
                .collect(Collectors.toList());
        cassandraHosts.forEach(this::addPool);
    }

    public void clearInitialCassandraHosts() {
        cassandraHosts = Collections.emptyList();
    }
}
