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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraLogHelper;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraUtils;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;

public class CassandraService implements AutoCloseable {
    // TODO(tboam): keep logging on old class?
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);

    private final MetricsManager metricsManager;
    private final CassandraKeyValueServiceConfig config;
    private final Blacklist blacklist;
    private final QosClient qosClient;

    private volatile RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap = ImmutableRangeMap.of();
    private final Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools = Maps.newConcurrentMap();

    private List<InetSocketAddress> cassandraHosts;

    public CassandraService(MetricsManager metricsManager, CassandraKeyValueServiceConfig config,
            Blacklist blacklist, QosClient qosClient) {
        this.metricsManager = metricsManager;
        this.config = config;
        this.blacklist = blacklist;
        this.qosClient = qosClient;
    }

    @Override
    public void close() {
        qosClient.close();
    }

    public Set<InetSocketAddress> refreshTokenRanges() {
        Set<InetSocketAddress> servers = Sets.newHashSet();

        try {
            ImmutableRangeMap.Builder<LightweightOppToken, List<InetSocketAddress>> newTokenRing =
                    ImmutableRangeMap.builder();

            // grab latest token ring view from a random node in the cluster
            List<TokenRange> tokenRanges = getTokenRanges();

            // RangeMap needs a little help with weird 1-node, 1-vnode, this-entire-feature-is-useless case
            if (tokenRanges.size() == 1) {
                String onlyEndpoint = Iterables.getOnlyElement(Iterables.getOnlyElement(tokenRanges).getEndpoints());
                InetSocketAddress onlyHost = getAddressForHost(onlyEndpoint);
                newTokenRing.put(Range.all(), ImmutableList.of(onlyHost));
            } else { // normal case, large cluster with many vnodes
                for (TokenRange tokenRange : tokenRanges) {
                    List<InetSocketAddress> hosts = tokenRange.getEndpoints().stream()
                            .map(this::getAddressForHostThrowUnchecked).collect(Collectors.toList());

                    servers.addAll(hosts);

                    LightweightOppToken startToken = new LightweightOppToken(
                            BaseEncoding.base16().decode(tokenRange.getStart_token().toUpperCase()));
                    LightweightOppToken endToken = new LightweightOppToken(
                            BaseEncoding.base16().decode(tokenRange.getEnd_token().toUpperCase()));
                    if (startToken.compareTo(endToken) <= 0) {
                        newTokenRing.put(Range.openClosed(startToken, endToken), hosts);
                    } else {
                        // Handle wrap-around
                        newTokenRing.put(Range.greaterThan(startToken), hosts);
                        newTokenRing.put(Range.atMost(endToken), hosts);
                    }
                }
            }
            tokenMap = newTokenRing.build();
            return servers;
        } catch (Exception e) {
            log.error("Couldn't grab new token ranges for token aware cassandra mapping!", e);

            // return the set of servers we knew about last time we successfully constructed the tokenMap
            return tokenMap.asMapOfRanges().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        }
    }

    private List<TokenRange> getTokenRanges() throws Exception {
        return getRandomGoodHost().runWithPooledResource(CassandraUtils.getDescribeRing(config));
    }

    private InetSocketAddress getAddressForHostThrowUnchecked(String host) {
        try {
            return getAddressForHost(host);
        } catch (UnknownHostException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @VisibleForTesting
    InetSocketAddress getAddressForHost(String host) throws UnknownHostException {
        if (config.addressTranslation().containsKey(host)) {
            return config.addressTranslation().get(host);
        }

        InetAddress resolvedHost = InetAddress.getByName(host);
        Set<InetSocketAddress> allKnownHosts = Sets.union(currentPools.keySet(), config.servers());
        for (InetSocketAddress address : allKnownHosts) {
            if (Objects.equals(address.getAddress(), resolvedHost)) {
                return address;
            }
        }

        Set<Integer> allKnownPorts = allKnownHosts.stream()
                .map(InetSocketAddress::getPort)
                .collect(Collectors.toSet());

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

        Set<InetSocketAddress> filteredHosts = pools.keySet().stream()
                .filter(predicate)
                .collect(Collectors.toSet());
        if (filteredHosts.isEmpty()) {
            log.error("No hosts match the provided predicate.");
            return Optional.empty();
        }

        Set<InetSocketAddress> livingHosts = blacklist.filterBlacklistedHostsFrom(filteredHosts);
        if (livingHosts.isEmpty()) {
            log.warn("There are no known live hosts in the connection pool matching the predicate. We're choosing"
                    + " one at random in a last-ditch attempt at forward progress.");
            livingHosts = filteredHosts;
        }

        Optional<InetSocketAddress> randomLivingHost = getRandomHostByActiveConnections(livingHosts);
        return randomLivingHost.flatMap(host -> Optional.ofNullable(pools.get(host)));
    }

    public CassandraClientPoolingContainer getRandomGoodHost() {
        return getRandomGoodHostForPredicate(address -> true).orElseThrow(
                () -> new IllegalStateException("No hosts available."));
    }

    private String getRingViewDescription() {
        return CassandraLogHelper.tokenMap(tokenMap).toString();
    }

    public RangeMap<LightweightOppToken, List<InetSocketAddress>> getTokenMap() {
        return tokenMap;
    }

    public Map<InetSocketAddress, CassandraClientPoolingContainer> getPools() {
        return currentPools;
    }

    private Optional<InetSocketAddress> getRandomHostByActiveConnections(Set<InetSocketAddress> desiredHosts) {
        Map<InetSocketAddress, CassandraClientPoolingContainer> matchingPools = Maps.filterKeys(currentPools,
                desiredHosts::contains);
        if (matchingPools.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(WeightedHosts.create(matchingPools).getRandomHost());
    }

    public void debugLogStateOfPool() {
        if (log.isDebugEnabled()) {
            StringBuilder currentState = new StringBuilder();
            currentState.append(
                    String.format("POOL STATUS: Current blacklist = %s,%n current hosts in pool = %s%n",
                            blacklist.describeBlacklistedHosts(),
                            currentPools.keySet().toString()));
            for (Map.Entry<InetSocketAddress, CassandraClientPoolingContainer> entry : currentPools.entrySet()) {
                int activeCheckouts = entry.getValue().getActiveCheckouts();
                int totalAllowed = entry.getValue().getPoolSize();

                currentState.append(
                        String.format("\tPOOL STATUS: Pooled host %s has %s out of %s connections checked out.%n",
                                entry.getKey(),
                                activeCheckouts > 0 ? Integer.toString(activeCheckouts) : "(unknown)",
                                totalAllowed > 0 ? Integer.toString(totalAllowed) : "(not bounded)"));
            }
            log.debug("Current pool state: {}", currentState.toString());
        }
    }

    public InetSocketAddress getRandomHostForKey(byte[] key) {
        List<InetSocketAddress> hostsForKey = getHostsFor(key);

        if (hostsForKey == null) {
            log.info("We attempted to route your query to a cassandra host that already contains the relevant data."
                    + " However, the mapping of which host contains which data is not available yet."
                    + " We will choose a random host instead.");
            return getRandomGoodHost().getHost();
        }

        Set<InetSocketAddress> liveOwnerHosts = blacklist.filterBlacklistedHostsFrom(hostsForKey);

        if (!liveOwnerHosts.isEmpty()) {
            Optional<InetSocketAddress> activeHost = getRandomHostByActiveConnections(liveOwnerHosts);
            if (activeHost.isPresent()) {
                return activeHost.get();
            }
        }

        log.warn("Perf / cluster stability issue. Token aware query routing has failed because there are no known "
                + "live hosts that claim ownership of the given range. Falling back to choosing a random live node."
                + " Current host blacklist is {}."
                + " Current state logged at TRACE",
                SafeArg.of("blacklistedHosts", blacklist.blacklistDetails()));
        log.trace("Current ring view is: {}.",
                SafeArg.of("tokenMap", getRingViewDescription()));
        return getRandomGoodHost().getHost();
    }

    public void addPool(InetSocketAddress server) {
        int currentPoolNumber = cassandraHosts.indexOf(server) + 1;
        currentPools.put(server,
                new CassandraClientPoolingContainer(metricsManager, qosClient, server, config, currentPoolNumber));
    }

    public void removePool(InetSocketAddress removedServerAddress) {
        blacklist.remove(removedServerAddress);
        try {
            currentPools.get(removedServerAddress).shutdownPooling();
        } catch (Exception e) {
            log.warn("While removing a host ({}) from the pool, we were unable to gently cleanup resources.",
                    SafeArg.of("removedServerAddress", CassandraLogHelper.host(removedServerAddress)),
                    e);
        }
        currentPools.remove(removedServerAddress);
    }

    public void cacheInitialCassandraHosts() {
        cassandraHosts = config.servers().stream()
                .sorted(Comparator.comparing(InetSocketAddress::toString))
                .collect(Collectors.toList());
        cassandraHosts.forEach(this::addPool);
    }

    public void clearInitialCassandraHosts() {
        cassandraHosts = Collections.emptyList();
    }
}
