/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.ClientCreationFailedException;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting2.tracing.Tracers;

/**
 * Feature breakdown:
 *   - Pooling
 *   - Token Aware Mapping / Query Routing / Data partitioning
 *   - Retriable Queries
 *   - Pool member error tracking / blacklisting*
 *   - Pool refreshing
 *   - Pool node autodiscovery
 *   - Pool member health checking*
 *
 *   *entirely new features
 *
 *   By our old system, this would be a
 *   RefreshingRetriableTokenAwareHealthCheckingManyHostCassandraClientPoolingContainerManager;
 *   ... this is one of the reasons why there is a new system.
 **/
@SuppressWarnings("VisibilityModifier")
public class CassandraClientPool {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);
    private static final String CONNECTION_FAILURE_MSG = "Tried to connect to cassandra {} times."
            + " Error writing to Cassandra socket."
            + " Likely cause: Exceeded maximum thrift frame size;"
            + " unlikely cause: network issues.";

    /**
     * This is the maximum number of times we'll accept connection failures to one host before blacklisting it. Note
     * that subsequent hosts we try in the same call will actually be blacklisted after one connection failure
     */
    @VisibleForTesting
    static final int MAX_TRIES_SAME_HOST = 3;
    @VisibleForTesting
    static final int MAX_TRIES_TOTAL = 6;

    volatile RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap = ImmutableRangeMap.of();
    Map<InetSocketAddress, Long> blacklistedHosts = Maps.newConcurrentMap();
    Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools = Maps.newConcurrentMap();
    final CassandraKeyValueServiceConfig config;
    final ScheduledExecutorService refreshDaemon;

    private final MetricsManager metricsManager = new MetricsManager();
    private final RequestMetrics aggregateMetrics = new RequestMetrics(null);
    private final Map<InetSocketAddress, RequestMetrics> metricsByHost = new HashMap<>();

    public static class LightweightOppToken implements Comparable<LightweightOppToken> {
        final byte[] bytes;

        public LightweightOppToken(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int compareTo(LightweightOppToken other) {
            return UnsignedBytes.lexicographicalComparator().compare(this.bytes, other.bytes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            LightweightOppToken that = (LightweightOppToken) obj;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return BaseEncoding.base16().encode(bytes);
        }
    }

    private class RequestMetrics {
        private final Meter totalRequests;
        private final Meter totalRequestExceptions;
        private final Meter totalRequestConnectionExceptions;

        RequestMetrics(String metricPrefix) {
            totalRequests = metricsManager.registerMeter(
                    CassandraClientPool.class, metricPrefix, "requests");
            totalRequestExceptions = metricsManager.registerMeter(
                    CassandraClientPool.class, metricPrefix, "requestExceptions");
            totalRequestConnectionExceptions = metricsManager.registerMeter(
                    CassandraClientPool.class, metricPrefix, "requestConnectionExceptions");
        }

        void markRequest() {
            totalRequests.mark();
        }

        void markRequestException() {
            totalRequestExceptions.mark();
        }

        void markRequestConnectionException() {
            totalRequestConnectionExceptions.mark();
        }

        // Approximate
        double getExceptionProportion() {
            return ((double) totalRequestExceptions.getCount()) / ((double) totalRequests.getCount());
        }

        // Approximate
        double getConnectionExceptionProportion() {
            return ((double) totalRequestConnectionExceptions.getCount()) / ((double) totalRequests.getCount());
        }
    }

    private enum StartupChecks {
        RUN,
        DO_NOT_RUN
    }

    @VisibleForTesting
    static CassandraClientPool createWithoutChecksForTesting(CassandraKeyValueServiceConfig config) {
        return new CassandraClientPool(config, StartupChecks.DO_NOT_RUN);
    }

    public CassandraClientPool(CassandraKeyValueServiceConfig config) {
        this(config, StartupChecks.RUN);
    }

    private CassandraClientPool(CassandraKeyValueServiceConfig config, StartupChecks startupChecks) {
        this.config = config;
        config.servers().forEach(this::addPool);
        refreshDaemon = Tracers.wrap(PTExecutors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CassandraClientPoolRefresh-%d")
                .build()));
        refreshDaemon.scheduleWithFixedDelay(() -> {
            try {
                refreshPool();
            } catch (Throwable t) {
                log.error("Failed to refresh Cassandra KVS pool."
                        + " Extended periods of being unable to refresh will cause perf degradation.", t);
            }
        }, config.poolRefreshIntervalSeconds(), config.poolRefreshIntervalSeconds(), TimeUnit.SECONDS);

        // for testability, mock/spy are bad at mockability of things called in constructors
        if (startupChecks == StartupChecks.RUN) {
            runOneTimeStartupChecks();
        }
        refreshPool(); // ensure we've initialized before returning
        registerAggregateMetrics();
    }

    public void shutdown() {
        refreshDaemon.shutdown();
        currentPools.forEach((address, cassandraClientPoolingContainer) ->
                cassandraClientPoolingContainer.shutdownPooling());
        metricsManager.deregisterMetrics();
    }

    private void registerAggregateMetrics() {
        metricsManager.registerMetric(
                CassandraClientPool.class, "numBlacklistedHosts",
                () -> blacklistedHosts.size());
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestFailureProportion",
                aggregateMetrics::getExceptionProportion);
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestConnectionExceptionProportion",
                aggregateMetrics::getConnectionExceptionProportion);
    }

    private synchronized void refreshPool() {
        checkAndUpdateBlacklist();

        Set<InetSocketAddress> serversToAdd = Sets.newHashSet(config.servers());
        Set<InetSocketAddress> serversToRemove = ImmutableSet.of();

        if (config.autoRefreshNodes()) {
            refreshTokenRanges(); // re-use token mapping as list of hosts in the cluster
            for (List<InetSocketAddress> rangeOwners : tokenMap.asMapOfRanges().values()) {
                for (InetSocketAddress address : rangeOwners) {
                    serversToAdd.add(address);
                }
            }
        }

        serversToAdd = Sets.difference(serversToAdd, currentPools.keySet());

        if (!config.autoRefreshNodes()) { // (we would just add them back in)
            serversToRemove = Sets.difference(currentPools.keySet(), config.servers());
        }

        serversToAdd.forEach(this::addPool);
        serversToRemove.forEach(this::removePool);

        if (!(serversToAdd.isEmpty() && serversToRemove.isEmpty())) { // if we made any changes
            sanityCheckRingConsistency();
            if (!config.autoRefreshNodes()) { // grab new token mapping, if we didn't already do this before
                refreshTokenRanges();
            }
        }

        log.debug("Cassandra pool refresh added hosts {}, removed hosts {}.", serversToAdd, serversToRemove);
        debugLogStateOfPool();
    }

    private void addPool(InetSocketAddress server) {
        addPool(server, new CassandraClientPoolingContainer(server, config));
    }

    @VisibleForTesting
    void addPool(InetSocketAddress server, CassandraClientPoolingContainer container) {
        currentPools.put(server, container);
        registerMetricsForHost(server);
    }

    private void removePool(InetSocketAddress removedServerAddress) {
        deregisterMetricsForHost(removedServerAddress);
        blacklistedHosts.remove(removedServerAddress);
        try {
            currentPools.get(removedServerAddress).shutdownPooling();
        } catch (Exception e) {
            log.warn("While removing a host ({}) from the pool, we were unable to gently cleanup resources.",
                    removedServerAddress, e);
        }
        currentPools.remove(removedServerAddress);
    }

    private void registerMetricsForHost(InetSocketAddress server) {
        RequestMetrics requestMetrics = new RequestMetrics(server.getHostString());
        metricsManager.registerMetric(
                CassandraClientPool.class,
                server.getHostString(), "requestFailureProportion",
                requestMetrics::getExceptionProportion);
        metricsManager.registerMetric(
                CassandraClientPool.class,
                server.getHostString(), "requestConnectionExceptionProportion",
                requestMetrics::getConnectionExceptionProportion);
        metricsByHost.put(server, requestMetrics);
    }

    private void deregisterMetricsForHost(InetSocketAddress removedServerAddress) {
        metricsByHost.remove(removedServerAddress);
        metricsManager.deregisterMetricsWithPrefix(CassandraClientPool.class, removedServerAddress.getHostString());
    }

    private void debugLogStateOfPool() {
        if (log.isDebugEnabled()) {
            StringBuilder currentState = new StringBuilder();
            currentState.append(
                    String.format("POOL STATUS: Current blacklist = %s,%n current hosts in pool = %s%n",
                    blacklistedHosts.keySet().toString(), currentPools.keySet().toString()));
            for (Entry<InetSocketAddress, CassandraClientPoolingContainer> entry : currentPools.entrySet()) {
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

    private void checkAndUpdateBlacklist() {
        // Check blacklist and re-integrate or continue to wait as necessary
        for (Map.Entry<InetSocketAddress, Long> blacklistedEntry : blacklistedHosts.entrySet()) {
            long backoffTimeMillis = TimeUnit.SECONDS.toMillis(config.unresponsiveHostBackoffTimeSeconds());
            if (blacklistedEntry.getValue() + backoffTimeMillis < System.currentTimeMillis()) {
                InetSocketAddress host = blacklistedEntry.getKey();
                if (isHostHealthy(host)) {
                    blacklistedHosts.remove(host);
                    log.error("Added host {} back into the pool after a waiting period and successful health check.",
                            host);
                }
            }
        }
    }

    private void addToBlacklist(InetSocketAddress badHost) {
        blacklistedHosts.put(badHost, System.currentTimeMillis());
        log.info("Blacklisted host '{}'", badHost);
    }

    private boolean isHostHealthy(InetSocketAddress host) {
        try {
            CassandraClientPoolingContainer testingContainer = currentPools.get(host);
            testingContainer.runWithPooledResource(describeRing);
            testingContainer.runWithPooledResource(validatePartitioner);
            return true;
        } catch (Exception e) {
            log.error("We tried to add {} back into the pool, but got an exception"
                    + " that caused to us distrust this host further.", host, e);
            return false;
        }
    }

    private CassandraClientPoolingContainer getRandomGoodHost() {
        return getRandomGoodHostForPredicate(address -> true).orElseThrow(
                () -> new IllegalStateException("No hosts available."));
    }

    @VisibleForTesting
    Optional<CassandraClientPoolingContainer> getRandomGoodHostForPredicate(Predicate<InetSocketAddress> predicate) {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = currentPools;

        Set<InetSocketAddress> filteredHosts = pools.keySet().stream()
                .filter(predicate)
                .collect(Collectors.toSet());
        if (filteredHosts.isEmpty()) {
            log.error("No hosts match the provided predicate.");
            return Optional.empty();
        }

        Set<InetSocketAddress> livingHosts = Sets.difference(filteredHosts, blacklistedHosts.keySet());
        if (livingHosts.isEmpty()) {
            log.error("There are no known live hosts in the connection pool matching the predicate. We're choosing"
                    + " one at random in a last-ditch attempt at forward progress.");
            livingHosts = filteredHosts;
        }

        InetSocketAddress randomLivingHost = getRandomHostByActiveConnections(
                Maps.filterKeys(currentPools, livingHosts::contains));
        return Optional.ofNullable(pools.get(randomLivingHost));
    }

    public InetSocketAddress getRandomHostForKey(byte[] key) {
        List<InetSocketAddress> hostsForKey = tokenMap.get(new LightweightOppToken(key));

        if (hostsForKey == null) {
            log.debug("We attempted to route your query to a cassandra host that already contains the relevant data."
                    + " However, the mapping of which host contains which data is not available yet."
                    + " We will choose a random host instead.");
            return getRandomGoodHost().getHost();
        }

        Set<InetSocketAddress> liveOwnerHosts = Sets.difference(
                ImmutableSet.copyOf(hostsForKey),
                blacklistedHosts.keySet());

        if (liveOwnerHosts.isEmpty()) {
            log.warn("Perf / cluster stability issue. Token aware query routing has failed because there are no known "
                    + "live hosts that claim ownership of the given range. Falling back to choosing a random live node."
                    + " Current state logged at DEBUG");
            log.debug("Current ring view is: {} and our current host blacklist is {}", tokenMap, blacklistedHosts);
            return getRandomGoodHost().getHost();
        } else {
            return getRandomHostByActiveConnections(Maps.filterKeys(currentPools, liveOwnerHosts::contains));
        }
    }

    private static InetSocketAddress getRandomHostByActiveConnections(
            Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        return WeightedHosts.create(pools).getRandomHost();
    }

    public void runOneTimeStartupChecks() {
        try {
            CassandraVerifier.ensureKeyspaceExistsAndIsUpToDate(this, config);
        } catch (Exception e) {
            log.error("Startup checks failed, was not able to create the keyspace or ensure it already existed.");
            throw new RuntimeException(e);
        }

        Map<InetSocketAddress, Exception> completelyUnresponsiveHosts = Maps.newHashMap();
        Map<InetSocketAddress, Exception> aliveButInvalidPartitionerHosts = Maps.newHashMap();
        boolean thisHostResponded = false;
        boolean atLeastOneHostResponded = false;
        for (InetSocketAddress host : currentPools.keySet()) {
            thisHostResponded = false;
            try {
                runOnHost(host, CassandraVerifier.healthCheck);
                thisHostResponded = true;
                atLeastOneHostResponded = true;
            } catch (Exception e) {
                completelyUnresponsiveHosts.put(host, e);
                addToBlacklist(host);
            }

            if (thisHostResponded) {
                try {
                    runOnHost(host, validatePartitioner);
                } catch (Exception e) {
                    aliveButInvalidPartitionerHosts.put(host, e);
                }
            }
        }

        StringBuilder errorBuilderForEntireCluster = new StringBuilder();
        if (completelyUnresponsiveHosts.size() > 0) {
            errorBuilderForEntireCluster.append("Performing routine startup checks,")
                    .append(" determined that the following hosts are unreachable for the following reasons: \n");
            completelyUnresponsiveHosts.forEach((host, exception) ->
                    errorBuilderForEntireCluster.append(String.format("\tHost: %s was marked unreachable"
                            + " via exception: %s%n", host.toString(), exception.toString())));
        }

        if (aliveButInvalidPartitionerHosts.size() > 0) {
            errorBuilderForEntireCluster.append("Performing routine startup checks,")
                    .append("determined that the following hosts were alive but are configured")
                    .append("with an invalid partitioner: \n");
            aliveButInvalidPartitionerHosts.forEach((host, exception) ->
                    errorBuilderForEntireCluster.append(String.format("\tHost: %s was marked as invalid partitioner"
                            + " via exception: %s%n", host.toString(), exception.toString())));
        }

        if (atLeastOneHostResponded && aliveButInvalidPartitionerHosts.size() == 0) {
            return;
        } else {
            throw new RuntimeException(errorBuilderForEntireCluster.toString());
        }
    }

    //todo dedupe this into a name-demangling class that everyone can access
    protected static String internalTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("\\.", "__");
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
        InetAddress resolvedHost = InetAddress.getByName(host);

        Set<InetSocketAddress> allKnownHosts = Sets.union(currentPools.keySet(), config.servers());
        for (InetSocketAddress address : allKnownHosts) {
            if (address.getAddress().equals(resolvedHost)) {
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

    private void refreshTokenRanges() {
        try {
            ImmutableRangeMap.Builder<LightweightOppToken, List<InetSocketAddress>> newTokenRing =
                    ImmutableRangeMap.builder();

            // grab latest token ring view from a random node in the cluster
            List<TokenRange> tokenRanges = getRandomGoodHost().runWithPooledResource(describeRing);

            // RangeMap needs a little help with weird 1-node, 1-vnode, this-entire-feature-is-useless case
            if (tokenRanges.size() == 1) {
                String onlyEndpoint = Iterables.getOnlyElement(Iterables.getOnlyElement(tokenRanges).getEndpoints());
                InetSocketAddress onlyHost = getAddressForHost(onlyEndpoint);
                newTokenRing.put(Range.all(), ImmutableList.of(onlyHost));
            } else { // normal case, large cluster with many vnodes
                for (TokenRange tokenRange : tokenRanges) {
                    List<InetSocketAddress> hosts = tokenRange.getEndpoints().stream()
                            .map(host -> getAddressForHostThrowUnchecked(host)).collect(Collectors.toList());
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
        } catch (Exception e) {
            log.error("Couldn't grab new token ranges for token aware cassandra mapping!", e);
        }
    }

    private FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception> describeRing =
            new FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception>() {
                @Override
                public List<TokenRange> apply(Cassandra.Client client) throws Exception {
                    return client.describe_ring(config.keyspace());
                }
            };

    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<Cassandra.Client, V, K> fn) throws K {
        return runWithRetryOnHost(getRandomGoodHost().getHost(), fn);
    }

    public <V, K extends Exception> V runWithRetryOnHost(
            InetSocketAddress specifiedHost,
            FunctionCheckedException<Cassandra.Client, V, K> fn) throws K {
        int numTries = 0;
        boolean shouldRetryOnDifferentHost = false;
        Set<InetSocketAddress> triedHosts = Sets.newHashSet();
        while (true) {
            if (log.isTraceEnabled()) {
                log.trace("Running function on host {}.", specifiedHost.getHostString());
            }
            CassandraClientPoolingContainer hostPool = currentPools.get(specifiedHost);

            if (blacklistedHosts.containsKey(specifiedHost) || hostPool == null || shouldRetryOnDifferentHost) {
                log.warn("Randomly redirected a query intended for host {}.", specifiedHost);
                Optional<CassandraClientPoolingContainer> hostPoolCandidate
                        = getRandomGoodHostForPredicate(address -> !triedHosts.contains(address));
                hostPool = hostPoolCandidate.orElseGet(this::getRandomGoodHost);
            }

            try {
                return runWithPooledResourceRecordingMetrics(hostPool, fn);
            } catch (Exception e) {
                numTries++;
                triedHosts.add(hostPool.getHost());
                this.<K>handleException(numTries, hostPool.getHost(), e);
                if (isRetriableWithBackoffException(e)) {
                    log.warn("Retrying with backoff a query intended for host {}.", hostPool.getHost(), e);
                    try {
                        // And value between -500 and +500ms to backoff to better spread load on failover
                        Thread.sleep(numTries * 1000 + (ThreadLocalRandom.current().nextInt(1000) - 500));
                    } catch (InterruptedException i) {
                        throw new RuntimeException(i);
                    }
                    if (numTries >= MAX_TRIES_SAME_HOST) {
                        shouldRetryOnDifferentHost = true;
                    }
                }
            }
        }
    }

    public <V, K extends Exception> V run(FunctionCheckedException<Cassandra.Client, V, K> fn) throws K {
        return runOnHost(getRandomGoodHost().getHost(), fn);
    }

    @VisibleForTesting
    <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
                                                 FunctionCheckedException<Cassandra.Client, V, K> fn) throws K {
        CassandraClientPoolingContainer hostPool = currentPools.get(specifiedHost);
        return runWithPooledResourceRecordingMetrics(hostPool, fn);
    }

    private <V, K extends Exception> V runWithPooledResourceRecordingMetrics(
            CassandraClientPoolingContainer hostPool,
            FunctionCheckedException<Cassandra.Client, V, K> fn) throws K {

        recordRequestOnHost(hostPool);
        try {
            return hostPool.runWithPooledResource(fn);
        } catch (Exception e) {
            recordExceptionOnHost(hostPool);
            if (isConnectionException(e)) {
                recordConnectionExceptionOnHost(hostPool);
            }
            throw e;
        }
    }

    private void recordRequestOnHost(CassandraClientPoolingContainer hostPool) {
        updateMetricOnAggregateAndHost(hostPool, RequestMetrics::markRequest);
    }

    private void recordExceptionOnHost(CassandraClientPoolingContainer hostPool) {
        updateMetricOnAggregateAndHost(hostPool, RequestMetrics::markRequestException);
    }

    private void recordConnectionExceptionOnHost(CassandraClientPoolingContainer hostPool) {
        updateMetricOnAggregateAndHost(hostPool, RequestMetrics::markRequestConnectionException);
    }

    private void updateMetricOnAggregateAndHost(
            CassandraClientPoolingContainer hostPool,
            Consumer<RequestMetrics> metricsConsumer) {
        metricsConsumer.accept(aggregateMetrics);
        RequestMetrics requestMetricsForHost = metricsByHost.get(hostPool.getHost());
        if (requestMetricsForHost != null) {
            metricsConsumer.accept(requestMetricsForHost);
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends Exception> void handleException(int numTries, InetSocketAddress host, Exception ex) throws K {
        if (isRetriableException(ex) || isRetriableWithBackoffException(ex)) {
            if (numTries >= MAX_TRIES_TOTAL) {
                if (ex instanceof TTransportException
                        && ex.getCause() != null
                        && (ex.getCause().getClass() == SocketException.class)) {
                    log.error(CONNECTION_FAILURE_MSG, numTries, ex);
                    String errorMsg = MessageFormatter.format(CONNECTION_FAILURE_MSG, numTries).getMessage();
                    throw (K) new TTransportException(((TTransportException) ex).getType(), errorMsg, ex);
                } else {
                    log.error("Tried to connect to cassandra {} times.", numTries, ex);
                    throw (K) ex;
                }
            } else {
                log.warn("Error occurred talking to cassandra. Attempt {} of {}.", numTries, MAX_TRIES_TOTAL, ex);
                if (isConnectionException(ex) && numTries >= MAX_TRIES_SAME_HOST) {
                    addToBlacklist(host);
                }
            }
        } else {
            throw (K) ex;
        }
    }

    // This method exists to verify a particularly nasty bug where cassandra doesn't have a
    // consistent ring across all of it's nodes.  One node will think it owns more than the others
    // think it does and they will not send writes to it, but it will respond to requests
    // acting like it does.
    private void sanityCheckRingConsistency() {
        Multimap<Set<TokenRange>, InetSocketAddress> tokenRangesToHost = HashMultimap.create();
        for (InetSocketAddress host : currentPools.keySet()) {
            Cassandra.Client client = null;
            try {
                client = CassandraClientFactory.getClientInternal(host, config);
                try {
                    client.describe_keyspace(config.keyspace());
                } catch (NotFoundException e) {
                    return; // don't care to check for ring consistency when we're not even fully initialized
                }
                tokenRangesToHost.put(ImmutableSet.copyOf(client.describe_ring(config.keyspace())), host);
            } catch (Exception e) {
                log.warn("failed to get ring info from host: {}", host, e);
            } finally {
                if (client != null) {
                    client.getOutputProtocol().getTransport().close();
                }
            }

            if (tokenRangesToHost.isEmpty()) {
                log.warn("Failed to get ring info for entire Cassandra cluster ({});"
                        + " ring could not be checked for consistency.", config.keyspace());
                return;
            }

            if (tokenRangesToHost.keySet().size() == 1) { // all nodes agree on a consistent view of the cluster. Good.
                return;
            }

            RuntimeException ex = new IllegalStateException("Hosts have differing ring descriptions."
                    + " This can lead to inconsistent reads and lost data. ");
            log.error("QA-86204 {}: The token ranges to host are:\n{}", ex.getMessage(), tokenRangesToHost, ex);


            // provide some easier to grok logging for the two most common cases
            if (tokenRangesToHost.size() > 2) {
                tokenRangesToHost.asMap().entrySet().stream()
                        .filter(entry -> entry.getValue().size() == 1)
                        .forEach(entry -> log.error("Host: {} disagrees with the other nodes about the ring state.",
                                Iterables.getFirst(entry.getValue(), null)));
            }
            if (tokenRangesToHost.keySet().size() == 2) {
                ImmutableList<Set<TokenRange>> sets = ImmutableList.copyOf(tokenRangesToHost.keySet());
                Set<TokenRange> set1 = sets.get(0);
                Set<TokenRange> set2 = sets.get(1);
                log.error("Hosts are split. group1: {} group2: {}",
                        tokenRangesToHost.get(set1),
                        tokenRangesToHost.get(set2));
            }

            CassandraVerifier.logErrorOrThrow(ex.getMessage(), config.safetyDisabled());
        }
    }

    @VisibleForTesting
    static boolean isConnectionException(Throwable ex) {
        return ex != null
                && (ex instanceof SocketTimeoutException
                || ex instanceof ClientCreationFailedException
                || isConnectionException(ex.getCause()));
    }

    @VisibleForTesting
    static boolean isRetriableException(Throwable ex) {
        return ex != null
                && (ex instanceof TTransportException
                || ex instanceof TimedOutException
                || ex instanceof InsufficientConsistencyException
                || isConnectionException(ex)
                || isRetriableException(ex.getCause()));
    }

    @VisibleForTesting
    static boolean isRetriableWithBackoffException(Throwable ex) {
        return ex != null
                // pool for this node is fully in use
                && (ex instanceof NoSuchElementException
                // remote cassandra node couldn't talk to enough other remote cassandra nodes to answer
                || ex instanceof UnavailableException
                // tcp socket timeout, possibly indicating network flake, long GC, or restarting server
                || isConnectionException(ex)
                || isRetriableWithBackoffException(ex.getCause()));
    }

    final FunctionCheckedException<Cassandra.Client, Void, Exception> validatePartitioner =
            new FunctionCheckedException<Cassandra.Client, Void, Exception>() {
                @Override
                public Void apply(Cassandra.Client client) throws Exception {
                    CassandraVerifier.validatePartitioner(client, config);
                    return null;
                }
            };

    /**
     * Weights hosts inversely by the number of active connections. {@link #getRandomHost()} should then be used to
     * pick a random host
     */
    @VisibleForTesting
    static final class WeightedHosts {
        final NavigableMap<Integer, InetSocketAddress> hosts;

        private WeightedHosts(NavigableMap<Integer, InetSocketAddress> hosts) {
            this.hosts = hosts;
        }

        static WeightedHosts create(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
            Preconditions.checkArgument(!pools.isEmpty(), "pools should be non-empty");
            return new WeightedHosts(buildHostsWeightedByActiveConnections(pools));
        }

        /**
         * The key for a host is the open upper bound of the weight. Since the domain is intended to be contiguous, the
         * closed lower bound of that weight is the key of the previous entry.
         * <p>
         * The closed lower bound of the first entry is 0.
         * <p>
         * Every weight is guaranteed to be non-zero in size. That is, every key is guaranteed to be at least one larger
         * than the previous key.
         */
        private static NavigableMap<Integer, InetSocketAddress> buildHostsWeightedByActiveConnections(
                Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {

            Map<InetSocketAddress, Integer> openRequestsByHost = new HashMap<>(pools.size());
            int totalOpenRequests = 0;
            for (Entry<InetSocketAddress, CassandraClientPoolingContainer> poolEntry : pools.entrySet()) {
                int openRequests = Math.max(poolEntry.getValue().getOpenRequests(), 0);
                openRequestsByHost.put(poolEntry.getKey(), openRequests);
                totalOpenRequests += openRequests;
            }

            int lowerBoundInclusive = 0;
            NavigableMap<Integer, InetSocketAddress> weightedHosts = new TreeMap<>();
            for (Entry<InetSocketAddress, Integer> entry : openRequestsByHost.entrySet()) {
                // We want the weight to be inversely proportional to the number of open requests so that we pick
                // less-active hosts. We add 1 to make sure that all ranges are non-empty
                int weight = totalOpenRequests - entry.getValue() + 1;
                weightedHosts.put(lowerBoundInclusive + weight, entry.getKey());
                lowerBoundInclusive += weight;
            }
            return weightedHosts;
        }

        InetSocketAddress getRandomHost() {
            int index = ThreadLocalRandom.current().nextInt(hosts.lastKey());
            return getRandomHostInternal(index);
        }

        // This basically exists for testing
        InetSocketAddress getRandomHostInternal(int index) {
            return hosts.higherEntry(index).getValue();
        }
    }
}
