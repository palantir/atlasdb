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
import java.util.Comparator;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

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
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.ClientCreationFailedException;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.processors.AutoDelegate;
import com.palantir.remoting3.tracing.Tracers;

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
@AutoDelegate(typeToExtend = CassandraClientPool.class)
public final class CassandraClientPoolImpl implements CassandraClientPool {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_CassandraClientPool {
        @Override
        public CassandraClientPool delegate() {
            checkInitialized();
            return CassandraClientPoolImpl.this;
        }

        @Override
        protected void tryInitialize() {
            CassandraClientPoolImpl.this.tryInitialize();
        }

        @Override
        protected void cleanUpOnInitFailure() {
            CassandraClientPoolImpl.this.cleanUpOnInitFailure();
        }

        @Override
        protected String getInitializingClassName() {
            return "CassandraClientPool";
        }

        @Override
        public void shutdown() {
            cancelInitialization(CassandraClientPoolImpl.this::shutdown);
        }
    }

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
    @VisibleForTesting
    volatile RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap = ImmutableRangeMap.of();

    private final Blacklist blacklist;

    private final CassandraKeyValueServiceConfig config;
    private final Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools = Maps.newConcurrentMap();
    private final StartupChecks startupChecks;
    private final ScheduledExecutorService refreshDaemon;
    private final CassandraClientPoolMetrics metrics = new CassandraClientPoolMetrics();
    private final InitializingWrapper wrapper = new InitializingWrapper();

    private List<InetSocketAddress> cassandraHosts;
    private ScheduledFuture<?> refreshPoolFuture;

    @VisibleForTesting
    static CassandraClientPoolImpl createImplForTest(CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks, Blacklist blacklist) {
        return create(config, startupChecks, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC, blacklist);
    }

    public static CassandraClientPool create(CassandraKeyValueServiceConfig config) {
        return create(config, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static CassandraClientPool create(CassandraKeyValueServiceConfig config, boolean initializeAsync) {
        CassandraClientPoolImpl cassandraClientPool = create(config,
                StartupChecks.RUN, initializeAsync, new Blacklist(config));
        return cassandraClientPool.wrapper.isInitialized() ? cassandraClientPool : cassandraClientPool.wrapper;
    }

    private static CassandraClientPoolImpl create(CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks, boolean initializeAsync, Blacklist blacklist) {
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(config, startupChecks, blacklist);
        cassandraClientPool.wrapper.initialize(initializeAsync);
        return cassandraClientPool;
    }

    private CassandraClientPoolImpl(
            CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks,
            Blacklist blacklist) {
        this.config = config;
        this.startupChecks = startupChecks;
        this.refreshDaemon = Tracers.wrap(PTExecutors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CassandraClientPoolRefresh-%d")
                .build()));
        this.blacklist = blacklist;
    }

    private void tryInitialize() {
        cassandraHosts = config.servers().stream()
                .sorted(Comparator.comparing(InetSocketAddress::toString))
                .collect(Collectors.toList());
        cassandraHosts.forEach(this::addPool);

        refreshPoolFuture = refreshDaemon.scheduleWithFixedDelay(() -> {
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
        metrics.registerAggregateMetrics(blacklist::size);
    }

    private void cleanUpOnInitFailure() {
        metrics.deregisterMetrics();
        refreshPoolFuture.cancel(true);
        currentPools.forEach((address, cassandraClientPoolingContainer) ->
                cassandraClientPoolingContainer.shutdownPooling());
        currentPools.clear();
    }

    @Override
    public void shutdown() {
        refreshDaemon.shutdown();
        currentPools.forEach((address, cassandraClientPoolingContainer) ->
                cassandraClientPoolingContainer.shutdownPooling());
        metrics.deregisterMetrics();
    }

    @Override
    public Map<InetSocketAddress, CassandraClientPoolingContainer> getCurrentPools() {
        return currentPools;
    }

    private synchronized void refreshPool() {
        blacklist.checkAndUpdate(getCurrentPools());

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

        log.debug("Cassandra pool refresh added hosts {}, removed hosts {}.",
                SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAdd)),
                SafeArg.of("serversToRemove", CassandraLogHelper.collectionOfHosts(serversToRemove)));
        debugLogStateOfPool();
    }

    @VisibleForTesting
    void addPool(InetSocketAddress server) {
        int currentPoolNumber = cassandraHosts.indexOf(server) + 1;
        addPool(server, new CassandraClientPoolingContainer(server, config, currentPoolNumber));
    }

    @VisibleForTesting
    void addPool(InetSocketAddress server, CassandraClientPoolingContainer container) {
        currentPools.put(server, container);
    }

    @VisibleForTesting
    void removePool(InetSocketAddress removedServerAddress) {
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

    private void debugLogStateOfPool() {
        if (log.isDebugEnabled()) {
            StringBuilder currentState = new StringBuilder();
            currentState.append(
                    String.format("POOL STATUS: Current blacklist = %s,%n current hosts in pool = %s%n",
                            blacklist.describeBlacklistedHosts(),
                            currentPools.keySet().toString()));
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

        Set<InetSocketAddress> livingHosts = blacklist.filterBlacklistedHostsFrom(filteredHosts);
        if (livingHosts.isEmpty()) {
            log.warn("There are no known live hosts in the connection pool matching the predicate. We're choosing"
                    + " one at random in a last-ditch attempt at forward progress.");
            livingHosts = filteredHosts;
        }

        InetSocketAddress randomLivingHost = getRandomHostByActiveConnections(
                Maps.filterKeys(currentPools, livingHosts::contains));
        return Optional.ofNullable(pools.get(randomLivingHost));
    }

    @Override
    public InetSocketAddress getRandomHostForKey(byte[] key) {
        List<InetSocketAddress> hostsForKey = tokenMap.get(new LightweightOppToken(key));

        if (hostsForKey == null) {
            log.debug("We attempted to route your query to a cassandra host that already contains the relevant data."
                    + " However, the mapping of which host contains which data is not available yet."
                    + " We will choose a random host instead.");
            return getRandomGoodHost().getHost();
        }

        Set<InetSocketAddress> liveOwnerHosts = blacklist.filterBlacklistedHostsFrom(hostsForKey);

        if (liveOwnerHosts.isEmpty()) {
            log.warn("Perf / cluster stability issue. Token aware query routing has failed because there are no known "
                    + "live hosts that claim ownership of the given range. Falling back to choosing a random live node."
                    + " Current host blacklist is {}."
                    + " Current state logged at TRACE",
                    SafeArg.of("blacklistedHosts", blacklist.blacklistDetails()));
            log.trace("Current ring view is: {}.",
                    SafeArg.of("tokenMap", CassandraLogHelper.tokenMap(tokenMap)));
            return getRandomGoodHost().getHost();
        } else {
            return getRandomHostByActiveConnections(Maps.filterKeys(currentPools, liveOwnerHosts::contains));
        }
    }

    private static InetSocketAddress getRandomHostByActiveConnections(
            Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        return WeightedHosts.create(pools).getRandomHost();
    }

    @VisibleForTesting
    void runOneTimeStartupChecks() {
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
                blacklist.add(host);
            }

            if (thisHostResponded) {
                try {
                    runOnHost(host, getValidatePartitioner());
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
                            + " via exception: %s%n", host.getHostString(), exception.toString())));
        }

        if (aliveButInvalidPartitionerHosts.size() > 0) {
            errorBuilderForEntireCluster.append("Performing routine startup checks,")
                    .append("determined that the following hosts were alive but are configured")
                    .append("with an invalid partitioner: \n");
            aliveButInvalidPartitionerHosts.forEach((host, exception) ->
                    errorBuilderForEntireCluster.append(String.format("\tHost: %s was marked as invalid partitioner"
                            + " via exception: %s%n", host.getHostString(), exception.toString())));
        }

        if (atLeastOneHostResponded && aliveButInvalidPartitionerHosts.size() == 0) {
            return;
        } else {
            throw new RuntimeException(errorBuilderForEntireCluster.toString());
        }
    }

    private InetSocketAddress getAddressForHostThrowUnchecked(String host) {
        try {
            return getAddressForHost(host);
        } catch (UnknownHostException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @Override
    public InetSocketAddress getAddressForHost(String host) throws UnknownHostException {
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
            List<TokenRange> tokenRanges = getRandomGoodHost().runWithPooledResource(getDescribeRing());

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

    @Override
    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runWithRetryOnHost(getRandomGoodHost().getHost(), fn);
    }

    @Override
    public <V, K extends Exception> V runWithRetryOnHost(
            InetSocketAddress specifiedHost,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        int numTries = 0;
        boolean shouldRetryOnDifferentHost = false;
        Set<InetSocketAddress> triedHosts = Sets.newHashSet();
        while (true) {
            if (log.isTraceEnabled()) {
                log.trace("Running function on host {}.",
                        SafeArg.of("host", CassandraLogHelper.host(specifiedHost)));
            }
            CassandraClientPoolingContainer hostPool = currentPools.get(specifiedHost);

            if (blacklist.contains(specifiedHost) || hostPool == null || shouldRetryOnDifferentHost) {
                InetSocketAddress previousHostPool = hostPool == null ? specifiedHost : hostPool.getHost();
                Optional<CassandraClientPoolingContainer> hostPoolCandidate
                        = getRandomGoodHostForPredicate(address -> !triedHosts.contains(address));
                hostPool = hostPoolCandidate.orElseGet(this::getRandomGoodHost);
                log.warn("Randomly redirected a query intended for host {} to {}.",
                        SafeArg.of("previousHost", CassandraLogHelper.host(previousHostPool)),
                        SafeArg.of("randomHost", CassandraLogHelper.host(hostPool.getHost())));
            }

            try {
                return runWithPooledResourceRecordingMetrics(hostPool, fn);
            } catch (Exception e) {
                numTries++;
                triedHosts.add(hostPool.getHost());
                this.<K>handleException(numTries, hostPool.getHost(), e);
                if (isRetriableWithBackoffException(e)) {
                    // And value between -500 and +500ms to backoff to better spread load on failover
                    int sleepDuration = numTries * 1000 + (ThreadLocalRandom.current().nextInt(1000) - 500);
                    log.warn("Retrying a query, {}, with backoff of {}ms, intended for host {}.",
                            UnsafeArg.of("queryString", fn.toString()),
                            SafeArg.of("sleepDuration", sleepDuration),
                            SafeArg.of("hostName", CassandraLogHelper.host(hostPool.getHost())));

                    try {
                        Thread.sleep(sleepDuration);
                    } catch (InterruptedException i) {
                        throw new RuntimeException(i);
                    }
                    if (numTries >= MAX_TRIES_SAME_HOST) {
                        shouldRetryOnDifferentHost = true;
                    }
                } else if (isFastFailoverException(e)) {
                    log.info("Retrying with fast failover a query intended for host {}.",
                            SafeArg.of("hostName", CassandraLogHelper.host(hostPool.getHost())));
                    shouldRetryOnDifferentHost = true;
                }
            }
        }
    }

    @Override
    public <V, K extends Exception> V run(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runOnHost(getRandomGoodHost().getHost(), fn);
    }

    @Override
    public <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        CassandraClientPoolingContainer hostPool = currentPools.get(specifiedHost);
        return runWithPooledResourceRecordingMetrics(hostPool, fn);
    }

    private <V, K extends Exception> V runWithPooledResourceRecordingMetrics(
            CassandraClientPoolingContainer hostPool,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K {

        metrics.recordRequestOnHost(hostPool);
        try {
            return hostPool.runWithPooledResource(fn);
        } catch (Exception e) {
            metrics.recordExceptionOnHost(hostPool);
            if (isConnectionException(e)) {
                metrics.recordConnectionExceptionOnHost(hostPool);
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends Exception> void handleException(int numTries, InetSocketAddress host, Exception ex) throws K {
        if (isRetriableException(ex) || isRetriableWithBackoffException(ex) || isFastFailoverException(ex)) {
            if (numTries >= MAX_TRIES_TOTAL) {
                if (ex instanceof TTransportException
                        && ex.getCause() != null
                        && (ex.getCause().getClass() == SocketException.class)) {
                    log.error(CONNECTION_FAILURE_MSG, numTries, ex);
                    String errorMsg = MessageFormatter.format(CONNECTION_FAILURE_MSG, numTries).getMessage();
                    throw (K) new TTransportException(((TTransportException) ex).getType(), errorMsg, ex);
                } else {
                    log.error("Tried to connect to cassandra {} times.", SafeArg.of("numTries", numTries), ex);
                    throw (K) ex;
                }
            } else {
                // Only log the actual exception the first time
                if (numTries > 1) {
                    log.warn("Error occurred talking to cassandra. Attempt {} of {}. Exception message was: {} : {}",
                            SafeArg.of("numTries", numTries),
                            SafeArg.of("maxTotalTries", MAX_TRIES_TOTAL),
                            SafeArg.of("exceptionClass", ex.getClass().getTypeName()),
                            UnsafeArg.of("exceptionMessage", ex.getMessage()));
                } else {
                    log.warn("Error occurred talking to cassandra. Attempt {} of {}.",
                            SafeArg.of("numTries", numTries),
                            SafeArg.of("maxTotalTries", MAX_TRIES_TOTAL),
                            ex);
                }
                if (isConnectionException(ex) && numTries >= MAX_TRIES_SAME_HOST) {
                    blacklist.add(host);
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
                    client.describe_keyspace(config.getKeyspaceOrThrow());
                } catch (NotFoundException e) {
                    return; // don't care to check for ring consistency when we're not even fully initialized
                }
                tokenRangesToHost.put(ImmutableSet.copyOf(client.describe_ring(config.getKeyspaceOrThrow())), host);
            } catch (Exception e) {
                log.warn("Failed to get ring info from host: {}",
                        SafeArg.of("host", CassandraLogHelper.host(host)),
                        e);
            } finally {
                if (client != null) {
                    client.getOutputProtocol().getTransport().close();
                }
            }

            if (tokenRangesToHost.isEmpty()) {
                log.warn("Failed to get ring info for entire Cassandra cluster ({});"
                        + " ring could not be checked for consistency.",
                        UnsafeArg.of("keyspace", config.getKeyspaceOrThrow()));
                return;
            }

            if (tokenRangesToHost.keySet().size() == 1) { // all nodes agree on a consistent view of the cluster. Good.
                return;
            }

            RuntimeException ex = new IllegalStateException("Hosts have differing ring descriptions."
                    + " This can lead to inconsistent reads and lost data. ");
            log.error("QA-86204 {}: The token ranges to host are:\n{}",
                    SafeArg.of("exception", ex.getMessage()),
                    UnsafeArg.of("tokenRangesToHost", CassandraLogHelper.tokenRangesToHost(tokenRangesToHost)),
                    ex);


            // provide some easier to grok logging for the two most common cases
            if (tokenRangesToHost.size() > 2) {
                tokenRangesToHost.asMap().entrySet().stream()
                        .filter(entry -> entry.getValue().size() == 1)
                        .forEach(entry -> {
                            // We've checked above that entry.getValue() has one element, so we never NPE here.
                            String hostString = CassandraLogHelper.host(Iterables.getFirst(entry.getValue(), null));
                            log.error("Host: {} disagrees with the other nodes about the ring state.",
                                    SafeArg.of("host", hostString));
                        });
            }
            if (tokenRangesToHost.keySet().size() == 2) {
                ImmutableList<Set<TokenRange>> sets = ImmutableList.copyOf(tokenRangesToHost.keySet());
                Set<TokenRange> set1 = sets.get(0);
                Set<TokenRange> set2 = sets.get(1);
                log.error("Hosts are split. group1: {} group2: {}",
                        SafeArg.of("hosts1", CassandraLogHelper.collectionOfHosts(tokenRangesToHost.get(set1))),
                        SafeArg.of("hosts2", CassandraLogHelper.collectionOfHosts(tokenRangesToHost.get(set2))));
            }

            CassandraVerifier.logErrorOrThrow(ex.getMessage(), config.ignoreInconsistentRingChecks());
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

    @VisibleForTesting
    static boolean isFastFailoverException(Throwable ex) {
        return ex != null
                // underlying cassandra table does not exist. The table might exist on other cassandra nodes.
                && (ex instanceof InvalidRequestException
                || isFastFailoverException(ex.getCause()));
    }

    @Override
    public FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner() {
        return CassandraUtils.getValidatePartitioner(config);
    }

    private FunctionCheckedException<CassandraClient, List<TokenRange>, Exception> getDescribeRing() {
        return CassandraUtils.getDescribeRing(config);
    }

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

    @VisibleForTesting
    enum StartupChecks {
        RUN,
        DO_NOT_RUN
    }
}
