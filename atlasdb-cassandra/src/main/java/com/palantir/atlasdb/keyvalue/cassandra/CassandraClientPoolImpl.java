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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.RangeMap;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.concurrent.InitializeableScheduledExecutorServiceSupplier;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TokenRange;

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
@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class CassandraClientPoolImpl implements CassandraClientPool {
    private static final InitializeableScheduledExecutorServiceSupplier SHARED_EXECUTOR_SUPPLIER =
            new InitializeableScheduledExecutorServiceSupplier(
                    new NamedThreadFactory("CassandraClientPoolRefresh", true));

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

    private static final SafeLogger log = SafeLoggerFactory.get(CassandraClientPoolImpl.class);

    private final Blacklist blacklist;
    private final CassandraRequestExceptionHandler exceptionHandler;
    private final CassandraService cassandra;

    private final CassandraKeyValueServiceConfig config;
    private final StartupChecks startupChecks;
    private final ScheduledExecutorService refreshDaemon;
    private final CassandraClientPoolMetrics metrics;
    private final InitializingWrapper wrapper = new InitializingWrapper();
    private final CassandraAbsentNodeTracker absentHostTracker;

    private ScheduledFuture<?> refreshPoolFuture;

    @VisibleForTesting
    static CassandraClientPoolImpl createImplForTest(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks,
            Blacklist blacklist) {
        CassandraRequestExceptionHandler exceptionHandler = testExceptionHandler(blacklist);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                metricsManager,
                config,
                startupChecks,
                exceptionHandler,
                blacklist,
                new CassandraClientPoolMetrics(metricsManager));
        cassandraClientPool.wrapper.initialize(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        return cassandraClientPool;
    }

    @VisibleForTesting
    static CassandraClientPoolImpl createImplForTest(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks,
            InitializeableScheduledExecutorServiceSupplier initializeableExecutorSupplier,
            Blacklist blacklist,
            CassandraService cassandra) {
        CassandraRequestExceptionHandler exceptionHandler = testExceptionHandler(blacklist);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                config,
                startupChecks,
                initializeableExecutorSupplier,
                exceptionHandler,
                blacklist,
                cassandra,
                new CassandraClientPoolMetrics(metricsManager));
        cassandraClientPool.wrapper.initialize(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        return cassandraClientPool;
    }

    public static CassandraClientPool create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            boolean initializeAsync) {
        Blacklist blacklist = new Blacklist(config);
        CassandraRequestExceptionHandler exceptionHandler = new CassandraRequestExceptionHandler(
                () -> runtimeConfig.get().numberOfRetriesOnSameHost(),
                () -> runtimeConfig.get().numberOfRetriesOnAllHosts(),
                () -> runtimeConfig.get().conservativeRequestExceptionHandler(),
                blacklist);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                metricsManager,
                config,
                StartupChecks.RUN,
                exceptionHandler,
                blacklist,
                new CassandraClientPoolMetrics(metricsManager));
        cassandraClientPool.wrapper.initialize(initializeAsync);
        return cassandraClientPool.wrapper.isInitialized() ? cassandraClientPool : cassandraClientPool.wrapper;
    }

    private CassandraClientPoolImpl(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks,
            CassandraRequestExceptionHandler exceptionHandler,
            Blacklist blacklist,
            CassandraClientPoolMetrics metrics) {
        this(
                config,
                startupChecks,
                SHARED_EXECUTOR_SUPPLIER,
                exceptionHandler,
                blacklist,
                new CassandraService(metricsManager, config, blacklist, metrics),
                metrics);
    }

    private CassandraClientPoolImpl(
            CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks,
            InitializeableScheduledExecutorServiceSupplier initializeableExecutorSupplier,
            CassandraRequestExceptionHandler exceptionHandler,
            Blacklist blacklist,
            CassandraService cassandra,
            CassandraClientPoolMetrics metrics) {
        this.config = config;
        this.startupChecks = startupChecks;
        initializeableExecutorSupplier.initialize(config.numPoolRefreshingThreads());
        this.refreshDaemon = initializeableExecutorSupplier.get();
        this.blacklist = blacklist;
        this.exceptionHandler = exceptionHandler;
        this.cassandra = cassandra;
        this.metrics = metrics;
        this.absentHostTracker = new CassandraAbsentNodeTracker(config.consecutiveAbsencesBeforePoolRemoval());
    }

    private void tryInitialize() {
        cassandra.cacheInitialCassandraHosts();

        refreshPoolFuture = refreshDaemon.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshPool();
                    } catch (Throwable t) {
                        log.warn(
                                "Failed to refresh Cassandra KVS pool."
                                        + " Extended periods of being unable to refresh will cause perf degradation.",
                                t);
                    }
                },
                config.poolRefreshIntervalSeconds(),
                config.poolRefreshIntervalSeconds(),
                TimeUnit.SECONDS);

        // for testability, mock/spy are bad at mockability of things called in constructors
        if (startupChecks == StartupChecks.RUN) {
            runOneTimeStartupChecks();
        }
        refreshPool(); // ensure we've initialized before returning
        metrics.registerAggregateMetrics(blacklist::size);
    }

    private void cleanUpOnInitFailure() {
        refreshPoolFuture.cancel(true);
        cassandra
                .getPools()
                .forEach((address, cassandraClientPoolingContainer) ->
                        cassandraClientPoolingContainer.shutdownPooling());
        cassandra.getPools().clear();
        cassandra.clearInitialCassandraHosts();
    }

    private static CassandraRequestExceptionHandler testExceptionHandler(Blacklist blacklist) {
        return CassandraRequestExceptionHandler.withNoBackoffForTest(
                CassandraClientPoolImpl::getMaxRetriesPerHost, CassandraClientPoolImpl::getMaxTriesTotal, blacklist);
    }

    @Override
    public void shutdown() {
        cassandra.close();
        refreshPoolFuture.cancel(false);
        cassandra
                .getPools()
                .forEach((address, cassandraClientPoolingContainer) ->
                        cassandraClientPoolingContainer.shutdownPooling());
        absentHostTracker.shutDown();
    }

    /**
     * This is the maximum number of times we'll accept connection failures to one host before blacklisting it. Note
     * that subsequent hosts we try in the same call will actually be blacklisted after one connection failure
     */
    @VisibleForTesting
    static int getMaxRetriesPerHost() {
        return CassandraKeyValueServiceRuntimeConfig.getDefault().numberOfRetriesOnSameHost();
    }

    @VisibleForTesting
    static int getMaxTriesTotal() {
        return CassandraKeyValueServiceRuntimeConfig.getDefault().numberOfRetriesOnAllHosts();
    }

    @Override
    public Map<InetSocketAddress, CassandraClientPoolingContainer> getCurrentPools() {
        return cassandra.getPools();
    }

    @VisibleForTesting
    RangeMap<LightweightOppToken, List<CassandraNodeIdentifier>> getTokenMap() {
        return cassandra.getTokenMap();
    }

    @VisibleForTesting
    Set<CassandraNodeIdentifier> getLocalHosts() {
        return cassandra.getLocalHosts();
    }

    private synchronized void refreshPool() {
        blacklist.checkAndUpdate(cassandra.getNodeIds());

        Set<CassandraNodeIdentifier> resolvedConfigAddresses = cassandra.getServersFromConfig();

        if (config.autoRefreshNodes()) {
            setServersInPoolTo(cassandra.refreshTokenRangesAndGetServers());
        } else {
            setServersInPoolTo(resolvedConfigAddresses);
        }

        cassandra.debugLogStateOfPool();
    }

    @VisibleForTesting
    void setServersInPoolTo(Set<CassandraNodeIdentifier> desiredServers) {
        Map<String, CassandraNodeIdentifier> desiredServersByHostName = KeyedStream.of(desiredServers)
                .mapKeys(CassandraNodeIdentifier::getHostName)
                .collectToMap();

        Set<CassandraNodeIdentifier> cachedServers = getCachedServers();

        AtomicBoolean didWeMakeAChange = new AtomicBoolean();

        // For all absent hosts, move their last used ip address
        cachedServers.forEach(server -> {
            if (!desiredServersByHostName.containsKey(server.getHostName())) {
                server.vacateLastUsedIpAddress();
                CassandraClientPoolingContainer cassandraClientPoolingContainer = cassandra.removePool(server);
                absentHostTracker.trackAbsentHost(server, cassandraClientPoolingContainer);
                didWeMakeAChange.set(true);
            }
        });

        Map<String, CassandraNodeIdentifier> cachedServersByHostName = KeyedStream.of(cachedServers)
                .mapKeys(CassandraNodeIdentifier::getHostName)
                .collectToMap();

        // For all present hosts, update their last used ip address
        // Todo(snanda): what to do with absent situation? Hmm maybe a nodeID with null ip address can be considered
        //  as absent :)
        desiredServers.forEach(serverToAdd -> {
            String desiredHostName = serverToAdd.getHostName();
            InetAddress address = serverToAdd.getLastUsedIpAddress();

            CassandraNodeIdentifier currentCassNodeIdentifier =
                    cachedServersByHostName.computeIfAbsent(desiredHostName, _x -> {
                        CassandraNodeIdentifier cassandraNodeIdentifier =
                                CassandraNodeIdentifier.create(address, serverToAdd.getPort());
                        cassandra.returnOrCreatePool(
                                cassandraNodeIdentifier, absentHostTracker.returnPool(cassandraNodeIdentifier));
                        didWeMakeAChange.set(true);
                        return cassandraNodeIdentifier;
                    });

            currentCassNodeIdentifier.updateMostRecentIpAddress(address);
        });

        Set<CassandraNodeIdentifier> serversToShutdown = absentHostTracker.incrementAbsenceAndRemove();

        if (didWeMakeAChange.get()) { // if we made any changes
            sanityCheckRingConsistency();
            cassandra.refreshTokenRangesAndGetServers();
        }

        // logRefreshedHosts(serversToAdd, serversToShutdown, absentServers);
    }

    private static void logRefreshedHosts(
            Set<CassandraNodeIdentifier> serversToAdd,
            Set<CassandraNodeIdentifier> serversToShutdown,
            Set<CassandraNodeIdentifier> absentServers) {
        if (serversToShutdown.isEmpty() && serversToAdd.isEmpty() && absentServers.isEmpty()) {
            log.debug("No hosts added or removed during Cassandra pool refresh");
        } else {
            log.info(
                    "Cassandra pool refresh added hosts {}, removed hosts {}, absentServers {}.",
                    SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAdd)),
                    SafeArg.of("serversToShutdown", CassandraLogHelper.collectionOfHosts(serversToShutdown)),
                    SafeArg.of("absentServers", CassandraLogHelper.collectionOfHosts(absentServers)));
        }
    }

    private Set<CassandraNodeIdentifier> getCachedServers() {
        return cassandra.getNodeIds().keySet();
    }

    @Override
    public CassandraNodeIdentifier getRandomHostForKey(byte[] key) {
        return cassandra.getRandomNodeIdentifierForKey(key);
    }

    @VisibleForTesting
    void runOneTimeStartupChecks() {
        try {
            CassandraVerifier.ensureKeyspaceExistsAndIsUpToDate(this, config);
        } catch (Exception e) {
            log.error("Startup checks failed, was not able to create the keyspace or ensure it already existed.", e);
            throw new RuntimeException(e);
        }

        Map<CassandraNodeIdentifier, Exception> completelyUnresponsiveHosts = new HashMap<>();
        Map<CassandraNodeIdentifier, Exception> aliveButInvalidPartitionerHosts = new HashMap<>();

        boolean thisHostResponded = false;
        boolean atLeastOneHostResponded = false;

        for (CassandraNodeIdentifier cassandraNodeIdentifier : getCachedServers()) {
            thisHostResponded = false;
            try {
                runOnHost(cassandraNodeIdentifier, CassandraVerifier.healthCheck);
                thisHostResponded = true;
                atLeastOneHostResponded = true;
            } catch (Exception e) {
                completelyUnresponsiveHosts.put(cassandraNodeIdentifier, e);
                blacklist.add(cassandraNodeIdentifier);
            }

            if (thisHostResponded) {
                try {
                    runOnHost(cassandraNodeIdentifier, getValidatePartitioner());
                } catch (Exception e) {
                    aliveButInvalidPartitionerHosts.put(cassandraNodeIdentifier, e);
                }
            }
        }

        StringBuilder errorBuilderForEntireCluster = new StringBuilder();
        if (completelyUnresponsiveHosts.size() > 0) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append(" determined that the following hosts are unreachable for the following reasons: \n");
            completelyUnresponsiveHosts.forEach((host, exception) -> errorBuilderForEntireCluster.append(String.format(
                    "\tHost: %s was marked unreachable" + " via exception: %s%n",
                    host.getHostString(), exception.toString())));
        }

        if (aliveButInvalidPartitionerHosts.size() > 0) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append("determined that the following hosts were alive but are configured")
                    .append("with an invalid partitioner: \n");
            aliveButInvalidPartitionerHosts.forEach(
                    (host, exception) -> errorBuilderForEntireCluster.append(String.format(
                            "\tHost: %s was marked as invalid partitioner" + " via exception: %s%n",
                            host.getHostString(), exception.toString())));
        }

        if (atLeastOneHostResponded && aliveButInvalidPartitionerHosts.size() == 0) {
            return;
        } else {
            throw new RuntimeException(errorBuilderForEntireCluster.toString());
        }
    }

    @Override
    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runWithRetryOnHost(cassandra.getRandomGoodHost().getCassandraNodeIdentifier(), fn);
    }

    @Override
    public <V, K extends Exception> V runWithRetryOnHost(
            CassandraNodeIdentifier specifiedHost, FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        RetryableCassandraRequest<V, K> req = new RetryableCassandraRequest<>(specifiedHost, fn);

        while (true) {
            if (log.isTraceEnabled()) {
                log.trace(
                        "Running function on host {}.",
                        SafeArg.of("host", CassandraLogHelper.host(req.getCassandraNodeIdentifier())));
            }
            CassandraClientPoolingContainer hostPool = getPreferredHostOrFallBack(req);

            try {
                V response = runWithPooledResourceRecordingMetrics(hostPool, req.getFunction());
                removeFromBlacklistAfterResponse(hostPool.getCassandraNodeIdentifier());
                return response;
            } catch (Exception ex) {
                exceptionHandler.handleExceptionFromRequest(req, hostPool.getCassandraNodeIdentifier(), ex);
            }
        }
    }

    private <V, K extends Exception> CassandraClientPoolingContainer getPreferredHostOrFallBack(
            RetryableCassandraRequest<V, K> req) {
        CassandraClientPoolingContainer hostPool = cassandra.getNodeIds().get(req.getCassandraNodeIdentifier());

        if (blacklist.contains(req.getCassandraNodeIdentifier())
                || hostPool == null
                || req.shouldGiveUpOnPreferredHost()) {
            CassandraNodeIdentifier previousHost =
                    hostPool == null ? req.getCassandraNodeIdentifier() : hostPool.getCassandraNodeIdentifier();
            Optional<CassandraClientPoolingContainer> hostPoolCandidate = cassandra.getRandomGoodHostForPredicate(
                    address -> !req.alreadyTriedOnHost(address), req.getTriedNodes());
            hostPool = hostPoolCandidate.orElseGet(cassandra::getRandomGoodHost);
            log.warn(
                    "Randomly redirected a query intended for host {} to {}.",
                    SafeArg.of("previousHost", CassandraLogHelper.host(previousHost)),
                    SafeArg.of("randomHost", CassandraLogHelper.host(hostPool.getCassandraNodeIdentifier())));
        }
        return hostPool;
    }

    @Override
    public <V, K extends Exception> V run(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runOnHost(cassandra.getRandomGoodHost().getCassandraNodeIdentifier(), fn);
    }

    @Override
    public <V, K extends Exception> V runOnHost(
            CassandraNodeIdentifier specifiedHost, FunctionCheckedException<CassandraClient, Void, Exception> fn) throws K {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(specifiedHost);
        V response = runWithPooledResourceRecordingMetrics(hostPool, fn);
        removeFromBlacklistAfterResponse(specifiedHost);
        return response;
    }

    private void removeFromBlacklistAfterResponse(CassandraNodeIdentifier host) {
        if (blacklist.contains(host)) {
            blacklist.remove(host);
            log.info(
                    "Added host {} back into the pool after receiving a successful response",
                    SafeArg.of("host", CassandraLogHelper.host(host)));
        }
    }

    private <V, K extends Exception> V runWithPooledResourceRecordingMetrics(
            CassandraClientPoolingContainer hostPool, FunctionCheckedException<CassandraClient, V, K> fn) throws K {

        metrics.recordRequestOnHost(hostPool);
        try {
            return hostPool.runWithPooledResource(fn);
        } catch (Exception e) {
            metrics.recordExceptionOnHost(hostPool);
            if (CassandraRequestExceptionHandler.isConnectionException(e)) {
                metrics.recordConnectionExceptionOnHost(hostPool);
            }
            throw e;
        }
    }

    // This method exists to verify a particularly nasty bug where cassandra doesn't have a
    // consistent ring across all of it's nodes.  One node will think it owns more than the others
    // think it does and they will not send writes to it, but it will respond to requests
    // acting like it does.
    private void sanityCheckRingConsistency() {
        Multimap<Set<TokenRange>, CassandraNodeIdentifier> tokenRangesToHost = HashMultimap.create();
        for (CassandraNodeIdentifier nodeIdentifier : getCachedServers()) {
            try (CassandraClient client = CassandraClientFactory.getClientInternal(nodeIdentifier, config)) {
                try {
                    client.describe_keyspace(config.getKeyspaceOrThrow());
                } catch (NotFoundException e) {
                    return; // don't care to check for ring consistency when we're not even fully initialized
                }
                tokenRangesToHost.put(ImmutableSet.copyOf(client.describe_ring(config.getKeyspaceOrThrow())), nodeIdentifier);
            } catch (Exception e) {
                log.warn("Failed to get ring info from nodeIdentifier: {}", SafeArg.of("nodeIdentifier", CassandraLogHelper.host(nodeIdentifier)), e);
            }
        }

        if (tokenRangesToHost.isEmpty()) {
            log.warn(
                    "Failed to get ring info for entire Cassandra cluster ({});"
                            + " ring could not be checked for consistency.",
                    UnsafeArg.of("keyspace", config.getKeyspaceOrThrow()));
            return;
        }

        if (tokenRangesToHost.keySet().size() == 1) { // all nodes agree on a consistent view of the cluster. Good.
            return;
        }

        RuntimeException ex = new SafeIllegalStateException(
                "Hosts have differing ring descriptions. This can lead to inconsistent reads and lost data. ");
        log.error(
                "Cassandra does not appear to have a consistent ring across all of its nodes. This could cause us to"
                        + " lose writes. The mapping of token ranges to hosts is:\n{}",
                UnsafeArg.of("tokenRangesToHost", CassandraLogHelper.tokenRangesToHost(tokenRangesToHost)),
                ex);

        // provide some easier to grok logging for the two most common cases
        if (tokenRangesToHost.size() > 2) {
            tokenRangesToHost.asMap().entrySet().stream()
                    .filter(entry -> entry.getValue().size() == 1)
                    .forEach(entry -> {
                        // We've checked above that entry.getValue() has one element, so we never NPE here.
                        CassandraLogHelper.HostAndIpAddress hostString =
                                CassandraLogHelper.host(Iterables.getFirst(entry.getValue(), null));
                        log.error(
                                "Host: {} disagrees with the other nodes about the ring state.",
                                SafeArg.of("host", hostString));
                    });
        }
        if (tokenRangesToHost.keySet().size() == 2) {
            ImmutableList<Set<TokenRange>> sets = ImmutableList.copyOf(tokenRangesToHost.keySet());
            Set<TokenRange> set1 = sets.get(0);
            Set<TokenRange> set2 = sets.get(1);
            log.error(
                    "Hosts are split. group1: {} group2: {}",
                    SafeArg.of("hosts1", CassandraLogHelper.collectionOfHosts(tokenRangesToHost.get(set1))),
                    SafeArg.of("hosts2", CassandraLogHelper.collectionOfHosts(tokenRangesToHost.get(set2))));
        }

        CassandraVerifier.logErrorOrThrow(ex.getMessage(), config.ignoreInconsistentRingChecks());
    }

    @Override
    public FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner() {
        return CassandraUtils.getValidatePartitioner(config);
    }

    public enum StartupChecks {
        RUN,
        DO_NOT_RUN
    }
}
