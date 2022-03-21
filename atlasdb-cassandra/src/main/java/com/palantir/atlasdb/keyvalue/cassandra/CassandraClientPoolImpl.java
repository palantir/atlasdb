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
import com.google.common.collect.Sets;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    private final CassandraAbsentHostTracker absentHostTracker;

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
        this.absentHostTracker = new CassandraAbsentHostTracker(config.consecutiveAbsencesBeforePoolRemoval());
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
    public Map<CassandraServer, CassandraClientPoolingContainer> getCurrentPools() {
        return cassandra.getPools();
    }

    @VisibleForTesting
    RangeMap<LightweightOppToken, List<CassandraServer>> getTokenMap() {
        return cassandra.getTokenMap();
    }

    @VisibleForTesting
    Set<CassandraServer> getLocalHosts() {
        return cassandra.getLocalHosts();
    }

    private synchronized void refreshPool() {
        blacklist.checkAndUpdate(cassandra.getPools());

        Set<CassandraServer> resolvedConfigAddresses = cassandra.getInitialServerList();

        if (config.autoRefreshNodes()) {
            setServersInPoolTo(cassandra.refreshTokenRangesAndGetServers());
        } else {
            setServersInPoolTo(resolvedConfigAddresses);
        }

        cassandra.debugLogStateOfPool();
    }

    @VisibleForTesting
    void setServersInPoolTo(Set<CassandraServer> desiredServers) {
        Set<CassandraServer> cachedServers = getCachedServers();
        Set<CassandraServer> serversToAdd = ImmutableSet.copyOf(Sets.difference(desiredServers, cachedServers));
        Set<CassandraServer> absentServers = ImmutableSet.copyOf(Sets.difference(cachedServers, desiredServers));

        serversToAdd.forEach(server -> cassandra.returnOrCreatePool(server, absentHostTracker.returnPool(server)));
        Map<CassandraServer, CassandraClientPoolingContainer> containersForAbsentHosts =
                KeyedStream.of(absentServers).map(cassandra::removePool).collectToMap();
        containersForAbsentHosts.forEach(absentHostTracker::trackAbsentCassandraServer);

        Set<CassandraServer> serversToShutdown = absentHostTracker.incrementAbsenceAndRemove();

        if (!(serversToAdd.isEmpty() && absentServers.isEmpty())) { // if we made any changes
            sanityCheckRingConsistency();
            cassandra.refreshTokenRangesAndGetServers();
        }

        logRefreshedHosts(serversToAdd, serversToShutdown, absentServers);
    }

    private static void logRefreshedHosts(
            Set<CassandraServer> serversToAdd,
            Set<CassandraServer> serversToShutdown,
            Set<CassandraServer> absentServers) {
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

    private Set<CassandraServer> getCachedServers() {
        return cassandra.getPools().keySet();
    }

    @Override
    public CassandraServer getRandomHostForKey(byte[] key) {
        return cassandra.getRandomCassandraNodeForKey(key);
    }

    @VisibleForTesting
    void runOneTimeStartupChecks() {
        try {
            CassandraVerifier.ensureKeyspaceExistsAndIsUpToDate(this, config);
        } catch (Exception e) {
            log.error("Startup checks failed, was not able to create the keyspace or ensure it already existed.", e);
            throw new RuntimeException(e);
        }

        Map<CassandraServer, Exception> completelyUnresponsiveNodes = new HashMap<>();
        Map<CassandraServer, Exception> aliveButInvalidPartitionerNodes = new HashMap<>();
        boolean thisHostResponded = false;
        boolean atLeastOneHostResponded = false;
        for (CassandraServer cassandraServer : getCachedServers()) {
            thisHostResponded = false;
            try {
                runOnCassandraNode(cassandraServer, CassandraVerifier.healthCheck);
                thisHostResponded = true;
                atLeastOneHostResponded = true;
            } catch (Exception e) {
                completelyUnresponsiveNodes.put(cassandraServer, e);
                blacklist.add(cassandraServer);
            }

            if (thisHostResponded) {
                try {
                    runOnCassandraNode(cassandraServer, getValidatePartitioner());
                } catch (Exception e) {
                    aliveButInvalidPartitionerNodes.put(cassandraServer, e);
                }
            }
        }

        StringBuilder errorBuilderForEntireCluster = new StringBuilder();
        if (completelyUnresponsiveNodes.size() > 0) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append(" determined that the following hosts are unreachable for the following reasons: \n");
            completelyUnresponsiveNodes.forEach((host, exception) -> errorBuilderForEntireCluster.append(String.format(
                    "\tHost: %s was marked unreachable" + " via exception: %s%n",
                    host.proxy().getHostString(), exception.toString())));
        }

        if (aliveButInvalidPartitionerNodes.size() > 0) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append("determined that the following hosts were alive but are configured")
                    .append("with an invalid partitioner: \n");
            aliveButInvalidPartitionerNodes.forEach(
                    (host, exception) -> errorBuilderForEntireCluster.append(String.format(
                            "\tHost: %s was marked as invalid partitioner" + " via exception: %s%n",
                            host.proxy().getHostString(), exception.toString())));
        }

        if (atLeastOneHostResponded && aliveButInvalidPartitionerNodes.size() == 0) {
            return;
        } else {
            throw new RuntimeException(errorBuilderForEntireCluster.toString());
        }
    }

    @Override
    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runWithRetryOnHost(cassandra.getRandomGoodHost().getCassandraNode(), fn);
    }

    @Override
    public <V, K extends Exception> V runWithRetryOnHost(
            CassandraServer specifiedHost, FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        RetryableCassandraRequest<V, K> req = new RetryableCassandraRequest<>(specifiedHost, fn);

        while (true) {
            if (log.isTraceEnabled()) {
                log.trace(
                        "Running function on host {}.",
                        SafeArg.of("host", CassandraLogHelper.cassandraHost(req.getCassandraNode())));
            }
            CassandraClientPoolingContainer hostPool = getPreferredHostOrFallBack(req);

            try {
                V response = runWithPooledResourceRecordingMetrics(hostPool, req.getFunction());
                removeFromBlacklistAfterResponse(hostPool.getCassandraNode());
                return response;
            } catch (Exception ex) {
                exceptionHandler.handleExceptionFromRequest(req, hostPool.getCassandraNode(), ex);
            }
        }
    }

    private <V, K extends Exception> CassandraClientPoolingContainer getPreferredHostOrFallBack(
            RetryableCassandraRequest<V, K> req) {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(req.getCassandraNode());

        if (blacklist.contains(req.getCassandraNode()) || hostPool == null || req.shouldGiveUpOnPreferredHost()) {
            CassandraServer previousHost = hostPool == null ? req.getCassandraNode() : hostPool.getCassandraNode();
            Optional<CassandraClientPoolingContainer> hostPoolCandidate = cassandra.getRandomGoodHostForPredicate(
                    address -> !req.alreadyTriedOnHost(address), req.getTriedHosts());
            hostPool = hostPoolCandidate.orElseGet(cassandra::getRandomGoodHost);
            log.warn(
                    "Randomly redirected a query intended for host {} to {}.",
                    SafeArg.of("previousHost", CassandraLogHelper.cassandraHost(previousHost)),
                    SafeArg.of("randomHost", CassandraLogHelper.cassandraHost(hostPool.getCassandraNode())));
        }
        return hostPool;
    }

    @Override
    public <V, K extends Exception> V run(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runOnCassandraNode(cassandra.getRandomGoodHost().getCassandraNode(), fn);
    }

    public <V, K extends Exception> V runOnCassandraNode(
            CassandraServer specifiedHost, FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(specifiedHost);
        V response = runWithPooledResourceRecordingMetrics(hostPool, fn);
        removeFromBlacklistAfterResponse(specifiedHost);
        return response;
    }

    private void removeFromBlacklistAfterResponse(CassandraServer cassandraServer) {
        if (blacklist.contains(cassandraServer)) {
            blacklist.remove(cassandraServer);
            log.info(
                    "Added cassandraServer {} back into the pool after receiving a successful response",
                    SafeArg.of("cassandraServer", CassandraLogHelper.cassandraHost(cassandraServer)));
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
        Multimap<Set<TokenRange>, CassandraServer> tokenRangesToHost = HashMultimap.create();
        for (CassandraServer host : getCachedServers()) {
            try (CassandraClient client = CassandraClientFactory.getClientInternal(host.proxy(), config)) {
                try {
                    client.describe_keyspace(config.getKeyspaceOrThrow());
                } catch (NotFoundException e) {
                    return; // don't care to check for ring consistency when we're not even fully initialized
                }
                tokenRangesToHost.put(ImmutableSet.copyOf(client.describe_ring(config.getKeyspaceOrThrow())), host);
            } catch (Exception e) {
                log.warn(
                        "Failed to get ring info from host: {}",
                        SafeArg.of("host", CassandraLogHelper.cassandraHost(host)),
                        e);
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
                "Hosts have differing ring descriptions." + " This can lead to inconsistent reads and lost data. ");
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
                                CassandraLogHelper.cassandraHost(Iterables.getFirst(entry.getValue(), null));
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
