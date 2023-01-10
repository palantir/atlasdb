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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.CassandraTopologyValidationMetrics;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraVerifier.CassandraVerifierConfig;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.concurrent.InitializeableScheduledExecutorServiceSupplier;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    private final Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig;
    private final StartupChecks startupChecks;
    private final ScheduledExecutorService refreshDaemon;
    private final CassandraClientPoolMetrics metrics;
    private final InitializingWrapper wrapper = new InitializingWrapper();
    private final CassandraAbsentHostTracker absentHostTracker;
    private final CassandraTopologyValidator cassandraTopologyValidator;

    private ScheduledFuture<?> refreshPoolFuture;

    @VisibleForTesting
    static CassandraClientPoolImpl createImplForTest(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            Blacklist blacklist,
            CassandraTopologyValidator cassandraTopologyValidator,
            CassandraAbsentHostTracker absentHostTracker) {
        CassandraRequestExceptionHandler exceptionHandler = testExceptionHandler(blacklist);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                metricsManager,
                config,
                runtimeConfig,
                startupChecks,
                exceptionHandler,
                blacklist,
                new CassandraClientPoolMetrics(metricsManager),
                cassandraTopologyValidator,
                absentHostTracker);
        cassandraClientPool.wrapper.initialize(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        return cassandraClientPool;
    }

    @VisibleForTesting
    static CassandraClientPoolImpl createImplForTest(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            InitializeableScheduledExecutorServiceSupplier initializeableExecutorSupplier,
            Blacklist blacklist,
            CassandraService cassandra,
            CassandraTopologyValidator cassandraTopologyValidator,
            CassandraAbsentHostTracker cassandraAbsentHostTracker) {
        CassandraRequestExceptionHandler exceptionHandler = testExceptionHandler(blacklist);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                config,
                runtimeConfig,
                startupChecks,
                initializeableExecutorSupplier,
                exceptionHandler,
                blacklist,
                cassandra,
                new CassandraClientPoolMetrics(metricsManager),
                cassandraTopologyValidator,
                cassandraAbsentHostTracker);
        cassandraClientPool.wrapper.initialize(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        return cassandraClientPool;
    }

    public static CassandraClientPool create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            boolean initializeAsync) {
        Blacklist blacklist = new Blacklist(
                config, runtimeConfig.map(CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds));
        CassandraRequestExceptionHandler exceptionHandler = new CassandraRequestExceptionHandler(
                () -> runtimeConfig.get().numberOfRetriesOnSameHost(),
                () -> runtimeConfig.get().numberOfRetriesOnAllHosts(),
                () -> runtimeConfig.get().conservativeRequestExceptionHandler(),
                blacklist);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                metricsManager,
                config,
                runtimeConfig,
                StartupChecks.RUN,
                exceptionHandler,
                blacklist,
                new CassandraClientPoolMetrics(metricsManager),
                new CassandraTopologyValidator(
                        CassandraTopologyValidationMetrics.of(metricsManager.getTaggedRegistry())),
                new CassandraAbsentHostTracker(config.consecutiveAbsencesBeforePoolRemoval()));
        cassandraClientPool.wrapper.initialize(initializeAsync);
        return cassandraClientPool.wrapper.isInitialized() ? cassandraClientPool : cassandraClientPool.wrapper;
    }

    private CassandraClientPoolImpl(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            CassandraRequestExceptionHandler exceptionHandler,
            Blacklist blacklist,
            CassandraClientPoolMetrics metrics,
            CassandraTopologyValidator cassandraTopologyValidator,
            CassandraAbsentHostTracker absentHostTracker) {
        this(
                config,
                runtimeConfig,
                startupChecks,
                SHARED_EXECUTOR_SUPPLIER,
                exceptionHandler,
                blacklist,
                new CassandraService(metricsManager, config, runtimeConfig, blacklist, metrics),
                metrics,
                cassandraTopologyValidator,
                absentHostTracker);
    }

    private CassandraClientPoolImpl(
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            InitializeableScheduledExecutorServiceSupplier initializeableExecutorSupplier,
            CassandraRequestExceptionHandler exceptionHandler,
            Blacklist blacklist,
            CassandraService cassandra,
            CassandraClientPoolMetrics metrics,
            CassandraTopologyValidator cassandraTopologyValidator,
            CassandraAbsentHostTracker absentHostTracker) {
        this.config = config;
        this.runtimeConfig = runtimeConfig;
        this.startupChecks = startupChecks;
        initializeableExecutorSupplier.initialize(config.numPoolRefreshingThreads());
        this.refreshDaemon = initializeableExecutorSupplier.get();
        this.blacklist = blacklist;
        this.exceptionHandler = exceptionHandler;
        this.cassandra = cassandra;
        this.metrics = metrics;
        this.absentHostTracker = absentHostTracker;
        this.cassandraTopologyValidator = cassandraTopologyValidator;
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
        if (refreshPoolFuture != null) {
            refreshPoolFuture.cancel(true);
        }
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
        if (refreshPoolFuture != null) {
            refreshPoolFuture.cancel(true);
        }
        cassandra.close();
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
    RangeMap<LightweightOppToken, ImmutableSet<CassandraServer>> getTokenMap() {
        return cassandra.getTokenMap();
    }

    @VisibleForTesting
    Set<CassandraServer> getLocalHosts() {
        return cassandra.getLocalHosts();
    }

    private synchronized void refreshPool() {
        blacklist.checkAndUpdate(cassandra.getPools());

        if (config.autoRefreshNodes()) {
            setServersInPoolTo(cassandra.refreshTokenRangesAndGetServers());
        } else {
            setServersInPoolTo(cassandra.getCurrentServerListFromConfig());
        }

        cassandra.debugLogStateOfPool();
    }

    @VisibleForTesting
    void setServersInPoolTo(ImmutableSet<CassandraServer> desiredServers) {
        Set<CassandraServer> currentServers = getCachedServers();
        SetView<CassandraServer> serversToAdd = Sets.difference(desiredServers, currentServers);
        SetView<CassandraServer> absentServers = Sets.difference(currentServers, desiredServers);

        absentServers.forEach(cassandraServer -> {
            CassandraClientPoolingContainer container = cassandra.removePool(cassandraServer);
            absentHostTracker.trackAbsentCassandraServer(cassandraServer, container);
        });

        Set<CassandraServer> serversToShutdown = absentHostTracker.incrementAbsenceAndRemove();

        Set<CassandraServer> validatedServersToAdd =
                validateNewHostsTopologiesAndMaybeAddToPool(getCurrentPools(), serversToAdd);

        if (!(validatedServersToAdd.isEmpty() && absentServers.isEmpty())) { // if we made any changes
            cassandra.refreshTokenRangesAndGetServers();
        }

        Preconditions.checkState(
                !getCurrentPools().isEmpty() || serversToAdd.isEmpty(),
                "No servers were successfully added to the pool. This means we could not come to a consensus on"
                    + " cluster topology, and the client cannot connect as there are no valid hosts. This state should"
                    + " be transient (<5 minutes), and if it is not, indicates that the user may have accidentally"
                    + " configured AltasDB to use two separate Cassandra clusters (i.e., user-led split brain).",
                SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAdd)));

        logRefreshedHosts(validatedServersToAdd, serversToShutdown, absentServers);
    }

    /**
     * Validates new servers to add to the cassandra client container pool,
     * by checking them with the {@link com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator}.
     * If any servers come back and are not in consensus this is OK, we will simply add them to the absent host
     * tracker, as we most likely will retry this host in subsequent calls.
     *
     * @return The set of cassandra servers which have valid topologies and have been added to the pool.
     */
    @VisibleForTesting
    Set<CassandraServer> validateNewHostsTopologiesAndMaybeAddToPool(
            Map<CassandraServer, CassandraClientPoolingContainer> currentContainers,
            Set<CassandraServer> serversToAdd) {
        if (serversToAdd.isEmpty()) {
            return Set.of();
        }

        Map<CassandraServer, CassandraClientPoolingContainer> serversToAddContainers =
                getContainerForNewServers(serversToAdd);

        Preconditions.checkArgument(
                Sets.intersection(currentContainers.keySet(), serversToAddContainers.keySet())
                        .isEmpty(),
                "The current pool of servers should not have any server(s) that are being added. This is unexpected"
                        + " and could lead to undefined behavior, as we should not be validating already validated"
                        + " servers. This suggests a bug in the calling method.",
                SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAddContainers.keySet())),
                SafeArg.of("currentServers", CassandraLogHelper.collectionOfHosts(currentContainers.keySet())));

        Map<CassandraServer, CassandraClientPoolingContainer> allContainers =
                ImmutableMap.<CassandraServer, CassandraClientPoolingContainer>builder()
                        .putAll(serversToAddContainers)
                        .putAll(currentContainers)
                        .buildOrThrow();

        // Max duration is one minute as we expect the cluster to have recovered by then due to gossip.
        Set<CassandraServer> newHostsWithDifferingTopology =
                cassandraTopologyValidator.getNewHostsWithInconsistentTopologiesAndRetry(
                        serversToAdd, allContainers, Duration.ofSeconds(5), Duration.ofMinutes(1));

        Set<CassandraServer> validatedServersToAdd = Sets.difference(serversToAdd, newHostsWithDifferingTopology);
        validatedServersToAdd.forEach(server -> cassandra.addPool(server, serversToAddContainers.get(server)));
        newHostsWithDifferingTopology.forEach(
                server -> absentHostTracker.trackAbsentCassandraServer(server, serversToAddContainers.get(server)));
        if (!newHostsWithDifferingTopology.isEmpty()) {
            log.warn(
                    "Some hosts are potentially unreachable or have topologies that are not in consensus with the"
                            + " majority (if startup) or the current servers in the pool (if refreshing).",
                    SafeArg.of(
                            "hostsWithDifferingTopology",
                            CassandraLogHelper.collectionOfHosts(newHostsWithDifferingTopology)));
        }
        return validatedServersToAdd;
    }

    private Map<CassandraServer, CassandraClientPoolingContainer> getContainerForNewServers(
            Set<CassandraServer> newServers) {
        return KeyedStream.of(newServers)
                .map(server -> absentHostTracker.returnPool(server).orElseGet(() -> cassandra.createPool(server)))
                .collectToMap();
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

    private ImmutableSet<CassandraServer> getCachedServers() {
        return ImmutableSet.copyOf(cassandra.getPools().keySet());
    }

    @Override
    public CassandraServer getRandomServerForKey(byte[] key) {
        return cassandra.getRandomCassandraNodeForKey(key);
    }

    @VisibleForTesting
    void runOneTimeStartupChecks() {
        CassandraVerifierConfig verifierConfig = CassandraVerifierConfig.of(config, runtimeConfig.get());
        try {
            CassandraVerifier.ensureKeyspaceExistsAndIsUpToDate(this, verifierConfig);
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
                runOnCassandraServer(cassandraServer, CassandraVerifier.healthCheck);
                thisHostResponded = true;
                atLeastOneHostResponded = true;
            } catch (Exception e) {
                completelyUnresponsiveNodes.put(cassandraServer, e);
                blacklist.add(cassandraServer);
            }

            if (thisHostResponded) {
                try {
                    runOnCassandraServer(cassandraServer, getValidatePartitioner());
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
            completelyUnresponsiveNodes.forEach(
                    (cassandraServer, exception) -> errorBuilderForEntireCluster.append(String.format(
                            "\tServer: %s was marked unreachable via proxy: %s, with exception: %s%n",
                            cassandraServer.cassandraHostName(),
                            cassandraServer.proxy().getHostString(),
                            exception.toString())));
        }

        if (aliveButInvalidPartitionerNodes.size() > 0) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append("determined that the following hosts were alive but are configured")
                    .append("with an invalid partitioner: \n");
            aliveButInvalidPartitionerNodes.forEach(
                    (host, exception) -> errorBuilderForEntireCluster.append(String.format(
                            "\tHost: %s was marked as invalid partitioner" + " via exception: %s%n",
                            host.cassandraHostName(), exception.toString())));
        }

        if (atLeastOneHostResponded && aliveButInvalidPartitionerNodes.size() == 0) {
            return;
        } else {
            throw new RuntimeException(errorBuilderForEntireCluster.toString());
        }
    }

    @Override
    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runWithRetryOnServer(cassandra.getRandomGoodHost().getCassandraServer(), fn);
    }

    @Override
    public <V, K extends Exception> V runWithRetryOnServer(
            CassandraServer specifiedServer, FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        RetryableCassandraRequest<V, K> req = new RetryableCassandraRequest<>(specifiedServer, fn);

        while (true) {
            if (log.isTraceEnabled()) {
                log.trace("Running function on host {}.", SafeArg.of("server", req.getCassandraServer()));
            }
            CassandraClientPoolingContainer hostPool = getPreferredHostOrFallBack(req);

            try {
                V response = runWithPooledResourceRecordingMetrics(hostPool, req.getFunction());
                removeFromBlacklistAfterResponse(hostPool.getCassandraServer());
                return response;
            } catch (Exception ex) {
                exceptionHandler.handleExceptionFromRequest(req, hostPool.getCassandraServer(), ex);
            }
        }
    }

    private <V, K extends Exception> CassandraClientPoolingContainer getPreferredHostOrFallBack(
            RetryableCassandraRequest<V, K> req) {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(req.getCassandraServer());

        if (blacklist.contains(req.getCassandraServer()) || hostPool == null || req.shouldGiveUpOnPreferredHost()) {
            CassandraServer previousHost = hostPool == null ? req.getCassandraServer() : hostPool.getCassandraServer();
            Optional<CassandraClientPoolingContainer> hostPoolCandidate = cassandra.getRandomGoodHostForPredicate(
                    address -> !req.alreadyTriedOnHost(address), req.getTriedHosts());
            hostPool = hostPoolCandidate.orElseGet(cassandra::getRandomGoodHost);
            log.warn(
                    "Randomly redirected a query intended for host {} to {}.",
                    SafeArg.of("previousHost", previousHost),
                    SafeArg.of("randomHost", hostPool.getCassandraServer()));
        }
        return hostPool;
    }

    @Override
    public <V, K extends Exception> V run(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runOnCassandraServer(cassandra.getRandomGoodHost().getCassandraServer(), fn);
    }

    @Override
    public <V, K extends Exception> V runOnCassandraServer(
            CassandraServer specifiedServer, FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(specifiedServer);
        V response = runWithPooledResourceRecordingMetrics(hostPool, fn);
        removeFromBlacklistAfterResponse(specifiedServer);
        return response;
    }

    private void removeFromBlacklistAfterResponse(CassandraServer cassandraServer) {
        if (blacklist.contains(cassandraServer)) {
            blacklist.remove(cassandraServer);
            log.info(
                    "Added cassandraServer {} back into the pool after receiving a successful response",
                    SafeArg.of("cassandraServer", cassandraServer));
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

    @Override
    public FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner() {
        return CassandraUtils.getValidatePartitioner(config);
    }

    public enum StartupChecks {
        RUN,
        DO_NOT_RUN
    }
}
