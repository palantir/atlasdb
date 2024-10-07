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
import com.palantir.atlasdb.tracing.Tracing;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.concurrent.InitializeableScheduledExecutorServiceSupplier;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.streams.KeyedStream;
import com.palantir.exception.CassandraAllHostsUnresponsiveException;
import com.palantir.exception.CassandraInvalidPartitionerException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tracing.CloseableTracer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import one.util.streamex.EntryStream;

/**
 * Feature breakdown:
 * - Pooling
 * - Token Aware Mapping / Query Routing / Data partitioning
 * - Retriable Queries
 * - Pool member error tracking / blacklisting*
 * - Pool refreshing
 * - Pool node autodiscovery
 * - Pool member health checking*
 * <p>
 * *entirely new features
 * <p>
 * By our old system, this would be a
 * RefreshingRetriableTokenAwareHealthCheckingManyHostCassandraClientPoolingContainerManager;
 * ... this is one of the reasons why there is a new system.
 **/
@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class CassandraClientPoolImpl implements CassandraClientPool {
    // 2^7 is 64, which is close enough to a minute when interpreted as seconds
    public static final int MAX_ATTEMPTS_BEFORE_CAPPING_BACKOFF = 7;
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

    private volatile ScheduledFuture<?> refreshPoolFuture;

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
        CassandraClientPoolMetrics cassandraClientPoolMetrics = new CassandraClientPoolMetrics(metricsManager);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                config,
                runtimeConfig,
                startupChecks,
                exceptionHandler,
                blacklist,
                cassandraClientPoolMetrics,
                cassandraTopologyValidator,
                absentHostTracker,
                CassandraService.createForTests(
                        metricsManager, config, runtimeConfig, blacklist, cassandraClientPoolMetrics));
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
        CassandraClientPoolMetrics cassandraClientPoolMetrics = new CassandraClientPoolMetrics(metricsManager);
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(
                config,
                runtimeConfig,
                StartupChecks.RUN,
                exceptionHandler,
                blacklist,
                new CassandraClientPoolMetrics(metricsManager),
                CassandraTopologyValidator.create(
                        CassandraTopologyValidationMetrics.of(metricsManager.getTaggedRegistry()), runtimeConfig),
                new CassandraAbsentHostTracker(config.consecutiveAbsencesBeforePoolRemoval()),
                CassandraService.create(metricsManager, config, runtimeConfig, blacklist, cassandraClientPoolMetrics));
        cassandraClientPool.wrapper.initialize(initializeAsync);
        return cassandraClientPool.wrapper.isInitialized() ? cassandraClientPool : cassandraClientPool.wrapper;
    }

    private CassandraClientPoolImpl(
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            CassandraRequestExceptionHandler exceptionHandler,
            Blacklist blacklist,
            CassandraClientPoolMetrics metrics,
            CassandraTopologyValidator cassandraTopologyValidator,
            CassandraAbsentHostTracker absentHostTracker,
            CassandraService cassandra) {
        this(
                config,
                runtimeConfig,
                startupChecks,
                SHARED_EXECUTOR_SUPPLIER,
                exceptionHandler,
                blacklist,
                cassandra,
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
        CassandraVerifierConfig verifierConfig = CassandraVerifierConfig.of(config, runtimeConfig.get());
        // for testability, mock/spy are bad at mockability of things called in constructors
        if (startupChecks == StartupChecks.RUN) {
            ensureKeyspaceExists(verifierConfig);
        }

        ImmutableMap<CassandraServer, CassandraServerOrigin> initialServers =
                cassandra.getCurrentServerListFromConfig();
        ImmutableSet<CassandraServer> cassandraServers = initialServers.keySet();

        cassandra.cacheInitialHostsForCalculatingPoolNumber(cassandraServers);

        setServersInPoolTo(initialServers);
        Set<CassandraServer> validatedServers = getCachedServers();
        logStartupValidationResults(cassandraServers, validatedServers);

        if (startupChecks == StartupChecks.RUN) {
            ensureKeyspaceIsUpToDate(verifierConfig);
            removeUnreachableHostsAndRequireValidPartitioner();
        }
        runAndScheduleNextRefresh(0);
        metrics.registerAggregateMetrics(blacklist::size);
    }

    private void runAndScheduleNextRefresh(int consecutivelyFailedAttempts) {
        try {
            refreshPool();
        } catch (Throwable t) {
            log.warn(
                    "Failed to refresh Cassandra KVS pool."
                            + " Extended periods of being unable to refresh will cause perf degradation.",
                    t);
        }

        scheduleNextRefresh(consecutivelyFailedAttempts);
    }

    private void scheduleNextRefresh(int consecutivelyFailedAttempts) {
        if (getCurrentPools().isEmpty()) {
            int maxShift = Math.min(MAX_ATTEMPTS_BEFORE_CAPPING_BACKOFF, consecutivelyFailedAttempts);

            // Caps out at 2^7 * 1000 = 64000
            long millisTillNextRefresh =
                    Math.max(1, ThreadLocalRandom.current().nextLong(TimeUnit.SECONDS.toMillis(1L << maxShift)));
            refreshPoolFuture = refreshDaemon.schedule(
                    () -> runAndScheduleNextRefresh(consecutivelyFailedAttempts + 1),
                    millisTillNextRefresh,
                    TimeUnit.MILLISECONDS);
            log.error(
                    "There are no pools remaining after refreshing and validating pools. Scheduling the next refresh"
                            + " very soon to avoid an extended downtime.",
                    SafeArg.of("consecutivelyFailedAttempts", consecutivelyFailedAttempts),
                    SafeArg.of("millisTillNextRefresh", millisTillNextRefresh));

        } else {
            refreshPoolFuture = refreshDaemon.schedule(
                    () -> runAndScheduleNextRefresh(0), config.poolRefreshIntervalSeconds(), TimeUnit.SECONDS);
        }
    }

    private static void logStartupValidationResults(
            ImmutableSet<CassandraServer> cassandraServers, Set<CassandraServer> validatedServers) {
        // The validatedServers.empty() case throws in the setServersInPoolTo directly, so we don't need to handle that
        // here explicitly.
        if (validatedServers.size() != cassandraServers.size()) {
            log.warn(
                    "Validated servers on startup, but only a proper subset of servers succeeded validation",
                    SafeArg.of("initialHosts", CassandraLogHelper.collectionOfHosts(cassandraServers)),
                    SafeArg.of("hostsPassingValidation", CassandraLogHelper.collectionOfHosts(validatedServers)));
        } else {
            log.info(
                    "Validated servers on startup. Successfully added all servers from config to pools",
                    SafeArg.of("hostsPassingValidation", CassandraLogHelper.collectionOfHosts(validatedServers)));
        }
    }

    private void cleanUpOnInitFailure() {
        cancelRefreshPoolTaskIfRunning();
        closeCassandraClientPools();
        cassandra.getPools().clear();
        cassandra.clearInitialCassandraHosts();
    }

    private static CassandraRequestExceptionHandler testExceptionHandler(Blacklist blacklist) {
        return CassandraRequestExceptionHandler.withNoBackoffForTest(
                CassandraClientPoolImpl::getMaxRetriesPerHost, CassandraClientPoolImpl::getMaxTriesTotal, blacklist);
    }

    @Override
    public void shutdown() {
        cancelRefreshPoolTaskIfRunning();
        cassandra.close();
        closeCassandraClientPools();
    }

    private void cancelRefreshPoolTaskIfRunning() {
        // It's possible for the cancelled refreshPoolFuture to schedule another task before terminating. If it does
        // schedule another task, we can just cancel that one too.
        while (refreshPoolFuture != null && !refreshPoolFuture.isCancelled()) {
            refreshPoolFuture.cancel(true);
        }
    }

    private void closeCassandraClientPools() {
        try {
            cassandra
                    .getPools()
                    .forEach((address, cassandraClientPoolingContainer) ->
                            cassandraClientPoolingContainer.shutdownPooling());
            absentHostTracker.shutDown();
        } catch (RuntimeException e) {
            log.warn("Failed to close Cassandra client pools. Some pools may be leaked.", e);
            throw e;
        }
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
    void setServersInPoolTo(ImmutableMap<CassandraServer, CassandraServerOrigin> desiredServers) {
        Set<CassandraServer> currentServers = getCachedServers();
        Map<CassandraServer, CassandraServerOrigin> serversToAdd = EntryStream.of(desiredServers)
                .removeKeys(currentServers::contains)
                .toImmutableMap();

        Set<CassandraServer> validatedServersToAdd =
                validateNewHostsTopologiesAndMaybeAddToPool(getCurrentPools(), serversToAdd);

        SetView<CassandraServer> absentServers = Sets.difference(currentServers, desiredServers.keySet());
        absentServers.forEach(cassandraServer -> {
            CassandraClientPoolingContainer container = cassandra.removePool(cassandraServer);
            absentHostTracker.trackAbsentCassandraServer(cassandraServer, container);
        });

        Set<CassandraServer> serversToShutdown = absentHostTracker.incrementAbsenceAndRemove();

        if (!(validatedServersToAdd.isEmpty() && absentServers.isEmpty())) { // if we made any changes
            cassandra.refreshTokenRangesAndGetServers();
            metrics.recordPoolSize(getCurrentPools().size());
        }

        Preconditions.checkState(
                !getCurrentPools().isEmpty() || serversToAdd.isEmpty(),
                "No servers were successfully added to the pool. This means we could not come to a consensus on"
                    + " cluster topology, and the client cannot connect as there are no valid hosts. This state should"
                    + " be transient (<5 minutes), and if it is not, indicates that the user may have accidentally"
                    + " configured AtlasDB to use two separate Cassandra clusters (i.e., user-led split brain).",
                SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAdd.keySet())));

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
            Map<CassandraServer, CassandraServerOrigin> serversToAdd) {
        if (serversToAdd.isEmpty()) {
            return Set.of();
        }

        Set<CassandraServer> serversToAddWithoutOrigin = serversToAdd.keySet();
        Preconditions.checkArgument(
                Sets.intersection(currentContainers.keySet(), serversToAddWithoutOrigin)
                        .isEmpty(),
                "The current pool of servers should not have any server(s) that are being added. This is unexpected"
                        + " and could lead to undefined behavior, as we should not be validating already validated"
                        + " servers. This suggests a bug in the calling method.",
                SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAddWithoutOrigin)),
                SafeArg.of("currentServers", CassandraLogHelper.collectionOfHosts(currentContainers.keySet())));

        Map<CassandraServer, CassandraClientPoolingContainer> serversToAddContainers =
                getContainerForNewServers(serversToAddWithoutOrigin);
        Map<CassandraServer, CassandraClientPoolingContainer> allContainers =
                ImmutableMap.<CassandraServer, CassandraClientPoolingContainer>builder()
                        .putAll(serversToAddContainers)
                        .putAll(currentContainers)
                        .buildOrThrow();
        Set<CassandraServer> newHostsWithDifferingTopology =
                tryGettingNewHostsWithDifferentTopologyOrInvalidateNewContainersAndThrow(serversToAdd, allContainers);

        Set<CassandraServer> validatedServersToAdd =
                Sets.difference(serversToAddWithoutOrigin, newHostsWithDifferingTopology);
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

    @VisibleForTesting
    Set<CassandraServer> tryGettingNewHostsWithDifferentTopologyOrInvalidateNewContainersAndThrow(
            Map<CassandraServer, CassandraServerOrigin> serversToAdd,
            Map<CassandraServer, CassandraClientPoolingContainer> allContainers) {
        try {
            // Max duration is one minute as we expect the cluster to have recovered by then due to gossip.
            return cassandraTopologyValidator.getNewHostsWithInconsistentTopologiesAndRetry(
                    serversToAdd, allContainers, Duration.ofSeconds(5), Duration.ofMinutes(1));
        } catch (Throwable t) {
            EntryStream.of(allContainers)
                    .filterKeys(serversToAdd::containsKey)
                    .values()
                    .forEach(CassandraClientPoolImpl::tryShuttingDownCassandraClientPoolingContainer);
            log.warn("Failed to get new hosts with inconsistent topologies.", t);
            throw t;
        }
    }

    public static void tryShuttingDownCassandraClientPoolingContainer(CassandraClientPoolingContainer container) {
        try {
            container.shutdownPooling();
        } catch (Throwable t) {
            log.warn(
                    "Failed to close Cassandra client pooling container. It might be leaked now.",
                    SafeArg.of("cassandraServer", container.getCassandraServer()),
                    t);
        }
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

    private void ensureKeyspaceExists(CassandraVerifierConfig verifierConfig) {
        try {
            CassandraVerifier.createKeyspace(verifierConfig);
        } catch (Exception e) {
            log.error("Startup checks failed, was not able to create the keyspace or ensure it already existed.", e);
            throw new RuntimeException(e);
        }
    }

    private void ensureKeyspaceIsUpToDate(CassandraVerifierConfig verifierConfig) {
        try {
            CassandraVerifier.updateExistingKeyspace(this, verifierConfig);
        } catch (Exception e) {
            log.error("Startup checks failed, was not able to update existing keyspace.", e);
            throw new RuntimeException(e);
        }
    }

    private void removeUnreachableHostsAndRequireValidPartitioner() {
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
        if (!completelyUnresponsiveNodes.isEmpty()) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append(" determined that the following hosts are unreachable for the following reasons: \n");
            completelyUnresponsiveNodes.forEach(
                    (cassandraServer, exception) -> errorBuilderForEntireCluster.append(String.format(
                            "\tServer: %s was marked unreachable via proxy: %s, with exception: %s%n",
                            cassandraServer.cassandraHostName(),
                            CassandraLogHelper.host(cassandraServer.proxy()),
                            exception.toString())));
        }

        if (!atLeastOneHostResponded) {
            throw new CassandraAllHostsUnresponsiveException(errorBuilderForEntireCluster.toString());
        }

        if (!aliveButInvalidPartitionerNodes.isEmpty()) {
            errorBuilderForEntireCluster
                    .append("Performing routine startup checks,")
                    .append("determined that the following hosts were alive but are configured")
                    .append("with an invalid partitioner: \n");
            aliveButInvalidPartitionerNodes.forEach(
                    (host, exception) -> errorBuilderForEntireCluster.append(String.format(
                            "\tHost: %s was marked as invalid partitioner" + " via exception: %s%n",
                            host.cassandraHostName(), exception.toString())));
            throw new CassandraInvalidPartitionerException(errorBuilderForEntireCluster.toString());
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
            try (CloseableTracer trace =
                    Tracing.startLocalTrace("CassandraClientPoolImpl.runWithRetryOnServer", tagConsumer -> {
                        tagConsumer.integer("try", req.getNumberOfAttempts());
                    })) {
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
    }

    private <V, K extends Exception> CassandraClientPoolingContainer getPreferredHostOrFallBack(
            RetryableCassandraRequest<V, K> req) {
        try (CloseableTracer trace =
                Tracing.startLocalTrace("CassandraClientPoolImpl.getPreferredHostOfFallBack", tagConsumer -> {})) {
            CassandraClientPoolingContainer hostPool = cassandra.getPools().get(req.getCassandraServer());

            if (blacklist.contains(req.getCassandraServer()) || hostPool == null || req.shouldGiveUpOnPreferredHost()) {
                CassandraServer previousHost =
                        hostPool == null ? req.getCassandraServer() : hostPool.getCassandraServer();
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

        try (CloseableTracer trace = Tracing.startLocalTrace(
                "CassandraClientPoolImpl.runWithPooledResourceRecordingMetrics", tagConsumer -> {
                    tagConsumer.accept("host", hostPool.getCassandraServer().cassandraHostName());
                })) {
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
