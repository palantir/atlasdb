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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraService;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.processors.AutoDelegate;

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
@AutoDelegate(typeToExtend = CassandraClientPool.class)
public class CassandraClientPoolImpl implements CassandraClientPool {
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

    private final Blacklist blacklist;
    private final CassandraRequestExceptionHandler exceptionHandler;
    private final CassandraService cassandra;

    private final CassandraKeyValueServiceConfig config;
    private final StartupChecks startupChecks;
    private final ScheduledExecutorService refreshDaemon;
    private final CassandraClientPoolMetrics metrics = new CassandraClientPoolMetrics();
    private final InitializingWrapper wrapper = new InitializingWrapper();

    private ScheduledFuture<?> refreshPoolFuture;

    @VisibleForTesting
    static CassandraClientPoolImpl createImplForTest(CassandraKeyValueServiceConfig config,
            StartupChecks startupChecks,
            Blacklist blacklist) {
        return create(config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                startupChecks,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                FakeQosClient.INSTANCE,
                blacklist);
    }

    public static CassandraClientPool create(CassandraKeyValueServiceConfig config) {
        return create(config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                FakeQosClient.INSTANCE);
    }

    public static CassandraClientPool create(CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            boolean initializeAsync,
            QosClient qosClient) {
        CassandraClientPoolImpl cassandraClientPool = create(config,
                runtimeConfig,
                StartupChecks.RUN,
                initializeAsync,
                qosClient,
                new Blacklist(config));
        return cassandraClientPool.wrapper.isInitialized() ? cassandraClientPool : cassandraClientPool.wrapper;
    }

    private static CassandraClientPoolImpl create(CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            boolean initializeAsync,
            QosClient qosClient,
            Blacklist blacklist) {
        CassandraClientPoolImpl cassandraClientPool = new CassandraClientPoolImpl(config,
                runtimeConfig,
                startupChecks,
                qosClient,
                blacklist);
        cassandraClientPool.wrapper.initialize(initializeAsync);
        return cassandraClientPool;
    }

    private CassandraClientPoolImpl(
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            StartupChecks startupChecks,
            QosClient qosClient,
            Blacklist blacklist) {
        this.config = config;
        this.startupChecks = startupChecks;
        this.refreshDaemon = PTExecutors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CassandraClientPoolRefresh-%d")
                .build());
        this.blacklist = blacklist;
        this.exceptionHandler = new CassandraRequestExceptionHandler(
                () -> runtimeConfig.get().numberOfRetriesOnSameHost(),
                () -> runtimeConfig.get().numberOfRetriesOnAllHosts(),
                () -> runtimeConfig.get().conservativeRequestExceptionHandler(),
                blacklist);
        cassandra = new CassandraService(config, blacklist, qosClient);
    }

    private void tryInitialize() {
        cassandra.cacheInitialCassandraHosts();

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
        cassandra.getPools().forEach((address, cassandraClientPoolingContainer) ->
                cassandraClientPoolingContainer.shutdownPooling());
        cassandra.getPools().clear();
        cassandra.clearInitialCassandraHosts();
    }

    @Override
    public void shutdown() {
        cassandra.close();
        refreshDaemon.shutdown();
        cassandra.getPools().forEach((address, cassandraClientPoolingContainer) ->
                cassandraClientPoolingContainer.shutdownPooling());
        metrics.deregisterMetrics();
    }

    /**
     * This is the maximum number of times we'll accept connection failures to one host before blacklisting it. Note
     * that subsequent hosts we try in the same call will actually be blacklisted after one connection failure
     */
    @VisibleForTesting
    int getMaxRetriesPerHost() {
        return 3;
    }

    @VisibleForTesting
    int getMaxTriesTotal() {
        return 6;
    }

    @Override
    public Map<InetSocketAddress, CassandraClientPoolingContainer> getCurrentPools() {
        return cassandra.getPools();
    }

    @Override
    public <V> void markWritesForTable(Map<Cell, V> entries, TableReference tableRef) {
        cassandra.markWritesForTable(entries, tableRef);
    }

    @VisibleForTesting
    TokenRangeWritesLogger getTokenRangeWritesLogger() {
        return cassandra.getTokenRangeWritesLogger();
    }

    @VisibleForTesting
    RangeMap<LightweightOppToken, List<InetSocketAddress>> getTokenMap() {
        return cassandra.getTokenMap();
    }

    private synchronized void refreshPool() {
        blacklist.checkAndUpdate(cassandra.getPools());

        Set<InetSocketAddress> serversToAdd = Sets.newHashSet(config.servers());
        Set<InetSocketAddress> serversToRemove = ImmutableSet.of();

        if (config.autoRefreshNodes()) {
            serversToAdd.addAll(cassandra.refreshTokenRanges());
        }

        serversToAdd = Sets.difference(serversToAdd, cassandra.getPools().keySet());

        if (!config.autoRefreshNodes()) { // (we would just add them back in)
            serversToRemove = Sets.difference(cassandra.getPools().keySet(), config.servers());
        }

        serversToAdd.forEach(cassandra::addPool);
        serversToRemove.forEach(cassandra::removePool);

        if (!(serversToAdd.isEmpty() && serversToRemove.isEmpty())) { // if we made any changes
            sanityCheckRingConsistency();
            if (!config.autoRefreshNodes()) { // grab new token mapping, if we didn't already do this before
                cassandra.refreshTokenRanges();
            }
        }

        log.debug("Cassandra pool refresh added hosts {}, removed hosts {}.",
                SafeArg.of("serversToAdd", CassandraLogHelper.collectionOfHosts(serversToAdd)),
                SafeArg.of("serversToRemove", CassandraLogHelper.collectionOfHosts(serversToRemove)));
        cassandra.debugLogStateOfPool();
    }

    @VisibleForTesting
    void addPool(InetSocketAddress server) {
        cassandra.addPool(server);
    }

    @VisibleForTesting
    void removePool(InetSocketAddress server) {
        cassandra.removePool(server);
    }

    @Override
    public InetSocketAddress getRandomHostForKey(byte[] key) {
        return cassandra.getRandomHostForKey(key);
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
        for (InetSocketAddress host : cassandra.getPools().keySet()) {
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

    @Override
    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runWithRetryOnHost(cassandra.getRandomGoodHost().getHost(), fn);
    }

    @Override
    public <V, K extends Exception> V runWithRetryOnHost(
            InetSocketAddress specifiedHost,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        RetryableCassandraRequest<V, K> req = new RetryableCassandraRequest<>(specifiedHost, fn);

        while (true) {
            if (log.isTraceEnabled()) {
                log.trace("Running function on host {}.",
                        SafeArg.of("host", CassandraLogHelper.host(req.getPreferredHost())));
            }
            CassandraClientPoolingContainer hostPool = getPreferredHostOrFallBack(req);

            try {
                return runWithPooledResourceRecordingMetrics(hostPool, req.getFunction());
            } catch (Exception ex) {
                exceptionHandler.handleExceptionFromRequest(req, hostPool.getHost(), ex);
            }
        }
    }

    private <V, K extends Exception> CassandraClientPoolingContainer getPreferredHostOrFallBack(
            RetryableCassandraRequest<V, K> req) {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(req.getPreferredHost());

        if (blacklist.contains(req.getPreferredHost()) || hostPool == null || req.shouldGiveUpOnPreferredHost()) {
            InetSocketAddress previousHost = hostPool == null ? req.getPreferredHost() : hostPool.getHost();
            Optional<CassandraClientPoolingContainer> hostPoolCandidate
                    = cassandra.getRandomGoodHostForPredicate(address -> !req.alreadyTriedOnHost(address));
            hostPool = hostPoolCandidate.orElseGet(cassandra::getRandomGoodHost);
            log.warn("Randomly redirected a query intended for host {} to {}.",
                    SafeArg.of("previousHost", CassandraLogHelper.host(previousHost)),
                    SafeArg.of("randomHost", CassandraLogHelper.host(hostPool.getHost())));
        }
        return hostPool;
    }

    @Override
    public <V, K extends Exception> V run(FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        return runOnHost(cassandra.getRandomGoodHost().getHost(), fn);
    }

    @Override
    public <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K {
        CassandraClientPoolingContainer hostPool = cassandra.getPools().get(specifiedHost);
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
        Multimap<Set<TokenRange>, InetSocketAddress> tokenRangesToHost = HashMultimap.create();
        for (InetSocketAddress host : cassandra.getPools().keySet()) {
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

    @Override
    public FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner() {
        return CassandraUtils.getValidatePartitioner(config);
    }

    @VisibleForTesting
    enum StartupChecks {
        RUN,
        DO_NOT_RUN
    }
}
