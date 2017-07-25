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
package com.palantir.atlasdb.factory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.ClientErrorException;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.config.TimestampClientConfig;
import com.palantir.atlasdb.factory.Leaders.LocalPaxosServices;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.factory.timestamp.DynamicDecoratedTimestampService;
import com.palantir.atlasdb.http.AtlasDbFeignTargetFactory;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.persistentlock.InitialisingPersistentLockService;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.BackgroundSweeperPerformanceLogger;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.ImmutableSweepBatchConfig;
import com.palantir.atlasdb.sweep.InitialisingSweeperService;
import com.palantir.atlasdb.sweep.NoOpBackgroundSweeperPerformanceLogger;
import com.palantir.atlasdb.sweep.PersistentLockManager;
import com.palantir.atlasdb.sweep.SpecificTableSweeper;
import com.palantir.atlasdb.sweep.SweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepMetrics;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweeperServiceImpl;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.RawTransaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManagerInterface;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.InitialisingRemoteLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.timestamp.InitialisingTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public final class TransactionManagers {

    private static final int LOGGING_INTERVAL = 60;
    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);
    public static final LockClient LOCK_CLIENT = LockClient.of("atlas instance");

    @VisibleForTesting
    static Consumer<Runnable> runAsync = task -> {
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
    };

    private TransactionManagers() {
        // Utility class
    }

    /**
     * Accepts a single {@link Schema}.
     *
     * @see TransactionManagers#createInMemory(Set)
     */
    public static SnapshotTransactionManagerInterface createInMemory(Schema schema) {
        return createInMemory(ImmutableSet.of(schema));
    }

    /**
     * Create a {@link SerializableTransactionManager} backed by an
     * {@link com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService}. This should be used for testing
     * purposes only.
     */
    public static SnapshotTransactionManagerInterface createInMemory(Set<Schema> schemas) {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder().keyValueService(new InMemoryAtlasDbConfig()).build();
        return create(config,
                java.util.Optional::empty,
                schemas,
                x -> {
                },
                false);
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configurations, {@link Schema},
     * and an environment in which to register HTTP server endpoints.
     */
    public static SnapshotTransactionManagerInterface create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Schema schema,
            Environment env,
            boolean allowHiddenTableAccess) {
        return create(config, runtimeConfigSupplier, ImmutableSet.of(schema), env, allowHiddenTableAccess);
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configurations, a set of
     * {@link Schema}s, and an environment in which to register HTTP server endpoints.
     */
    public static SnapshotTransactionManagerInterface create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            Environment env,
            boolean allowHiddenTableAccess) {
        log.info("Called TransactionManagers.create. This should only happen once.",
                UnsafeArg.of("thread name", Thread.currentThread().getName()));
        return create(config, runtimeConfigSupplier, schemas, env, LockServerOptions.DEFAULT, allowHiddenTableAccess);
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configurations, a set of
     * {@link Schema}s, {@link LockServerOptions}, and an environment in which to register HTTP server endpoints.
     */
    public static SnapshotTransactionManagerInterface create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess) {
        return create(config, runtimeConfigSupplier, schemas, env, lockServerOptions, allowHiddenTableAccess,
                UserAgents.DEFAULT_USER_AGENT);
    }

    public static SnapshotTransactionManagerInterface create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess,
            Class<?> callingClass) {
        return create(config, runtimeConfigSupplier, schemas, env, lockServerOptions, allowHiddenTableAccess,
                UserAgents.fromClass(callingClass));
    }

    private static SnapshotTransactionManagerInterface create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess,
            String userAgent) {

        checkInstallConfig(config);

        return new InitialisingTransactionManager(config, runtimeConfigSupplier, schemas, env, lockServerOptions, allowHiddenTableAccess,
                userAgent);
    }

    private static void checkInstallConfig(AtlasDbConfig config) {
        if (config.getSweepBatchSize() != null
                || config.getSweepCellBatchSize() != null
                || config.getSweepReadLimit() != null
                || config.getSweepCandidateBatchHint() != null
                || config.getSweepDeleteBatchHint() != null) {
            log.error("Your configuration specifies sweep parameters on the install config. They will be ignored."
                    + " Please use the runtime config to specify them.");
        }
    }

    private static PersistentLockService createPersistentLockService(KeyValueService kvs, Environment env) {
        if (!kvs.supportsCheckAndSet()) {
            return new NoOpPersistentLockService();
        }

        return KvsBackedPersistentLockService.create(kvs);
    }

    /**
     * This method should not be used directly. It remains here to support the AtlasDB-Dagger module and the CLIs, but
     * may be removed at some point in the future.
     *
     * @deprecated Not intended for public use outside of the AtlasDB CLIs
     */
    // TODO gmaretic: fix as below
    @Deprecated
    public static LockAndTimestampServices createLockAndTimestampServices(
            AtlasDbConfig config,
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        LockAndTimestampServices lockAndTimestampServices =
                createRawServices(config,
                        env,
                        lock,
                        time,
                        () -> {
                            log.warn("Note: Automatic migration isn't performed by the CLI tools.");
                            return AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
                        },
                        UserAgents.DEFAULT_USER_AGENT);
        return withRefreshingLockService(lockAndTimestampServices);
    }

    @VisibleForTesting
    static LockAndTimestampServices createLockAndTimestampServices(
            AtlasDbConfig config,
            java.util.function.Supplier<TimestampClientConfig> runtimeConfigSupplier,
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        log.warn("TRING TO CREATE LOCKANDTIMESTAMPSERVICE");
        LockAndTimestampServices lockAndTimestampServices =
                createRawServices(config, env, lock, time, invalidator, userAgent);
        return withRateLimitedTimestampService(
                runtimeConfigSupplier,
                withRefreshingLockService(lockAndTimestampServices));
    }

    private static LockAndTimestampServices withRefreshingLockService(
            LockAndTimestampServices lockAndTimestampServices) {
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .lock(LockRefreshingRemoteLockService.create(lockAndTimestampServices.lock()))
                .build();
    }

    private static LockAndTimestampServices withRateLimitedTimestampService(
            java.util.function.Supplier<TimestampClientConfig> timestampClientConfigSupplier,
            LockAndTimestampServices lockAndTimestampServices) {
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .time(DynamicDecoratedTimestampService.createWithRateLimiting(
                        lockAndTimestampServices.time(),
                        timestampClientConfigSupplier))
                .build();
    }

    @VisibleForTesting
    static LockAndTimestampServices createRawServices(
            AtlasDbConfig config,
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        if (config.leader().isPresent()) {
            return createRawLeaderServices(config.leader().get(), env, lock, time, userAgent);
        } else if (config.timestamp().isPresent() && config.lock().isPresent()) {
            return createRawRemoteServices(config, userAgent);
        } else if (config.timelock().isPresent()) {
            TimeLockClientConfig timeLockClientConfig = config.timelock().get();
            TimeLockMigrator.create(timeLockClientConfig, invalidator, userAgent).migrate();
            return createNamespacedRawRemoteServices(timeLockClientConfig, userAgent);
        } else {
            return createRawEmbeddedServices(env, lock, time, userAgent);
        }
    }

    private static LockAndTimestampServices createNamespacedRawRemoteServices(
            TimeLockClientConfig config,
            String userAgent) {
        ServerListConfig namespacedServerListConfig = config.toNamespacedServerList();
        return getLockAndTimestampServices(namespacedServerListConfig, userAgent);
    }

    private static LockAndTimestampServices getLockAndTimestampServices(
            ServerListConfig timelockServerListConfig,
            String userAgent) {
        RemoteLockService lockService = new ServiceCreator<>(RemoteLockService.class, userAgent)
                .apply(timelockServerListConfig);
        TimestampService timeService = new ServiceCreator<>(TimestampService.class, userAgent)
                .apply(timelockServerListConfig);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .time(timeService)
                .build();
    }

    private static LockAndTimestampServices createRawLeaderServices(
            LeaderConfig leaderConfig,
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time,
            String userAgent) {
        // Create local services, that may or may not end up being registered in an environment.
        LocalPaxosServices localPaxosServices = Leaders.createAndRegisterLocalServices(env, leaderConfig, userAgent);

        LeaderElectionService leader = localPaxosServices.leaderElectionService();
        RemoteLockService localLock = AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, lock, leader);
        TimestampService localTime = AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, time, leader);
//        env.register(localLock);
//        env.register(localTime);

        // Create remote services, that may end up calling our own local services.
        Optional<SSLSocketFactory> sslSocketFactory = ServiceCreator.createSslSocketFactory(
                leaderConfig.sslConfiguration());
        RemoteLockService remoteLock = ServiceCreator.createService(
                sslSocketFactory,
                leaderConfig.leaders(),
                RemoteLockService.class,
                userAgent);
        TimestampService remoteTime = ServiceCreator.createService(
                sslSocketFactory,
                leaderConfig.leaders(),
                TimestampService.class,
                userAgent);

        if (leaderConfig.leaders().size() == 1) {
            // Attempting to connect to ourself while processing a request can lead to deadlock if incoming request
            // volume is high, as all Jetty threads end up waiting for the timestamp server, and no threads remain to
            // actually handle the timestamp server requests. If we are the only single leader, we can avoid the
            // deadlock entirely; so use PingableLeader's getUUID() to detect this situation and eliminate the redundant
            // call.

            PingableLeader localPingableLeader = localPaxosServices.pingableLeader();
            String localServerId = localPingableLeader.getUUID();
            PingableLeader remotePingableLeader = AtlasDbFeignTargetFactory.createRsProxy(
                    sslSocketFactory,
                    Iterables.getOnlyElement(leaderConfig.leaders()),
                    PingableLeader.class,
                    userAgent);

            // Determine asynchronously whether the remote services are talking to our local services.
            CompletableFuture<Boolean> useLocalServicesFuture = new CompletableFuture<>();
            runAsync.accept(() -> {
                int logAfter = LOGGING_INTERVAL;
                while (true) {
                    try {
                        String remoteServerId = remotePingableLeader.getUUID();
                        useLocalServicesFuture.complete(localServerId.equals(remoteServerId));
                        return;
                    } catch (ClientErrorException e) {
                        useLocalServicesFuture.complete(false);
                        return;
                    } catch (Throwable e) {
                        if (--logAfter == 0) {
                            log.warn("Failed to read remote timestamp server ID", e);
                            logAfter = LOGGING_INTERVAL;
                        }
                    }
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }
            });

            // Create dynamic service proxies, that switch to talking directly to our local services if it turns out our
            // remote services are pointed at them anyway.
            RemoteLockService dynamicLockService = LocalOrRemoteProxy.newProxyInstance(
                    RemoteLockService.class, localLock, remoteLock, useLocalServicesFuture);
            TimestampService dynamicTimeService = LocalOrRemoteProxy.newProxyInstance(
                    TimestampService.class, localTime, remoteTime, useLocalServicesFuture);
            return ImmutableLockAndTimestampServices.builder()
                    .lock(dynamicLockService)
                    .time(dynamicTimeService)
                    .build();

        } else {
            return ImmutableLockAndTimestampServices.builder()
                    .lock(remoteLock)
                    .time(remoteTime)
                    .build();
        }
    }

    private static LockAndTimestampServices createRawRemoteServices(AtlasDbConfig config, String userAgent) {
        RemoteLockService lockService = new ServiceCreator<>(RemoteLockService.class, userAgent)
                .apply(config.lock().get());
        TimestampService timeService = new ServiceCreator<>(TimestampService.class, userAgent)
                .apply(config.timestamp().get());

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .time(timeService)
                .build();
    }

    private static LockAndTimestampServices createRawEmbeddedServices(
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time,
            String userAgent) {
        RemoteLockService lockService = ServiceCreator.createInstrumentedService(lock.get(),
                RemoteLockService.class,
                userAgent);
        TimestampService timeService = ServiceCreator.createInstrumentedService(time.get(),
                TimestampService.class,
                userAgent);

//        env.register(lockService);
//        env.register(timeService);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .time(timeService)
                .build();
    }

    @Value.Immutable
    public interface LockAndTimestampServices {
        RemoteLockService lock();
        TimestampService time();
    }

    public interface Environment {
        void register(Object resource);
    }


    public static class InitialisingTransactionManager implements SnapshotTransactionManagerInterface {

        private volatile SerializableTransactionManager delegate = null;
        ServiceDiscoveringAtlasSupplier atlasFactory;
        AtlasDbConfig config;
        java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier;
        Set<Schema> schemas;
        Environment env;
        LockServerOptions lockServerOptions;
        boolean allowHiddenTableAccess;
        String userAgent;

        LockAndTimestampServices lockAndTimestampServices;
        LocalPaxosServices localPaxosServices;
        InitialisingRemoteLockService remoteLockService;
        InitialisingTimestampService timestampService;
        InitialisingPersistentLockService persistentLockService;
        InitialisingSweeperService sweeperService;
        KeyValueService kvs;

        public InitialisingTransactionManager(
                AtlasDbConfig config,
                java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
                Set<Schema> schemas,
                Environment env,
                LockServerOptions lockServerOptions,
                boolean allowHiddenTableAccess,
                String userAgent) {

            atlasFactory = new ServiceDiscoveringAtlasSupplier(config);
            this.config = config;
            this.runtimeConfigSupplier = runtimeConfigSupplier;
            this.schemas = schemas;
            this.env = env;
            this.lockServerOptions = lockServerOptions;
            this.allowHiddenTableAccess = allowHiddenTableAccess;
            this.userAgent = userAgent;

            log.warn("REGISTERING ENDPOINTS");
            register();
            log.warn("ENDPOINTS REGISTERED");

            try{
                initialise();
            } catch (Throwable th) {
                log.warn("COULD NOT INITIALISE, WILL TRY TO INITIALISE ASYNCHRONOUSLY");
                ExecutorService exec = Executors.newFixedThreadPool(
                        1, new ThreadFactoryBuilder().setNameFormat("TransactionManager-Initialiser-%d").build());
                exec.submit(() -> initialiseAsync());
            }
        }

        private void initialiseAsync() {
            while (!initialised()) {
                try {
                    initialise();
                } catch (Throwable th) {
                    log.warn("FAILED TO INITIALISE, RETRYING IN 30 SECONDS...");
                    log.error(th.getMessage());
                    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
                }
            }
        }

        private void register() {
            registerLockAndTimestampServices();
            log.warn("ENDPOINT LATS REGISTERED");
            if (config.leader().isPresent()) {
                registerPaxos(config.leader().get());
            }
            registerPersistentLockService();
            log.warn("ENDPOINT PLS REGISTERED");
            registerSweeperService();
            log.warn("ENDPOINT SS REGISTERED");
        }

        private LockAndTimestampServices initialiseLockAndTimestampServices() {
            log.warn("INITIALISING LOCK AND TIMESTAMP");

            Supplier<RemoteLockService> lock = () -> LockServiceImpl.create(lockServerOptions);
            Supplier<TimestampService> time = atlasFactory::getTimestampService;
            TimestampStoreInvalidator invalidator = atlasFactory.getTimestampStoreInvalidator();

            if (config.leader().isPresent()) {
                initialiseRawLeaderServices(config.leader().get(), lock, time);
            } else if (config.timestamp().isPresent() && config.lock().isPresent()) {
                createRawRemoteServices();
            } else if (config.timelock().isPresent()) {
                TimeLockClientConfig timeLockClientConfig = config.timelock().get();
                TimeLockMigrator.create(timeLockClientConfig, invalidator, userAgent).migrate();
                createNamespacedRawRemoteServices(timeLockClientConfig);
            } else {
                createRawEmbeddedServices(lock, time);
            }
            return ImmutableLockAndTimestampServices.builder()
                    .lock(remoteLockService)
                    .time(timestampService)
                    .build();
        }


        private void initialiseRawLeaderServices(
                LeaderConfig leaderConfig,
                Supplier<RemoteLockService> lock,
                Supplier<TimestampService> time) {
            // Create local services, that may or may not end up being registered in an environment.

            LeaderElectionService leader = localPaxosServices.leaderElectionService();
            RemoteLockService localLock = AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, lock, leader);
            TimestampService localTime = AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, time, leader);

            // Create remote services, that may end up calling our own local services.
            Optional<SSLSocketFactory> sslSocketFactory = ServiceCreator.createSslSocketFactory(
                    leaderConfig.sslConfiguration());
            RemoteLockService remoteLock = ServiceCreator.createService(
                    sslSocketFactory,
                    leaderConfig.leaders(),
                    RemoteLockService.class,
                    userAgent);
            TimestampService remoteTime = ServiceCreator.createService(
                    sslSocketFactory,
                    leaderConfig.leaders(),
                    TimestampService.class,
                    userAgent);

            if (leaderConfig.leaders().size() == 1) {
                // Attempting to connect to ourself while processing a request can lead to deadlock if incoming request
                // volume is high, as all Jetty threads end up waiting for the timestamp server, and no threads remain to
                // actually handle the timestamp server requests. If we are the only single leader, we can avoid the
                // deadlock entirely; so use PingableLeader's getUUID() to detect this situation and eliminate the redundant
                // call.

                PingableLeader localPingableLeader = localPaxosServices.pingableLeader();
                String localServerId = localPingableLeader.getUUID();
                PingableLeader remotePingableLeader = AtlasDbFeignTargetFactory.createRsProxy(
                        sslSocketFactory,
                        Iterables.getOnlyElement(leaderConfig.leaders()),
                        PingableLeader.class,
                        userAgent);

                // Determine asynchronously whether the remote services are talking to our local services.
                CompletableFuture<Boolean> useLocalServicesFuture = new CompletableFuture<>();
                runAsync.accept(() -> {
                    int logAfter = LOGGING_INTERVAL;
                    while (true) {
                        try {
                            String remoteServerId = remotePingableLeader.getUUID();
                            useLocalServicesFuture.complete(localServerId.equals(remoteServerId));
                            return;
                        } catch (ClientErrorException e) {
                            useLocalServicesFuture.complete(false);
                            return;
                        } catch (Throwable e) {
                            if (--logAfter == 0) {
                                log.warn("Failed to read remote timestamp server ID", e);
                                logAfter = LOGGING_INTERVAL;
                            }
                        }
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                    }
                });

                // Create dynamic service proxies, that switch to talking directly to our local services if it turns out our
                // remote services are pointed at them anyway.
                RemoteLockService dynamicLockService = LocalOrRemoteProxy.newProxyInstance(
                        RemoteLockService.class, localLock, remoteLock, useLocalServicesFuture);
                TimestampService dynamicTimeService = LocalOrRemoteProxy.newProxyInstance(
                        TimestampService.class, localTime, remoteTime, useLocalServicesFuture);

                remoteLockService.initialise(dynamicLockService);
                timestampService.initialise(dynamicTimeService);

            } else {
                remoteLockService.initialise(remoteLock);
                timestampService.initialise(remoteTime);
            }
        }

        private void createRawRemoteServices() {
            RemoteLockService lockService = new ServiceCreator<>(RemoteLockService.class, userAgent)
                    .apply(config.lock().get());
            TimestampService timeService = new ServiceCreator<>(TimestampService.class, userAgent)
                    .apply(config.timestamp().get());

            remoteLockService.initialise(lockService);
            timestampService.initialise(timeService);
        }

        private void createNamespacedRawRemoteServices(TimeLockClientConfig config) {
            ServerListConfig namespacedServerListConfig = config.toNamespacedServerList();
            RemoteLockService lockService = new ServiceCreator<>(RemoteLockService.class, userAgent)
                    .apply(namespacedServerListConfig);
            TimestampService timeService = new ServiceCreator<>(TimestampService.class, userAgent)
                    .apply(namespacedServerListConfig);

            remoteLockService.initialise(lockService);
            timestampService.initialise(timeService);
        }

        private void createRawEmbeddedServices(
                Supplier<RemoteLockService> lock,
                Supplier<TimestampService> time) {
            RemoteLockService lockService = ServiceCreator.createInstrumentedService(lock.get(),
                    RemoteLockService.class,
                    userAgent);
            TimestampService timeService = ServiceCreator.createInstrumentedService(time.get(),
                    TimestampService.class,
                    userAgent);

            remoteLockService.initialise(lockService);
            timestampService.initialise(timeService);
        }


        private void registerLockAndTimestampServices() {
            remoteLockService = InitialisingRemoteLockService.createUninitialised();
            env.register(remoteLockService);
            timestampService = InitialisingTimestampService.createUninitialised();
            env.register(timestampService);
        }

        private void registerPaxos(LeaderConfig leaderConfig) {
            localPaxosServices = Leaders.createAndRegisterLocalServices(env, leaderConfig, userAgent);
        }

        private void registerPersistentLockService() {
            persistentLockService = InitialisingPersistentLockService.createUninitialised();
            env.register(persistentLockService);
            env.register(new CheckAndSetExceptionMapper());
        }

        private void registerSweeperService() {
            sweeperService = InitialisingSweeperService.createUninitialised();
            env.register(sweeperService);
        }

        private void initialisePersistentLockService() {
            persistentLockService.initialise(createPersistentLockService(kvs, env));
        }

        private void initialise() {
            log.warn("TRYING TO CREATE RAWKVS");
            KeyValueService rawKvs = atlasFactory.getKeyValueService();
            log.warn("CREATED RAWKVS");

            LockRequest.setDefaultLockTimeout(
                    SimpleTimeDuration.of(config.getDefaultLockTimeoutSeconds(), TimeUnit.SECONDS));
            lockAndTimestampServices = initialiseLockAndTimestampServices();

            kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs);
            kvs = ProfilingKeyValueService.create(kvs, config.getKvsSlowLogThresholdMillis());
            kvs = SweepStatsKeyValueService.create(kvs, lockAndTimestampServices.time());
            kvs = TracingKeyValueService.create(kvs);
            kvs = AtlasDbMetrics.instrument(KeyValueService.class, kvs,
                    MetricRegistry.name(KeyValueService.class, userAgent));
            kvs = ValidatingQueryRewritingKeyValueService.create(kvs);
            log.warn("CREATED KVS");

            TransactionTables.createTables(kvs);
            log.warn("CREATED TRANSACTION TABLES");

            initialisePersistentLockService();
            log.warn("INITIALISED PERSISTENT LOCK SERVICE");

            TransactionService transactionService = TransactionServices.createTransactionService(kvs);
            ConflictDetectionManager conflictManager = ConflictDetectionManagers.create(kvs);
            SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(kvs);

            Set<Schema> allSchemas = ImmutableSet.<Schema>builder()
                    .add(SweepSchema.INSTANCE.getLatestSchema())
                    .addAll(schemas)
                    .build();
            for (Schema schema : allSchemas) {
                Schemas.createTablesAndIndexes(schema, kvs);
            }

            CleanupFollower follower = CleanupFollower.create(schemas);

            Cleaner cleaner = new DefaultCleanerBuilder(
                    kvs,
                    lockAndTimestampServices.lock(),
                    lockAndTimestampServices.time(),
                    LOCK_CLIENT,
                    ImmutableList.of(follower),
                    transactionService)
                    .setBackgroundScrubAggressively(config.backgroundScrubAggressively())
                    .setBackgroundScrubBatchSize(config.getBackgroundScrubBatchSize())
                    .setBackgroundScrubFrequencyMillis(config.getBackgroundScrubFrequencyMillis())
                    .setBackgroundScrubThreads(config.getBackgroundScrubThreads())
                    .setPunchIntervalMillis(config.getPunchIntervalMillis())
                    .setTransactionReadTimeout(config.getTransactionReadTimeoutMillis())
                    .buildCleaner();

            SerializableTransactionManager transactionManager = new SerializableTransactionManager(kvs,
                    lockAndTimestampServices.time(),
                    LOCK_CLIENT,
                    lockAndTimestampServices.lock(),
                    transactionService,
                    Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                    conflictManager,
                    sweepStrategyManager,
                    cleaner,
                    allowHiddenTableAccess);

            PersistentLockManager persistentLockManager = new PersistentLockManager(
                    persistentLockService,
                    config.getSweepPersistentLockWaitMillis());

            initializeSweepEndpointAndBackgroundProcess(
                    transactionService,
                    sweepStrategyManager,
                    follower,
                    transactionManager,
                    persistentLockManager);

            delegate = transactionManager;
        }

        private void initializeSweepEndpointAndBackgroundProcess(
                TransactionService transactionService,
                SweepStrategyManager sweepStrategyManager,
                CleanupFollower follower,
                SerializableTransactionManager transactionManager,
                PersistentLockManager persistentLockManager) {
            CellsSweeper cellsSweeper = new CellsSweeper(
                    transactionManager,
                    kvs,
                    persistentLockManager,
                    ImmutableList.of(follower));
            SweepTaskRunner sweepRunner = new SweepTaskRunner(
                    kvs,
                    transactionManager::getUnreadableTimestamp,
                    transactionManager::getImmutableTimestamp,
                    transactionService,
                    sweepStrategyManager,
                    cellsSweeper);
            BackgroundSweeperPerformanceLogger sweepPerfLogger = new NoOpBackgroundSweeperPerformanceLogger();
            Supplier<SweepBatchConfig> sweepBatchConfig =
                    Suppliers.ofInstance(getSweepBatchConfig(getAtlasDbRuntimeConfig().sweep()));
            SweepMetrics sweepMetrics = new SweepMetrics();

            SpecificTableSweeper specificTableSweeper = SpecificTableSweeper.create(
                    transactionManager,
                    kvs,
                    sweepRunner,
                    sweepBatchConfig,
                    SweepTableFactory.of(),
                    sweepPerfLogger,
                    sweepMetrics);

            sweeperService.initialise(new SweeperServiceImpl(specificTableSweeper));

            BackgroundSweeperImpl backgroundSweeper = BackgroundSweeperImpl.create(
                    () -> getAtlasDbRuntimeConfig().sweep().enabled(),
                    () -> getAtlasDbRuntimeConfig().sweep().pauseMillis(),
                    persistentLockManager,
                    specificTableSweeper);

            transactionManager.registerClosingCallback(backgroundSweeper::shutdown);
            backgroundSweeper.runInBackground();
        }

        private AtlasDbRuntimeConfig getAtlasDbRuntimeConfig() {
            return runtimeConfigSupplier.get().orElse(AtlasDbRuntimeConfig.defaultRuntimeConfig());
        }

        private static SweepBatchConfig getSweepBatchConfig(SweepConfig sweepConfig) {
            return ImmutableSweepBatchConfig.builder()
                    .maxCellTsPairsToExamine(sweepConfig.readLimit())
                    .candidateBatchSize(sweepConfig.candidateBatchHint())
                    .deleteBatchSize(sweepConfig.deleteBatchHint())
                    .build();
        }

        @Override
        public RawTransaction setupRunTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens) {
            checkInitialised();
            return delegate.setupRunTaskWithLocksThrowOnConflict(lockTokens);
        }

        @Override
        public <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
                TransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
            checkInitialised();
            return delegate.finishRunTaskWithLockThrowOnConflict(tx, task);
        }

        @Override
        public void registerClosingCallback(Runnable closingCallback) {
            checkInitialised();
            delegate.registerClosingCallback(closingCallback);
        }

        @Override
        public Cleaner getCleaner() {
            checkInitialised();
            return delegate.getCleaner();
        }

        @Override
        public KeyValueService getKeyValueService() {
            checkInitialised();
            return delegate.getKeyValueService();
        }

        @Override
        public TimestampService getTimestampService() {
            checkInitialised();
            return delegate.getTimestampService();
        }

        @Override
        public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
            checkInitialised();
            return delegate.runTaskWithRetry(task);
        }

        @Override
        public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task)
                throws E, TransactionFailedRetriableException {
            checkInitialised();
            return delegate.runTaskThrowOnConflict(task);
        }

        @Override
        public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
            checkInitialised();
            return delegate.runTaskReadOnly(task);
        }

        @Override
        public long getImmutableTimestamp() {
            checkInitialised();
            return delegate.getImmutableTimestamp();
        }

        @Override
        public KeyValueServiceStatus getKeyValueServiceStatus() {
            checkInitialised();
            return delegate.getKeyValueServiceStatus();
        }

        @Override
        public long getUnreadableTimestamp() {
            checkInitialised();
            return delegate.getUnreadableTimestamp();
        }

        @Override
        public void clearTimestampCache() {
            checkInitialised();
            delegate.clearTimestampCache();
        }

        @Override
        public void close() {
            checkInitialised();
            delegate.close();
        }

        @Override
        public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
                LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException {
            checkInitialised();
            return delegate.runTaskWithLocksWithRetry(lockSupplier, task);
        }

        @Override
        public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
                Supplier<LockRequest> lockSupplier, LockAwareTransactionTask<T, E> task)
                throws E, InterruptedException, LockAcquisitionException {
            checkInitialised();
            return delegate.runTaskWithLocksWithRetry(lockTokens, lockSupplier, task);
        }

        @Override
        public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
                LockAwareTransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
            checkInitialised();
            return delegate.runTaskWithLocksThrowOnConflict(lockTokens, task);
        }

        @Override
        public RemoteLockService getLockService() {
            checkInitialised();
            return delegate.getLockService();
        }

        private void checkInitialised() {
//            if (!initialised()) {
//                log.warn("NOT INITIALISED, TRYING TO INITIALISE...");
//                initialise();
//            }
            if (!initialised()) throw new IllegalStateException("Transaction Manager is not yet initialised!");
        }

        private boolean initialised() {
            return delegate != null;
        }
    }
}
