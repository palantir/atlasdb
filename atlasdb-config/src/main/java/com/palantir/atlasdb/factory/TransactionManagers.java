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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.ClientErrorException;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.Leaders.LocalPaxosServices;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.http.AtlasDbFeignTargetFactory;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManagerImpl;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.logsafe.UnsafeArg;
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
    public static SerializableTransactionManager createInMemory(Schema schema) {
        return createInMemory(ImmutableSet.of(schema));
    }

    /**
     * Create a {@link SerializableTransactionManagerImpl} backed by an
     * {@link com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService}. This should be used for testing
     * purposes only.
     */
    public static SerializableTransactionManager createInMemory(Set<Schema> schemas) {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder().keyValueService(new InMemoryAtlasDbConfig()).build();
        return create(config,
                java.util.Optional::empty,
                schemas,
                x -> {
                },
                false);
    }

    /**
     * Create a {@link SerializableTransactionManagerImpl} with provided configurations, {@link Schema},
     * and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Schema schema,
            Environment env,
            boolean allowHiddenTableAccess) {
        return create(config, runtimeConfigSupplier, ImmutableSet.of(schema), env, allowHiddenTableAccess);
    }

    /**
     * Create a {@link SerializableTransactionManagerImpl} with provided configurations, a set of
     * {@link Schema}s, and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(
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
     * Create a {@link SerializableTransactionManagerImpl} with provided configurations, a set of
     * {@link Schema}s, {@link LockServerOptions}, and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess) {
        return create(config, runtimeConfigSupplier, schemas, env, lockServerOptions, allowHiddenTableAccess,
                UserAgents.DEFAULT_USER_AGENT);
    }

    public static SerializableTransactionManager create(
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

    private static SerializableTransactionManager create(
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

    /**
     * This method should not be used directly. It remains here to support the AtlasDB-Dagger module and the CLIs, but
     * may be removed at some point in the future.
     *
     * @deprecated Not intended for public use outside of the AtlasDB CLIs
     */
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

    private static LockAndTimestampServices withRefreshingLockService(
            LockAndTimestampServices lockAndTimestampServices) {
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .lock(LockRefreshingRemoteLockService.create(lockAndTimestampServices.lock()))
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
        env.register(localLock);
        env.register(localTime);

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

        env.register(lockService);
        env.register(timeService);

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
}
