/**
 * Copyright 2015 Palantir Technologies
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

import java.util.ServiceLoader;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.AtlasDbServerEnvironment;
import com.palantir.atlasdb.spi.AtlasDbServicePlugin;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.paxos.PaxosManyLogImpl;
import com.palantir.timestamp.TimestampService;

public class TransactionManagers {

    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);

    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    /**
     * Create a {@link SerializableTransactionManager} with provided configuration, {@link SSLSocketFactory}, {@link Schema},
     * and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(AtlasDbConfig config,
                                                        Optional<SSLSocketFactory> sslSocketFactory,
                                                        Schema schema,
                                                        AtlasDbServerEnvironment env) {
        return create(config, sslSocketFactory, ImmutableSet.of(schema), env);
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configuration, {@link SSLSocketFactory}, a set of
     * {@link Schema}s, and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(final AtlasDbConfig config,
                                                        Optional<SSLSocketFactory> sslSocketFactory,
                                                        Set<Schema> schemas,
                                                        AtlasDbServerEnvironment env) {
        if (!config.additionalServiceResources().isEmpty()) {
            for (AtlasDbServicePlugin plugin : config.additionalServiceResources()) {
                plugin.registerServices(env);
            }
        }

        final AtlasDbFactory kvsFactory = getKeyValueServiceFactory(config.getType());
        final KeyValueService rawKvs = kvsFactory.createRawKeyValueService(config.keyValueService());

        LockAndTimestampServices lts = createLockAndTimestampServices(config, sslSocketFactory, env,
                new Supplier<RemoteLockService>() {
                    @Override
                    public RemoteLockService get() {
                        return LockServiceImpl.create();
                    }
                },
                new Supplier<TimestampService>() {
                    @Override
                    public TimestampService get() {
                        return kvsFactory.createTimestampService(config.timestampService(), rawKvs);
                    }
                });
        lts = ImmutableLockAndTimestampServices.builder()
                .from(lts)
                .lock(LockRefreshingRemoteLockService.create(lts.lock()))
                .build();

        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs);
        kvs = new SweepStatsKeyValueService(kvs, lts.time());

        SnapshotTransactionManager.createTables(kvs);

        LockClient lockClient = LockClient.of("atlas instance");

        TransactionService transactionService = kvsFactory.createTransactionService(config.transactionService(), kvs);
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(kvs);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(kvs);

        for (Schema schema : ImmutableSet.<Schema>builder().add(SweepSchema.INSTANCE.getLatestSchema()).addAll(schemas).build()) {
            Schemas.createTablesAndIndexes(schema, kvs);
        }

        CleanupFollower follower = CleanupFollower.create(schemas);

        Cleaner cleaner = new DefaultCleanerBuilder(
                kvs,
                lts.lock(),
                lts.time(),
                lockClient,
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
                lts.time(),
                lockClient,
                lts.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);

        SweepTaskRunner sweepRunner = new SweepTaskRunnerImpl(
                transactionManager,
                kvs,
                getUnreadableTsSupplier(transactionManager),
                getImmutableTsSupplier(transactionManager),
                transactionService,
                sweepStrategyManager,
                ImmutableList.<Follower>of(follower));
        BackgroundSweeper backgroundSweeper = new BackgroundSweeperImpl(
                transactionManager,
                kvs,
                sweepRunner,
                Suppliers.ofInstance(config.enableSweep()),
                Suppliers.ofInstance(config.getSweepPauseMillis()),
                Suppliers.ofInstance(config.getSweepBatchSize()),
                SweepTableFactory.of());
        backgroundSweeper.runInBackground();

        return transactionManager;
    }

    private static Supplier<Long> getImmutableTsSupplier(final TransactionManager txManager) {
        return new Supplier<Long>() {
            @Override
            public Long get() {
                return txManager.getImmutableTimestamp();
            }
        };
    }

    private static Supplier<Long> getUnreadableTsSupplier(final TransactionManager txManager) {
        return new Supplier<Long>() {
            @Override
            public Long get() {
                return txManager.getUnreadableTimestamp();
            }
        };
    }

    private static AtlasDbFactory getKeyValueServiceFactory(String type) {
        for (AtlasDbFactory factory : loader) {
            if (factory.getType().equalsIgnoreCase(type)) {
                return factory;
            }
        }
        throw new IllegalStateException("No atlas provider for KeyValueService type " + type + " is on your classpath.");
    }

    private static LockAndTimestampServices createLockAndTimestampServices(
            AtlasDbConfig config,
            Optional<SSLSocketFactory> sslSocketFactory,
            AtlasDbServerEnvironment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        if (config.leader().isPresent()) {
            LeaderConfig leaderConfig = config.leader().get();
            if (!leaderConfig.additionalPaxosEndpointsToLogDir().isEmpty()) {
                env.register(PaxosManyLogImpl.create(leaderConfig.additionalPaxosEndpointsToLogDir()));
            }
            LeaderElectionService leader = Leaders.create(sslSocketFactory, env, leaderConfig);
            env.register(AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, lock, leader));
            env.register(AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, time, leader));

            warnIf(config.lock().isPresent(), "Ignoring lock server configuration because leadership election is enabled");
            warnIf(config.timestamp().isPresent(), "Ignoring timestamp server configuration because leadership election is enabled");

            return ImmutableLockAndTimestampServices.builder()
                    .lock(createRemoteServiceWithFailover(sslSocketFactory, leaderConfig.leaders(), RemoteLockService.class))
                    .time(createRemoteServiceWithFailover(sslSocketFactory, leaderConfig.leaders(), TimestampService.class))
                    .build();
        } else {
            warnIf(config.lock().isPresent() != config.timestamp().isPresent(), "Using embedded instances for one (but not both) of lock and timestamp services");

            return ImmutableLockAndTimestampServices.builder()
                    .lock(config.lock().transform(new ServiceCreator<>(sslSocketFactory, RemoteLockService.class)).or(lock))
                    .time(config.timestamp().transform(new ServiceCreator<>(sslSocketFactory, TimestampService.class)).or(time))
                    .build();
        }
    }

    private static void warnIf(boolean arg, String warning) {
        if (arg) {
            log.warn(warning);
        }
    }

    private static <T> T createRemoteServiceWithFailover(Optional<SSLSocketFactory> sslSocketFactory, Set<String> uris, Class<T> serviceClass) {
        return AtlasDbHttpClients.createRemoteProxyWithFailover(sslSocketFactory, uris, serviceClass);
    }

    private static class ServiceCreator<T> implements Function<ServerListConfig, T> {
        private Optional<SSLSocketFactory> sslSocketFactory;
        private Class<T> serviceClass;

        public ServiceCreator(Optional<SSLSocketFactory> sslSocketFactory, Class<T> serviceClass) {
            this.sslSocketFactory = sslSocketFactory;
            this.serviceClass = serviceClass;
        }

        @Override
        public T apply(ServerListConfig input) {
            return createRemoteServiceWithFailover(sslSocketFactory, input.servers(), serviceClass);
        }
    }

    @Value.Immutable
    interface LockAndTimestampServices {
        RemoteLockService lock();
        TimestampService time();
    }
}
