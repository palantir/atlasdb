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
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.spi.AtlasDbFactory;
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
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.remoting.ssl.SslSocketFactories;
import com.palantir.timestamp.TimestampService;

public final class TransactionManagers {
    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);

    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);
    public static final LockClient LOCK_CLIENT = LockClient.of("atlas instance");

    private TransactionManagers() {
        // Utility class
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configuration,
     * {@link SSLSocketFactory}, {@link Schema}, and an environment in which to
     * register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            Schema schema,
            Environment env,
            boolean allowHiddenTableAccess) {
        return create(config, ImmutableSet.of(schema), env, allowHiddenTableAccess);
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configuration, {@link SSLSocketFactory}, a set of
     * {@link Schema}s, and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            Set<Schema> schemas,
            Environment env,
            boolean allowHiddenTableAccess) {
        ServiceDiscoveringAtlasSupplier atlasFactory =
                new ServiceDiscoveringAtlasSupplier(config.keyValueService(), config.leader());
        KeyValueService rawKvs = atlasFactory.getKeyValueService();

        LockAndTimestampServices lts = createLockAndTimestampServices(
                config,
                env,
                LockServiceImpl::create,
                atlasFactory::getTimestampService);

        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs);
        kvs = ProfilingKeyValueService.create(kvs);
        kvs = new SweepStatsKeyValueService(kvs, lts.time());

        TransactionTables.createTables(kvs);

        TransactionService transactionService = TransactionServices.createTransactionService(kvs);
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(kvs);
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
                lts.lock(),
                lts.time(),
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
                lts.time(),
                LOCK_CLIENT,
                lts.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess);

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
        return () -> txManager.getImmutableTimestamp();
    }

    private static Supplier<Long> getUnreadableTsSupplier(final TransactionManager txManager) {
        return () -> txManager.getUnreadableTimestamp();
    }

    public static LockAndTimestampServices createLockAndTimestampServices(
            AtlasDbConfig config,
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        LockAndTimestampServices lockAndTimestampServices =
                createRawServices(config, env, lock, time);
        return withRefreshingLockService(lockAndTimestampServices);
    }

    private static LockAndTimestampServices withRefreshingLockService(
            LockAndTimestampServices lockAndTimestampServices) {
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .lock(LockRefreshingRemoteLockService.create(lockAndTimestampServices.lock()))
                .build();
    }

    private static LockAndTimestampServices createRawServices(
            AtlasDbConfig config,
            Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        if (config.leader().isPresent()) {
            LeaderElectionService leader = Leaders.create(env, config.leader().get());
            env.register(AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, lock, leader));
            env.register(AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, time, leader));

            warnIf(config.lock().isPresent(),
                    "Ignoring lock server configuration because leadership election is enabled");
            warnIf(config.timestamp().isPresent(),
                    "Ignoring timestamp server configuration because leadership election is enabled");

            Optional<SSLSocketFactory> sslSocketFactory =
                    createSslSocketFactory(config.leader().get().sslConfiguration());

            return ImmutableLockAndTimestampServices.builder()
                    .lock(createService(sslSocketFactory, config.leader().get().leaders(), RemoteLockService.class))
                    .time(createService(sslSocketFactory, config.leader().get().leaders(), TimestampService.class))
                    .build();
        } else {
            warnIf(config.lock().isPresent() != config.timestamp().isPresent(),
                    "Using embedded instances for one (but not both) of lock and timestamp services");

            RemoteLockService lockService = config.lock()
                    .transform(new ServiceCreator<>(RemoteLockService.class))
                    .or(lock);
            TimestampService timeService = config.timestamp()
                    .transform(new ServiceCreator<>(TimestampService.class))
                    .or(time);

            if (!config.lock().isPresent()) {
                env.register(lockService);
            }
            if (!config.timestamp().isPresent()) {
                env.register(timeService);
            }

            return ImmutableLockAndTimestampServices.builder()
                    .lock(lockService)
                    .time(timeService)
                    .build();
        }
    }

    private static void warnIf(boolean arg, String warning) {
        if (arg) {
            log.warn(warning);
        }
    }

    /**
     * Utility method for transforming an optional {@link SslConfiguration} into an optional {@link SSLSocketFactory}.
     */
    public static Optional<SSLSocketFactory> createSslSocketFactory(Optional<SslConfiguration> sslConfiguration) {
        return sslConfiguration.transform(config -> SslSocketFactories.createSslSocketFactory(config));
    }

    private static <T> T createService(
            Optional<SSLSocketFactory> sslSocketFactory,
            Set<String> uris,
            Class<T> serviceClass) {
        return AtlasDbHttpClients.createProxyWithFailover(sslSocketFactory, uris, serviceClass);
    }

    private static class ServiceCreator<T> implements Function<ServerListConfig, T> {
        private Class<T> serviceClass;

        ServiceCreator(Class<T> serviceClass) {
            this.serviceClass = serviceClass;
        }

        @Override
        public T apply(ServerListConfig input) {
            Optional<SSLSocketFactory> sslSocketFactory = createSslSocketFactory(input.sslConfiguration());
            return createService(sslSocketFactory, input.servers(), serviceClass);
        }
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
