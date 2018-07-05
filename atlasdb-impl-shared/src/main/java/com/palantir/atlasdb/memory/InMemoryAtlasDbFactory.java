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
package com.palantir.atlasdb.memory;

import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
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
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

/**
 * This is the easiest way to try out AtlasDB with your schema.  It runs entirely in memory but has
 * all the features of a full cluster including {@link OnCleanupTask}s.
 * <p>
 * This method creates all the tables in the pass {@link Schema} and provides Snapshot Isolation
 * (SI) on all of the transactions it creates.
 */
@AutoService(AtlasDbFactory.class)
public class InMemoryAtlasDbFactory implements AtlasDbFactory {
    private static final Logger log = LoggerFactory.getLogger(InMemoryAtlasDbFactory.class);

    /**
     * @deprecated see usage below. Should be configured with the {@link InMemoryAtlasDbConfig}.
     * To be removed whenever someone removes the deprecated constructors that don't know about atlas configs...
     */
    @Deprecated
    private static final long DEFAULT_TIMESTAMP_CACHE_SIZE = AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE;

    /**
     * @deprecated see usage below. Should be configured with the {@link InMemoryAtlasDbConfig}.
     */
    @Deprecated
    private static final int DEFAULT_MAX_CONCURRENT_RANGES = 64;

    /**
     * @deprecated see usage below. Should be configured with the {@link InMemoryAtlasDbConfig}.
     */
    @Deprecated
    private static final int DEFAULT_GET_RANGES_CONCURRENCY = 8;

    @Override
    public String getType() {
        return "memory";
    }

    /**
     * Creates an InMemoryKeyValueService.
     *
     * @param config Configuration file.
     * @param runtimeConfig unused.
     * @param leaderConfig unused.
     * @param unused unused.
     * @param unusedLongSupplier unused.
     * @param initializeAsync unused. Async initialization has not been implemented and is not propagated.
     * @param unusedQosClient unused.
     * @return The requested KeyValueService instance
     */
    @Override
    public InMemoryKeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> unused,
            LongSupplier unusedLongSupplier,
            boolean initializeAsync,
            QosClient unusedQosClient) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronously.");
        }

        AtlasDbVersion.ensureVersionReported();
        return new InMemoryKeyValueService(false);
    }

    @Override
    public TimestampService createTimestampService(
            KeyValueService rawKvs,
            Optional<TableReference> unused,
            boolean initializeAsync) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronously.");
        }

        AtlasDbVersion.ensureVersionReported();
        return new InMemoryTimestampService();
    }

    /**
     * @deprecated use {@link TransactionManagers#createInMemory(...)}
     *
     * There are some differences in set up between the methods though they are unlikely to cause breaks.
     * This method has been deprecated as all testing should be conducted on the code path used for
     * production TransactionManager instantiation (regardless of the backing store).  It will be removed in
     * future versions.
     */
    @Deprecated
    public static TransactionManager createInMemoryTransactionManager(AtlasSchema schema,
            AtlasSchema... otherSchemas) {

        Set<Schema> schemas = Lists.asList(schema, otherSchemas).stream()
                .map(AtlasSchema::getLatestSchema)
                .collect(Collectors.toSet());

        return createInMemoryTransactionManagerInternal(schemas);
    }

    private static TransactionManager createInMemoryTransactionManagerInternal(Set<Schema> schemas) {
        TimestampService ts = new InMemoryTimestampService();
        KeyValueService keyValueService = new InMemoryKeyValueService(false);

        schemas.forEach(s -> Schemas.createTablesAndIndexes(s, keyValueService));
        TransactionTables.createTables(keyValueService);

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        LockService lock = LockRefreshingLockService.create(LockServiceImpl.create(
                 LockServerOptions.builder().isStandaloneServer(false).build()));
        LockClient client = LockClient.of("in memory atlasdb instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createWithoutWarmingCache(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schemas);
        Cleaner cleaner = new DefaultCleanerBuilder(
                keyValueService,
                lock,
                ts,
                client,
                ImmutableList.of(follower),
                transactionService).buildCleaner();
        TransactionManager ret = SerializableTransactionManager.createForTest(
                MetricsManagers.createForTests(),
                keyValueService,
                ts,
                client,
                lock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner,
                DEFAULT_MAX_CONCURRENT_RANGES,
                DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP);
        cleaner.start(ret);
        return ret;
    }
}
