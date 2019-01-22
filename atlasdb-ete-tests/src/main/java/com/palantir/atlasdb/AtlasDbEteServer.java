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
package com.palantir.atlasdb;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.blob.BlobSchema;
import com.palantir.atlasdb.cas.CheckAndSetClient;
import com.palantir.atlasdb.cas.CheckAndSetSchema;
import com.palantir.atlasdb.cas.SimpleCheckAndSetResource;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.coordination.SimpleCoordinationResource;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.NotInitializedExceptionMapper;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.PersistentLockManager;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.TargetedSweepFollower;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timestamp.SimpleEteTimestampResource;
import com.palantir.atlasdb.todo.SimpleTodoResource;
import com.palantir.atlasdb.todo.TodoClient;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.remoting3.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbEteServer extends Application<AtlasDbEteConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbEteServer.class);
    private static final long CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS = 60;
    private static final long CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS = 5;
    private static final Set<Schema> ETE_SCHEMAS = ImmutableSet.of(
            CheckAndSetSchema.getSchema(),
            TodoSchema.getSchema(),
            BlobSchema.getSchema());
    private static final Follower FOLLOWER = CleanupFollower.create(ETE_SCHEMAS);


    public static void main(String[] args) throws Exception {
        new AtlasDbEteServer().run(args);
    }

    @Override
    public void initialize(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setMetricRegistry(SharedMetricRegistries.getOrCreate("AtlasDbTest"));
        enableEnvironmentVariablesInConfig(bootstrap);
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
    }

    @Override
    public void run(AtlasDbEteConfiguration config, final Environment environment) throws Exception {
        TaggedMetricRegistry taggedMetrics = new DefaultTaggedMetricRegistry();
        TransactionManager txManager = tryToCreateTransactionManager(config, environment, taggedMetrics);
        Supplier<SweepTaskRunner> sweepTaskRunner = Suppliers.memoize(() ->
                getSweepTaskRunner(txManager, environment.metrics(), taggedMetrics));
        TargetedSweeper sweeper = TargetedSweeper.createUninitializedForTest(() -> 1);
        Supplier<TargetedSweeper> sweeperSupplier = Suppliers.memoize(() -> initializeAndGet(sweeper, txManager));
        environment.jersey().register(new SimpleTodoResource(new TodoClient(
                txManager,
                sweepTaskRunner,
                sweeperSupplier)));
        environment.jersey().register(SimpleCoordinationResource.create(txManager));
        environment.jersey().register(new SimpleCheckAndSetResource(new CheckAndSetClient(txManager)));
        environment.jersey().register(HttpRemotingJerseyFeature.INSTANCE);
        environment.jersey().register(new NotInitializedExceptionMapper());
        environment.jersey().register(new SimpleEteTimestampResource(txManager));
    }

    private TransactionManager tryToCreateTransactionManager(AtlasDbEteConfiguration config,
            Environment environment, TaggedMetricRegistry taggedMetricRegistry) throws InterruptedException {
        if (config.getAtlasDbConfig().initializeAsync()) {
            return createTransactionManager(
                    config.getAtlasDbConfig(), config.getAtlasDbRuntimeConfig(), environment, taggedMetricRegistry);
        } else {
            return createTransactionManagerWithRetry(config.getAtlasDbConfig(),
                    config.getAtlasDbRuntimeConfig(),
                    environment,
                    taggedMetricRegistry);
        }
    }

    private SweepTaskRunner getSweepTaskRunner(
            TransactionManager transactionManager, MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry) {
        KeyValueService kvs = transactionManager.getKeyValueService();
        LongSupplier ts = transactionManager.getTimestampService()::getFreshTimestamp;
        TransactionService txnService
                = TransactionServices.createForTesting(kvs, transactionManager.getTimestampService(), false);
        SweepStrategyManager ssm = SweepStrategyManagers.completelyConservative(kvs); // maybe createDefault
        PersistentLockManager noLocks = new PersistentLockManager(
                MetricsManagers.of(metricRegistry, taggedMetricRegistry),
                new NoOpPersistentLockService(),
                AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS);
        CleanupFollower follower = CleanupFollower.create(ETE_SCHEMAS);
        CellsSweeper cellsSweeper = new CellsSweeper(transactionManager, kvs, noLocks, ImmutableList.of(follower));
        return new SweepTaskRunner(kvs, ts, ts, txnService, ssm, cellsSweeper);
    }

    private TargetedSweeper initializeAndGet(TargetedSweeper sweeper, TransactionManager txManager) {
        sweeper.initializeWithoutRunning(
                new SpecialTimestampsSupplier(txManager::getImmutableTimestamp, txManager::getImmutableTimestamp),
                txManager.getTimelockService(),
                txManager.getKeyValueService(),
                TransactionServices.createForTesting(
                        txManager.getKeyValueService(), txManager.getTimestampService(), false),
                new TargetedSweepFollower(ImmutableList.of(FOLLOWER), txManager));
        sweeper.runInBackground();
        return sweeper;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private TransactionManager createTransactionManagerWithRetry(AtlasDbConfig config,
            Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfig,
            Environment environment,
            TaggedMetricRegistry taggedMetricRegistry)
            throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(TimeUnit.SECONDS) < CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS) {
            try {
                return createTransactionManager(config, atlasDbRuntimeConfig, environment, taggedMetricRegistry);
            } catch (RuntimeException e) {
                log.warn("An error occurred while trying to create transaction manager. Retrying...", e);
                Thread.sleep(CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS);
            }
        }
        throw new IllegalStateException("Timed-out because we were unable to create transaction manager");
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private TransactionManager createTransactionManager(AtlasDbConfig config,
            Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfigOptional, Environment environment,
            TaggedMetricRegistry taggedMetricRegistry) {
        return TransactionManagers.builder()
                .config(config)
                .userAgent("ete test")
                .globalMetricsRegistry(environment.metrics())
                .globalTaggedMetricRegistry(taggedMetricRegistry)
                .registrar(environment.jersey()::register)
                .addAllSchemas(ETE_SCHEMAS)
                .runtimeConfigSupplier(() -> atlasDbRuntimeConfigOptional)
                .build()
                .serializable();
    }

    private void enableEnvironmentVariablesInConfig(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(
                        bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor()));
    }
}
