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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.backup.AtlasBackupResource;
import com.palantir.atlasdb.backup.AtlasBackupService;
import com.palantir.atlasdb.backup.AtlasRestoreResource;
import com.palantir.atlasdb.backup.AtlasRestoreService;
import com.palantir.atlasdb.backup.AuthHeaderValidator;
import com.palantir.atlasdb.backup.DelegatingBackupTimeLockServiceView;
import com.palantir.atlasdb.backup.ExternalBackupPersister;
import com.palantir.atlasdb.backup.SimpleBackupAndRestoreResource;
import com.palantir.atlasdb.backup.api.AtlasBackupClient;
import com.palantir.atlasdb.backup.api.AtlasRestoreClient;
import com.palantir.atlasdb.blob.BlobSchema;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.SimpleCoordinationResource;
import com.palantir.atlasdb.factory.AtlasDbDialogueServiceProvider;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.NotInitializedExceptionMapper;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.lock.SimpleLockResource;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.TargetedSweepFollower;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.BackupTimeLockServiceView;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.atlasdb.timestamp.SimpleEteTimestampResource;
import com.palantir.atlasdb.todo.SimpleTodoResource;
import com.palantir.atlasdb.todo.TodoClient;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.simple.test.SimpleTestServer;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tokens.auth.BearerToken;
import com.palantir.tritium.metrics.registry.DropwizardTaggedMetricSet;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.jetty.HttpsConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

@SuppressWarnings("ShutdownHook")
public class AtlasDbEteServer {
    public static final long CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS = 300;

    private static final SafeLogger log = SafeLoggerFactory.get(AtlasDbEteServer.class);
    private static final long CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS = 5;
    private static final ImmutableSet<Schema> ETE_SCHEMAS =
            ImmutableSet.of(TodoSchema.getSchema(), BlobSchema.getSchema());
    private static final Follower FOLLOWER = CleanupFollower.create(ETE_SCHEMAS);

    public static void main(String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 2, "Expected arg 'server' and a path to a configuration");
        Preconditions.checkArgument("server".equals(args[0]), "Expected arg 'server'", SafeArg.of("args", args));
        String configString = Files.readString(Path.of(args[1]));
        configString = new EnvironmentVariableSubstitutor().replace(configString);
        AtlasDbEteConfiguration configuration = ObjectMappers.newClientJsonMapper()
                .setSubtypeResolver(new DiscoverableSubtypeResolver())
                .readValue(configString, AtlasDbEteConfiguration.class);
        new AtlasDbEteServer().run(configuration);
    }

    public void run(AtlasDbEteConfiguration config) throws Exception {
        DefaultServerFactory serverFactory = (DefaultServerFactory) config.getServerFactory();
        HttpConnectorFactory connector =
                (HttpConnectorFactory) Iterables.getOnlyElement(serverFactory.getApplicationConnectors());
        if (connector instanceof HttpsConnectorFactory) {
            throw new UnsupportedOperationException("TLS support has not been implemented");
        }
        SimpleTestServer.Builder serverBuilder = SimpleTestServer.builder()
                .contextPath(serverFactory.getApplicationContextPath())
                .port(connector.getPort());

        Consumer<Object> registrar = object -> serverBuilder.jersey(resourceConfig -> resourceConfig.register(object));
        Consumer<UndertowService> undertowRegistrar = serverBuilder::undertow;

        TaggedMetricRegistry taggedMetrics = SharedTaggedMetricRegistries.getSingleton();
        TransactionManager txManager =
                tryToCreateTransactionManager(config, registrar, undertowRegistrar, taggedMetrics);
        Supplier<SweepTaskRunner> sweepTaskRunner = Suppliers.memoize(() -> getSweepTaskRunner(txManager));
        TargetedSweeper sweeper = TargetedSweeper.createUninitializedForTest(() -> 1);
        Supplier<TargetedSweeper> sweeperSupplier = Suppliers.memoize(() -> initializeAndGet(sweeper, txManager));
        ensureTransactionSchemaVersionInstalled(config.getAtlasDbConfig(), config.getAtlasDbRuntimeConfig(), txManager);

        if (shouldSetUpBackupAndRestoreResource(config)) {
            createAndRegisterBackupAndRestoreResource(config, registrar, undertowRegistrar, txManager, taggedMetrics);
        }

        serverBuilder.jersey(resourceConfig -> {
            resourceConfig.register(
                    new SimpleTodoResource(new TodoClient(txManager, sweepTaskRunner, sweeperSupplier)));
            resourceConfig.register(SimpleCoordinationResource.create(txManager));
            resourceConfig.register(new NotInitializedExceptionMapper());
            resourceConfig.register(new SimpleEteTimestampResource(txManager));
            resourceConfig.register(new SimpleLockResource(txManager));
        });
        SimpleTestServer server = serverBuilder.build();
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }

    private boolean shouldSetUpBackupAndRestoreResource(AtlasDbEteConfiguration config) {
        boolean isCassandra = CassandraKeyValueServiceConfig.TYPE.equals(
                config.getAtlasDbConfig().keyValueService().type());
        boolean hasTimelock = config.getAtlasDbRuntimeConfig()
                .flatMap(AtlasDbRuntimeConfig::timelockRuntime)
                .isPresent();
        return isCassandra && hasTimelock;
    }

    private void createAndRegisterBackupAndRestoreResource(
            AtlasDbEteConfiguration config,
            Consumer<Object> registrar,
            Consumer<UndertowService> _undertowRegistrar,
            TransactionManager txManager,
            TaggedMetricRegistry taggedMetrics)
            throws IOException {
        AuthHeader authHeader = AuthHeader.of(BearerToken.valueOf("test-auth"));
        URL localServer = new URL("https://localhost:1234");

        Path backupFolder = Paths.get("/var/data/backup");
        Files.createDirectories(backupFolder);
        Function<Namespace, Path> backupFolderFactory = _unused -> backupFolder;
        ExternalBackupPersister externalBackupPersister = new ExternalBackupPersister(backupFolderFactory);

        Function<String, BackupTimeLockServiceView> timelockServices =
                _unused -> createBackupTimeLockServiceView(txManager);
        AuthHeaderValidator authHeaderValidator =
                new AuthHeaderValidator(() -> Optional.of(authHeader.getBearerToken()));
        RedirectRetryTargeter redirectRetryTargeter =
                RedirectRetryTargeter.create(localServer, ImmutableList.of(localServer));
        AtlasBackupClient atlasBackupClient =
                AtlasBackupResource.jersey(authHeaderValidator, redirectRetryTargeter, timelockServices);
        AtlasRestoreClient atlasRestoreClient =
                AtlasRestoreResource.jersey(authHeaderValidator, redirectRetryTargeter, timelockServices);
        Refreshable<ServerListConfig> serverListConfig = getServerListConfigForTimeLock(config);
        TimeLockManagementService timeLockManagementService =
                getRemoteTimeLockManagementService(serverListConfig, taggedMetrics);

        AtlasBackupService atlasBackupService =
                AtlasBackupService.createForTests(authHeader, atlasBackupClient, txManager, backupFolderFactory);
        AtlasRestoreService atlasRestoreService = AtlasRestoreService.createForTests(
                authHeader,
                atlasRestoreClient,
                timeLockManagementService,
                externalBackupPersister,
                txManager,
                _unused -> (CassandraKeyValueServiceConfig)
                        config.getAtlasDbConfig().keyValueService());

        registrar.accept(
                new SimpleBackupAndRestoreResource(atlasBackupService, atlasRestoreService, externalBackupPersister));
    }

    private Refreshable<ServerListConfig> getServerListConfigForTimeLock(AtlasDbEteConfiguration eteConfig) {
        AtlasDbConfig config = eteConfig.getAtlasDbConfig();
        Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfig = eteConfig.getAtlasDbRuntimeConfig();
        return ServerListConfigs.getTimeLockServersFromAtlasDbConfig(config, atlasDbRuntimeConfig);
    }

    private TimeLockManagementService getRemoteTimeLockManagementService(
            Refreshable<ServerListConfig> serverListConfig, TaggedMetricRegistry taggedMetrics) {
        UserAgent userAgent = UserAgent.of(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
        DialogueClients.ReloadingFactory reloadingFactory = DialogueClients.create(
                        Refreshable.only(ServicesConfigBlock.builder().build()))
                .withUserAgent(userAgent);
        AtlasDbDialogueServiceProvider dialogueServiceProvider =
                AtlasDbDialogueServiceProvider.create(serverListConfig, reloadingFactory, userAgent, taggedMetrics);
        return dialogueServiceProvider.getTimeLockManagementService();
    }

    private void ensureTransactionSchemaVersionInstalled(
            AtlasDbConfig config, Optional<AtlasDbRuntimeConfig> runtimeConfig, TransactionManager transactionManager) {
        if (runtimeConfig.isEmpty()
                || runtimeConfig
                        .get()
                        .internalSchema()
                        .targetTransactionsSchemaVersion()
                        .isEmpty()) {
            return;
        }
        CoordinationService<InternalSchemaMetadata> coordinationService = CoordinationServices.createDefault(
                transactionManager.getKeyValueService(),
                transactionManager.getTimestampService(),
                MetricsManagers.createForTests(),
                config.initializeAsync());
        TransactionSchemaManager manager = new TransactionSchemaManager(coordinationService);

        int targetSchemaVersion = runtimeConfig
                .get()
                .internalSchema()
                .targetTransactionsSchemaVersion()
                .get();
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                manager,
                transactionManager.getTimestampService(),
                transactionManager.getTimestampManagementService(),
                targetSchemaVersion);
    }

    private TransactionManager tryToCreateTransactionManager(
            AtlasDbEteConfiguration config,
            Consumer<Object> registrar,
            Consumer<UndertowService> undertowRegistrar,
            TaggedMetricRegistry taggedMetricRegistry)
            throws InterruptedException {
        if (config.getAtlasDbConfig().initializeAsync()) {
            return createTransactionManager(
                    config.getAtlasDbConfig(),
                    config.getAtlasDbRuntimeConfig(),
                    registrar,
                    undertowRegistrar,
                    taggedMetricRegistry);
        } else {
            return createTransactionManagerWithRetry(
                    config.getAtlasDbConfig(),
                    config.getAtlasDbRuntimeConfig(),
                    registrar,
                    undertowRegistrar,
                    taggedMetricRegistry);
        }
    }

    private BackupTimeLockServiceView createBackupTimeLockServiceView(TransactionManager txManager) {
        TimelockService timelockService = txManager.getTimelockService();
        TimestampManagementService timestampManagementService = txManager.getTimestampManagementService();
        return new DelegatingBackupTimeLockServiceView(timelockService, timestampManagementService);
    }

    private SweepTaskRunner getSweepTaskRunner(TransactionManager transactionManager) {
        KeyValueService kvs = transactionManager.getKeyValueService();
        LongSupplier ts = transactionManager.getTimestampService()::getFreshTimestamp;
        TransactionService txnService =
                TransactionServices.createRaw(kvs, transactionManager.getTimestampService(), false);
        CleanupFollower follower = CleanupFollower.create(ETE_SCHEMAS);
        CellsSweeper cellsSweeper = new CellsSweeper(transactionManager, kvs, ImmutableList.of(follower));
        return new SweepTaskRunner(kvs, ts, ts, txnService, cellsSweeper);
    }

    private TargetedSweeper initializeAndGet(TargetedSweeper sweeper, TransactionManager txManager) {
        // Intentionally providing the immutable timestamp instead of unreadable to avoid the delay
        sweeper.initializeWithoutRunning(
                new SpecialTimestampsSupplier(txManager::getImmutableTimestamp, txManager::getImmutableTimestamp),
                txManager.getTimelockService(),
                txManager.getKeyValueService(),
                TransactionServices.createRaw(txManager.getKeyValueService(), txManager.getTimestampService(), false),
                new TargetedSweepFollower(ImmutableList.of(FOLLOWER), txManager));
        sweeper.runInBackground();
        return sweeper;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private TransactionManager createTransactionManagerWithRetry(
            AtlasDbConfig config,
            Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfig,
            Consumer<Object> registrar,
            Consumer<UndertowService> undertowRegistrar,
            TaggedMetricRegistry taggedMetricRegistry)
            throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(TimeUnit.SECONDS) < CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS) {
            try {
                return createTransactionManager(
                        config, atlasDbRuntimeConfig, registrar, undertowRegistrar, taggedMetricRegistry);
            } catch (RuntimeException e) {
                log.warn("An error occurred while trying to create transaction manager. Retrying...", e);
                Thread.sleep(CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS);
            }
        }
        throw new SafeIllegalStateException("Timed-out because we were unable to create transaction manager");
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private TransactionManager createTransactionManager(
            AtlasDbConfig config,
            Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfigOptional,
            Consumer<Object> registrar,
            Consumer<UndertowService> _undertowRegistrar,
            TaggedMetricRegistry taggedMetricRegistry) {
        MetricRegistry registry = new MetricRegistry();
        taggedMetricRegistry.addMetrics("registry", "codahale", new DropwizardTaggedMetricSet(registry));
        return TransactionManagers.builder()
                .config(config)
                .userAgent(UserAgent.of(UserAgent.Agent.of("atlasdb-ete-test", "0.0.0")))
                .globalMetricsRegistry(registry)
                .globalTaggedMetricRegistry(taggedMetricRegistry)
                .registrar(registrar)
                .addAllSchemas(ETE_SCHEMAS)
                .runtimeConfigSupplier(() -> atlasDbRuntimeConfigOptional)
                .build()
                .serializable();
    }
}
