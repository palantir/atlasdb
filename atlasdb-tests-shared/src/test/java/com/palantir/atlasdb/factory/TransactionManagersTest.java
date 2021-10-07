/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableTimestampClientConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockRuntimeConfig;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.memory.InMemoryAsyncAtlasDbConfig;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.exception.NotInitializedException;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.AutoDelegate_LockService;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.NamespaceAgnosticLockRpcClient;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.TimeDuration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.LockResponse;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Files;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TransactionManagersTest {
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT_VERSION = "3.1415926.5358979";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(USER_AGENT_NAME, USER_AGENT_VERSION));
    private static final String EXPECTED_USER_AGENT_STRING = UserAgents.format(USER_AGENT);
    private static final String USER_AGENT_HEADER = "User-Agent";

    private static final long EMBEDDED_BOUND = 3;

    private static final String CURRENT_TIME_MILLIS = "currentTimeMillis";
    private static final String LOCK_SERVICE_CURRENT_TIME_METRIC =
            MetricRegistry.name(LockService.class, CURRENT_TIME_MILLIS);
    private static final String NAMESPACE_AGNOSTIC_LOCK_RPC_CLIENT_CURRENT_TIME_METRIC =
            MetricRegistry.name(NamespaceAgnosticLockRpcClient.class, CURRENT_TIME_MILLIS);
    private static final String TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC =
            MetricRegistry.name(TimestampService.class, "getFreshTimestamp");

    private static final String LEADER_UUID_PATH = "/leader/uuid";
    private static final MappingBuilder LEADER_UUID_MAPPING = WireMock.get(WireMock.urlEqualTo(LEADER_UUID_PATH));
    private static final String TIMESTAMP_PATH = "/timestamp/fresh-timestamp";
    private static final MappingBuilder TIMESTAMP_MAPPING = WireMock.post(WireMock.urlEqualTo(TIMESTAMP_PATH));
    private static final String LOCK_PATH = "/lock/current-time-millis";
    private static final MappingBuilder LOCK_MAPPING = WireMock.post(WireMock.urlEqualTo(LOCK_PATH));

    private static final String FEEDBACK_PATH = "/tl/feedback/reportFeedback";
    private static final MappingBuilder FEEDBACK_MAPPING = WireMock.post(WireMock.urlEqualTo(FEEDBACK_PATH));

    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));

    private final TimeLockMigrator migrator = mock(TimeLockMigrator.class);
    private final LockAndTimestampServices lockAndTimestampServices = mock(LockAndTimestampServices.class);
    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final DialogueClients.ReloadingFactory reloadingFactory = DialogueClients.create(
            Refreshable.only(ServicesConfigBlock.builder().build()));

    private int availablePort;

    private TimeLockRuntimeConfig timeLockRuntimeConfig;
    private ServerListConfig rawRemoteServerConfig;
    private AtlasDbConfig config;
    private AtlasDbRuntimeConfig mockAtlasDbRuntimeConfig;

    private Consumer<Object> environment;
    private TimestampStoreInvalidator invalidator;
    private Consumer<Runnable> originalAsyncMethod;

    private InMemoryTimelockServices services;

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder(Files.currentFolder());

    @Rule
    public WireMockRule availableServer =
            new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public TemporaryFolder inMemoryTimeLockFolder = new TemporaryFolder();

    @Before
    public void setup() {
        // Change code to run synchronously, but with a timeout in case something's gone horribly wrong
        originalAsyncMethod = TransactionManagers.runAsync;
        TransactionManagers.runAsync =
                task -> Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(task::run);

        availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(WireMock.aResponse()
                .withStatus(200)
                .withBody(("\"" + UUID.randomUUID().toString() + "\"").getBytes(StandardCharsets.UTF_8))));
        availableServer.stubFor(TIMESTAMP_MAPPING.willReturn(
                WireMock.aResponse().withStatus(200).withBody("1")));
        availableServer.stubFor(
                LOCK_MAPPING.willReturn(WireMock.aResponse().withStatus(200).withBody("2")));
        availableServer.stubFor(FEEDBACK_MAPPING.willReturn(WireMock.aResponse().withStatus(204)));

        config = mock(AtlasDbConfig.class);
        when(config.leader()).thenReturn(Optional.empty());
        when(config.timestamp()).thenReturn(Optional.empty());
        when(config.lock()).thenReturn(Optional.empty());
        when(config.timelock()).thenReturn(Optional.empty());
        when(config.keyValueService()).thenReturn(new InMemoryAtlasDbConfig());
        when(config.initializeAsync()).thenReturn(false);

        mockAtlasDbRuntimeConfig = mock(AtlasDbRuntimeConfig.class);
        when(mockAtlasDbRuntimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(false));
        when(mockAtlasDbRuntimeConfig.timelockRuntime()).thenReturn(Optional.empty());
        when(mockAtlasDbRuntimeConfig.remotingClient()).thenReturn(RemotingClientConfigs.DEFAULT);

        environment = mock(Consumer.class);

        invalidator = mock(TimestampStoreInvalidator.class);
        when(invalidator.backupAndInvalidate()).thenReturn(EMBEDDED_BOUND);

        availablePort = availableServer.port();

        rawRemoteServerConfig = ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort))
                .sslConfiguration(SSL_CONFIGURATION)
                .build();

        services = InMemoryTimelockServices.create(inMemoryTimeLockFolder);
    }

    @After
    public void restoreAsyncExecution() {
        TransactionManagers.runAsync = originalAsyncMethod;
        services.close();
        ;
    }

    @Test
    public void cannotProvideRuntimeConfigTwice() {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .build();
        assertThatThrownBy(() -> TransactionManagers.builder()
                        .config(atlasDbConfig)
                        .userAgent(USER_AGENT)
                        .globalMetricsRegistry(new MetricRegistry())
                        .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                        .registrar(environment)
                        .runtimeConfig(Refreshable.only(Optional.empty()))
                        .runtimeConfigSupplier(Optional::empty)
                        .build())
                .hasMessage("Cannot provide both Refreshable and Supplier of runtime config");
    }

    @Test
    public void userAgentsPresentOnRequestsToRemoteTimestampAndLockServices() {
        setUpRemoteTimestampAndLockBlocksInConfig();

        verifyUserAgentOnRawTimestampAndLockRequests();
    }

    @Test
    public void userAgentsPresentOnRequestsWithLeaderBlockConfigured() throws IOException {
        setUpLeaderBlockInConfig();

        verifyUserAgentOnRawTimestampAndLockRequests();
    }

    @Test
    public void remoteCallsStillMadeIfPingableLeader404s() throws IOException {
        setUpForRemoteServices();
        setUpLeaderBlockInConfig();

        LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        availableServer.verify(WireMock.getRequestedFor(WireMock.urlMatching(LEADER_UUID_PATH)));

        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.lock().currentTimeMillis();

        // TODO (jkong): Assert v2 once we move Leader Block to v2 as well
        availableServer.verify(WireMock.postRequestedFor(WireMock.urlMatching(TIMESTAMP_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.containing(EXPECTED_USER_AGENT_STRING)));
        availableServer.verify(WireMock.postRequestedFor(WireMock.urlMatching(LOCK_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.containing(EXPECTED_USER_AGENT_STRING)));
    }

    @Test
    public void remoteCallsElidedIfTalkingToLocalServer() throws IOException {
        setUpForLocalServices();
        setUpLeaderBlockInConfig();

        LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        availableServer.verify(WireMock.getRequestedFor(WireMock.urlMatching(LEADER_UUID_PATH)));

        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.lock().currentTimeMillis();

        availableServer.verify(
                0,
                WireMock.postRequestedFor(WireMock.urlMatching(TIMESTAMP_PATH))
                        .withHeader(USER_AGENT_HEADER, WireMock.equalTo(EXPECTED_USER_AGENT_STRING)));
        availableServer.verify(
                0,
                WireMock.postRequestedFor(WireMock.urlMatching(LOCK_PATH))
                        .withHeader(USER_AGENT_HEADER, WireMock.equalTo(EXPECTED_USER_AGENT_STRING)));
    }

    @Test
    public void tryUnlockIsAsync() throws IOException {
        setUpForLocalServices();
        setUpLeaderBlockInConfig();

        ThreadLocal<Boolean> inRequest = ThreadLocal.withInitial(() -> false);
        Supplier<LockService> lockServiceSupplier = () -> {
            LockService lockService = LockServiceImpl.create();
            return new AutoDelegate_LockService() {

                @Override
                public boolean unlock(LockRefreshToken token) {
                    assertThat(inRequest.get())
                            .describedAs("unlock was synchronous")
                            .isFalse();
                    return delegate().unlock(token);
                }

                @Override
                public LockService delegate() {
                    return lockService;
                }
            };
        };

        LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();

        LockRequest lockRequest = LockRequest.builder(
                        ImmutableSortedMap.of(StringLockDescriptor.of("foo"), LockMode.WRITE))
                .build();
        LockResponse lockResponse = lockAndTimestamp
                .timelock()
                .lock(com.palantir.lock.v2.LockRequest.of(lockRequest.getLockDescriptors(), 0));
        assertThat(lockResponse.wasSuccessful()).isTrue();

        try {
            inRequest.set(true);
            lockAndTimestamp.timelock().tryUnlock(ImmutableSet.of(lockResponse.getToken()));
        } finally {
            inRequest.set(false);
        }
    }

    @Test
    public void setsGlobalDefaultLockTimeout() {
        TimeDuration expectedTimeout = SimpleTimeDuration.of(47, TimeUnit.SECONDS);
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .defaultLockTimeoutSeconds((int) expectedTimeout.getTime())
                .build();
        TransactionManager tm = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build()
                .serializable();

        assertThat(LockRequest.getDefaultLockTimeout()).isEqualTo(expectedTimeout);

        LockRequest lockRequest = LockRequest.builder(
                        ImmutableSortedMap.of(StringLockDescriptor.of("foo"), LockMode.WRITE))
                .build();
        assertThat(lockRequest.getLockTimeout()).isEqualTo(expectedTimeout);
        tm.close();
    }

    @Test
    public void canCreateInMemory() {
        TransactionManagers.createInMemory(GenericTestSchema.getSchema());
    }

    @Test
    public void canCreateInMemoryWithSetOfSchemas() {
        TransactionManagers.createInMemory(ImmutableSet.of(GenericTestSchema.getSchema()));
    }

    @Test
    public void canDropTablesWhenSweepQueueWritesAreDisabled() {
        AtlasDbConfig inMemoryNoQueueWrites = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .targetedSweep(ImmutableTargetedSweepInstallConfig.builder()
                        .enableSweepQueueWrites(false)
                        .build())
                .build();
        KeyValueService kvs = TransactionManagers.builder()
                .config(inMemoryNoQueueWrites)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .build()
                .serializable()
                .getKeyValueService();

        TableReference testTable = TableReference.createFromFullyQualifiedName("test.test");

        kvs.createTable(testTable, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertThat(kvs.getAllTableNames()).contains(testTable);

        kvs.dropTable(testTable);
        assertThat(kvs.getAllTableNames()).doesNotContain(testTable);
    }

    @Test
    public void runsClosingCallbackOnShutdown() throws Exception {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .defaultLockTimeoutSeconds(120)
                .build();

        Runnable callback = mock(Runnable.class);

        TransactionManager manager = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build()
                .serializable();
        manager.registerClosingCallback(callback);
        manager.close();
        verify(callback, times(1)).run();
    }

    @Test
    public void keyValueServiceMetricsDoNotContainUserAgent() {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .build();

        MetricRegistry metrics = new MetricRegistry();
        TransactionManager tm = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(metrics)
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build()
                .serializable();
        assertThat(metrics.getNames().stream().anyMatch(metricName -> metricName.contains(USER_AGENT_NAME)))
                .isFalse();
        tm.close();
    }

    @Test
    public void overriddenServiceNameIsReturned() {
        KeyValueServiceConfig kvs = new InMemoryAtlasDbConfig();
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(kvs)
                .namespace(Optional.of("namespace"))
                .build();
        MetricRegistry metrics = new MetricRegistry();
        TransactionManagers transactionManagers = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(metrics)
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .serviceIdentifierOverride("overriden")
                .build();

        assertThat(transactionManagers.serviceName()).isEqualTo("overriden");
    }

    @Test
    public void serviceNameIsFetchedFromAtlasConfig() {
        KeyValueServiceConfig kvs = new InMemoryAtlasDbConfig();
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(kvs)
                .namespace(Optional.of("namespace"))
                .build();
        MetricRegistry metrics = new MetricRegistry();
        TransactionManagers transactionManagers = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(metrics)
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build();
        assertThat(transactionManagers.serviceName()).isEqualTo("namespace");
    }

    @Test
    public void serviceNameIsFetchedFromKvsConfigWhenItIsNotPresentInAtlasConfig() {
        KeyValueServiceConfig kvs = mock(KeyValueServiceConfig.class);
        when(kvs.type()).thenReturn("memory");
        when(kvs.namespace()).thenReturn(Optional.of("namespace"));
        when(kvs.concurrentGetRangesThreadPoolSize()).thenReturn(1);

        AtlasDbConfig atlasDbConfig =
                ImmutableAtlasDbConfig.builder().keyValueService(kvs).build();
        MetricRegistry metrics = new MetricRegistry();
        TransactionManagers transactionManagers = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(metrics)
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build();

        assertThat(transactionManagers.serviceName()).isEqualTo("namespace");
    }

    @Test
    public void serviceNameFallsBackToDefaultWhenNamespaceIsNotPresent() {
        KeyValueServiceConfig kvs = new InMemoryAtlasDbConfig();
        AtlasDbConfig atlasDbConfig =
                ImmutableAtlasDbConfig.builder().keyValueService(kvs).build();
        MetricRegistry metrics = new MetricRegistry();
        TransactionManagers transactionManagers = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(metrics)
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build();

        assertThat(transactionManagers.serviceName()).isEqualTo("UNKNOWN");
    }

    @Test
    public void interruptingLocalLeaderElectionTerminates() throws IOException {
        Leaders.LocalPaxosServices localPaxosServices = Leaders.createAndRegisterLocalServices(
                metricsManager,
                environment,
                ImmutableLeaderConfig.builder()
                        .localServer("https://example")
                        .quorumSize(1)
                        .addLeaders("https://example")
                        .acceptorLogDir(temporaryFolder.newFolder())
                        .learnerLogDir(temporaryFolder.newFolder())
                        .build(),
                USER_AGENT);
        LockService lockService = LockServiceImpl.create();
        LockService leadershipLock = AwaitingLeadershipProxy.newProxyInstance(
                LockService.class, () -> lockService, localPaxosServices.leadershipCoordinator());
        LockService localOrRemoteLock = LocalOrRemoteProxy.newProxyInstance(
                LockService.class, leadershipLock, null, CompletableFuture.completedFuture(true));
        try {
            Thread.currentThread().interrupt();
            assertThatCode(localOrRemoteLock::currentTimeMillis)
                    .as("proxy correctly handles interrupts")
                    .isInstanceOf(SafeIllegalStateException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // clear the interrupt flag to avoid affecting future tests
            Thread.interrupted();
        }
    }

    @Test
    public void grabImmutableTsLockIsConfiguredWithBuilderOption() {
        TransactionConfig transactionConfig =
                ImmutableTransactionConfig.builder().build();

        assertThat(withLockImmutableTsOnReadOnlyTransaction(true)
                        .withConsolidatedGrabImmutableTsLockFlag(transactionConfig)
                        .lockImmutableTsOnReadOnlyTransactions())
                .isTrue();

        assertThat(withLockImmutableTsOnReadOnlyTransaction(false)
                        .withConsolidatedGrabImmutableTsLockFlag(transactionConfig)
                        .lockImmutableTsOnReadOnlyTransactions())
                .isFalse();
    }

    @Test
    public void useRuntimeConfigFlagIfBuilderOptionIsSetToFalse() {
        TransactionConfig transactionConfigLocking = ImmutableTransactionConfig.builder()
                .lockImmutableTsOnReadOnlyTransactions(true)
                .build();

        TransactionConfig transactionConfigNotLocking = ImmutableTransactionConfig.builder()
                .lockImmutableTsOnReadOnlyTransactions(false)
                .build();

        assertThat(withLockImmutableTsOnReadOnlyTransaction(false)
                        .withConsolidatedGrabImmutableTsLockFlag(transactionConfigLocking)
                        .lockImmutableTsOnReadOnlyTransactions())
                .isTrue();

        assertThat(withLockImmutableTsOnReadOnlyTransaction(false)
                        .withConsolidatedGrabImmutableTsLockFlag(transactionConfigNotLocking)
                        .lockImmutableTsOnReadOnlyTransactions())
                .isFalse();
    }

    private TransactionManagers withLockImmutableTsOnReadOnlyTransaction(boolean option) {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .build();

        return TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .lockImmutableTsOnReadOnlyTransactions(option)
                .build();
    }

    @Test
    public void metricsAreNotDirectlyReportedFromLocalService() throws IOException {
        setUpForLocalServices();
        setUpLeaderBlockInConfig();

        assertThatTimeAndLockMetricsAreNotRecorded(
                TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC, LOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingRemoteService() throws IOException {
        setUpForRemoteServices();
        setUpLeaderBlockInConfig();

        assertThatTimeAndLockMetricsWithTagsAreRecorded(
                TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                NAMESPACE_AGNOSTIC_LOCK_RPC_CLIENT_CURRENT_TIME_METRIC,
                ImmutableMap.of());
    }

    @Test
    public void timeLockMigrationReportsReadyIfMigrationDone() {
        when(migrator.isInitialized()).thenReturn(true);
        when(lockAndTimestampServices.migrator()).thenReturn(Optional.of(migrator));

        assertThat(TransactionManagers.timeLockMigrationCompleteIfNeeded(lockAndTimestampServices))
                .isTrue();
    }

    @Test
    public void timeLockMigrationReportsNotReadyIfMigrationNotDone() {
        when(migrator.isInitialized()).thenReturn(false);
        when(lockAndTimestampServices.migrator()).thenReturn(Optional.of(migrator));

        assertThat(TransactionManagers.timeLockMigrationCompleteIfNeeded(lockAndTimestampServices))
                .isFalse();
    }

    @Test
    public void timeLockMigrationReportsReadyIfMigrationNotNeeded() {
        when(lockAndTimestampServices.migrator()).thenReturn(Optional.empty());

        assertThat(TransactionManagers.timeLockMigrationCompleteIfNeeded(lockAndTimestampServices))
                .isTrue();
    }

    @Test
    public void throwsIfInstallConfigHasLeaderBlockButInitialRuntimeConfigContainsTimeLockBlock() throws IOException {
        setUpLeaderBlockInConfig();
        setUpTimeLockBlockInRuntimeConfig();
        assertGetLockAndTimestampServicesThrows();
    }

    @Test
    public void throwsIfInstallConfigHasRemoteBlockButInitialRuntimeConfigContainsTimeLockBlock() {
        setUpRemoteTimestampAndLockBlocksInConfig();
        setUpTimeLockBlockInRuntimeConfig();
        assertGetLockAndTimestampServicesThrows();
    }

    @Test
    public void timelockServiceStatusReturnsHealthyWithoutRequest() {
        TransactionManager tm = TransactionManagers.createInMemory(GenericTestSchema.getSchema());
        assertThat(tm.getTimelockServiceStatus().isHealthy()).isTrue();
    }

    @Test
    public void timelockServiceStatusReturnsHealthyAfterSuccessfulRequests() {
        TransactionManager tm = TransactionManagers.createInMemory(GenericTestSchema.getSchema());
        tm.getUnreadableTimestamp();
        assertThat(tm.getTimelockServiceStatus().isHealthy()).isTrue();
    }

    @Test
    public void asyncInitializationEventuallySucceeds() {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAsyncAtlasDbConfig())
                .initializeAsync(true)
                .build();

        TransactionManager manager = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .addSchemas(GenericTestSchema.getSchema())
                .build()
                .serializable();

        assertThat(manager.isInitialized()).isFalse();
        assertThatThrownBy(() -> manager.runTaskWithRetry(unused -> null)).isInstanceOf(NotInitializedException.class);

        Awaitility.await().atMost(Duration.ofSeconds(12)).until(manager::isInitialized);

        performTransaction(manager);
    }

    @Test
    public void kvsRecordsSweepStatsIfBothSweepQueueWritesAndTargetedSweepDisabled() {
        KeyValueService keyValueService = initializeKeyValueServiceWithSweepSettings(false, false);
        assertThat(isSweepStatsKvsPresentInDelegatingChain(keyValueService)).isTrue();
    }

    @Test
    public void kvsRecordsSweepStatsIfSweepQueueWritesDisabledButTargetedSweepEnabled() {
        KeyValueService keyValueService = initializeKeyValueServiceWithSweepSettings(false, true);
        assertThat(isSweepStatsKvsPresentInDelegatingChain(keyValueService)).isTrue();
    }

    @Test
    public void kvsRecordsSweepStatsIfSweepQueueWritesEnabledButTargetedSweepDisabled() {
        KeyValueService keyValueService = initializeKeyValueServiceWithSweepSettings(true, false);
        assertThat(isSweepStatsKvsPresentInDelegatingChain(keyValueService)).isTrue();
    }

    @Test
    public void kvsDoesNotRecordSweepStatsIfSweepQueueWritesAndTargetedSweepEnabled() {
        KeyValueService keyValueService = initializeKeyValueServiceWithSweepSettings(true, true);
        assertThat(isSweepStatsKvsPresentInDelegatingChain(keyValueService)).isFalse();
    }

    private KeyValueService initializeKeyValueServiceWithSweepSettings(
            boolean enableSweepQueueWrites, boolean enableTargetedSweep) {
        AtlasDbConfig installConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .targetedSweep(ImmutableTargetedSweepInstallConfig.builder()
                        .enableSweepQueueWrites(enableSweepQueueWrites)
                        .build())
                .build();
        AtlasDbRuntimeConfig atlasDbRuntimeConfig = ImmutableAtlasDbRuntimeConfig.builder()
                .targetedSweep(ImmutableTargetedSweepRuntimeConfig.builder()
                        .enabled(enableTargetedSweep)
                        .build())
                .build();

        TransactionManager manager = TransactionManagers.builder()
                .config(installConfig)
                .userAgent(UserAgent.of(UserAgent.Agent.of("test", "0.0.0")))
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .addSchemas(GenericTestSchema.getSchema())
                .runtimeConfigSupplier(() -> Optional.of(atlasDbRuntimeConfig))
                .build()
                .serializable();
        return manager.getKeyValueService();
    }

    private static boolean isSweepStatsKvsPresentInDelegatingChain(KeyValueService keyValueService) {
        if (keyValueService instanceof SweepStatsKeyValueService) {
            return true;
        }
        Collection<? extends KeyValueService> services = keyValueService.getDelegates();
        return services.stream().anyMatch(TransactionManagersTest::isSweepStatsKvsPresentInDelegatingChain);
    }

    private static void performTransaction(TransactionManager manager) {
        RangeScanTestTable.RangeScanTestRow testRow = RangeScanTestTable.RangeScanTestRow.of("foo");
        manager.runTaskWithRetry(tx -> {
            GenericTestSchemaTableFactory.of().getRangeScanTestTable(tx).putColumn1(testRow, 12345L);
            return null;
        });
        Map<RangeScanTestTable.RangeScanTestRow, Long> result = manager.runTaskWithRetry(tx ->
                GenericTestSchemaTableFactory.of().getRangeScanTestTable(tx).getColumn1s(ImmutableSet.of(testRow)));

        assertThat(Iterables.getOnlyElement(result.entrySet()).getValue()).isEqualTo(12345L);
    }

    private void assertThatTimeAndLockMetricsAreNotRecorded(String timestampMetric, String lockMetric) {
        assertThat(metricsManager.getRegistry().timer(timestampMetric).getCount())
                .isEqualTo(0L);
        assertThat(metricsManager.getRegistry().timer(lockMetric).getCount()).isEqualTo(0L);

        LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.timelock().currentTimeMillis();

        assertThat(metricsManager.getRegistry().timer(timestampMetric).getCount())
                .isEqualTo(0L);
        assertThat(metricsManager.getRegistry().timer(lockMetric).getCount()).isEqualTo(0L);
    }

    private void assertThatTimeAndLockMetricsWithTagsAreRecorded(
            String timestampMetric, String lockMetric, Map<String, String> tags) {
        MetricName timestampMetricName = MetricName.builder()
                .safeName(timestampMetric)
                .putAllSafeTags(tags)
                .build();
        MetricName lockMetricName =
                MetricName.builder().safeName(lockMetric).putAllSafeTags(tags).build();

        assertThat(metricsManager.getTaggedRegistry().timer(timestampMetricName).getCount())
                .isEqualTo(0L);
        assertThat(metricsManager.getTaggedRegistry().timer(lockMetricName).getCount())
                .isEqualTo(0L);

        LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.timelock().currentTimeMillis();

        assertThat(metricsManager.getTaggedRegistry().timer(timestampMetricName).getCount())
                .isEqualTo(1L);
        assertThat(metricsManager.getTaggedRegistry().timer(lockMetricName).getCount())
                .isEqualTo(1L);
    }

    private void setUpForLocalServices() throws IOException {
        doAnswer(invocation -> {
                    // Configure our server to reply with the same server ID as the registered PingableLeader.
                    PingableLeader localPingableLeader = invocation.getArgument(0);
                    availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(WireMock.aResponse()
                            .withStatus(200)
                            .withBody(("\"" + localPingableLeader.getUUID() + "\"").getBytes(StandardCharsets.UTF_8))));
                    return null;
                })
                .when(environment)
                .accept(isA(PingableLeader.class));
        setUpLeaderBlockInConfig();
    }

    private void setUpForRemoteServices() throws IOException {
        availableServer.stubFor(
                LEADER_UUID_MAPPING.willReturn(WireMock.aResponse().withStatus(404)));
        setUpLeaderBlockInConfig();
    }

    private void setUpTimeLockBlockInRuntimeConfig() {
        when(mockAtlasDbRuntimeConfig.timelockRuntime())
                .thenReturn(Optional.of(ImmutableTimeLockRuntimeConfig.builder()
                        .serversList(ImmutableServerListConfig.builder()
                                .addServers(getUriForPort(availablePort))
                                .sslConfiguration(SSL_CONFIGURATION)
                                .build())
                        .build()));
    }

    private void setUpRemoteTimestampAndLockBlocksInConfig() {
        when(config.timestamp()).thenReturn(Optional.of(rawRemoteServerConfig));
        when(config.lock()).thenReturn(Optional.of(rawRemoteServerConfig));
    }

    private void setUpLeaderBlockInConfig() throws IOException {
        when(config.leader())
                .thenReturn(Optional.of(ImmutableLeaderConfig.builder()
                        .localServer(getUriForPort(availablePort))
                        .addLeaders(getUriForPort(availablePort))
                        .acceptorLogDir(temporaryFolder.newFolder())
                        .learnerLogDir(temporaryFolder.newFolder())
                        .quorumSize(1)
                        .sslConfiguration(SSL_CONFIGURATION)
                        .build()));
    }

    private LockAndTimestampServices getLockAndTimestampServices() {
        ManagedTimestampService ts = services.getManagedTimestampService();
        return new DefaultLockAndTimestampServiceFactory(
                        metricsManager,
                        config,
                        Refreshable.only(mockAtlasDbRuntimeConfig),
                        environment,
                        LockServiceImpl::create,
                        () -> ts,
                        invalidator,
                        USER_AGENT,
                        Optional.empty(),
                        reloadingFactory,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableSet.of())
                .createLockAndTimestampServices();
        //        return new InMemoryLockAndTimestampServiceFactory(services).createLockAndTimestampServices();
    }

    private void verifyUserAgentOnRawTimestampAndLockRequests() {
        verifyUserAgentOnTimestampAndLockRequests(TIMESTAMP_PATH, LOCK_PATH);
    }

    private void verifyUserAgentOnTimestampAndLockRequests(String timestampPath, String lockPath) {
        LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.timelock().currentTimeMillis();

        // TODO (jkong): Assert v2 once we move all paths to v2
        availableServer.verify(WireMock.postRequestedFor(WireMock.urlMatching(timestampPath))
                .withHeader(USER_AGENT_HEADER, WireMock.containing(EXPECTED_USER_AGENT_STRING)));
        availableServer.verify(WireMock.postRequestedFor(WireMock.urlMatching(lockPath))
                .withHeader(USER_AGENT_HEADER, WireMock.containing(EXPECTED_USER_AGENT_STRING)));
    }

    @Test
    public void sanityCheckFeedbackReportingPipeline() {
        setUpTimeLockConfig();
        verifyFeedbackIsReportedToService();
    }

    private void setUpTimeLockConfig() {
        timeLockRuntimeConfig = getTimelockRuntimeConfig(ImmutableList.of(getUriForPort(availablePort)));
        when(mockAtlasDbRuntimeConfig.timelockRuntime()).thenReturn(Optional.of(timeLockRuntimeConfig));
    }

    private void verifyFeedbackIsReportedToService() {
        AuthHeader authHeader = AuthHeader.valueOf("Bearer omitted");
        SettableRefreshable<AtlasDbRuntimeConfig> refreshableRuntimeConfig =
                Refreshable.create(mockAtlasDbRuntimeConfig);
        Refreshable<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices =
                TransactionManagers.getTimeLockClientFeedbackServices(
                        config,
                        refreshableRuntimeConfig,
                        USER_AGENT,
                        DialogueClients.create(
                                Refreshable.only(ServicesConfigBlock.builder().build())));
        ConjureTimeLockClientFeedback feedbackReport = ConjureTimeLockClientFeedback.builder()
                .atlasVersion("1.0")
                .serviceName("service")
                .nodeId(UUID.randomUUID())
                .build();
        assertThat(timeLockClientFeedbackServices.current()).hasSize(1);
        timeLockClientFeedbackServices.current().get(0).reportFeedback(authHeader, feedbackReport);
        List<LoggedRequest> requests = WireMock.findAll(WireMock.postRequestedFor(WireMock.urlMatching(FEEDBACK_PATH)));
        Assertions.assertThat(requests).hasSize(1);
        availableServer.verify(WireMock.postRequestedFor(WireMock.urlMatching(FEEDBACK_PATH))
                .withHeader("Authorization", WireMock.containing("Bearer omitted")));

        /* config with no servers */
        timeLockRuntimeConfig = getTimelockRuntimeConfig(ImmutableList.of());
        mockAtlasDbRuntimeConfig = mock(AtlasDbRuntimeConfig.class);
        when(mockAtlasDbRuntimeConfig.timelockRuntime()).thenReturn(Optional.of(timeLockRuntimeConfig));
        refreshableRuntimeConfig.update(mockAtlasDbRuntimeConfig);
        assertThat(timeLockClientFeedbackServices.current()).isEmpty();
    }

    private void assertGetLockAndTimestampServicesThrows() {
        String expectedErrorPrefix =
                "Found a service configured not to use timelock, with a timelock block in" + " the runtime config!";
        assertThatThrownBy(this::getLockAndTimestampServices)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(expectedErrorPrefix);
    }

    private static String getUriForPort(int port) {
        return String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }

    private static TimeLockRuntimeConfig getTimelockRuntimeConfig(List<String> servers) {
        return ImmutableTimeLockRuntimeConfig.builder()
                .serversList(ImmutableServerListConfig.builder()
                        .addAllServers(servers)
                        .sslConfiguration(SSL_CONFIGURATION)
                        .build())
                .build();
    }
}
