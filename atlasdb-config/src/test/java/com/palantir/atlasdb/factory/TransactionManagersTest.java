/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableTimestampClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.qos.config.QosClientConfig;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.util.MetricsRule;
import com.palantir.leader.PingableLeader;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.TimeDuration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class TransactionManagersTest {
    private static final String CLIENT = "testClient";
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT = USER_AGENT_NAME + " (3.14159265)";
    private static final String USER_AGENT_HEADER = "User-Agent";
    private static final long EMBEDDED_BOUND = 3;

    private static final String TIMELOCK_SERVICE_FRESH_TIMESTAMP_METRIC =
            MetricRegistry.name(TimelockService.class, "getFreshTimestamp");
    private static final String TIMELOCK_SERVICE_CURRENT_TIME_METRIC =
            MetricRegistry.name(TimelockService.class, "currentTimeMillis");
    private static final String LOCK_SERVICE_CURRENT_TIME_METRIC =
            MetricRegistry.name(LockService.class, "currentTimeMillis");
    private static final String TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC =
            MetricRegistry.name(TimestampService.class, "getFreshTimestamp");

    private static final String LEADER_UUID_PATH = "/leader/uuid";
    private static final MappingBuilder LEADER_UUID_MAPPING = get(urlEqualTo(LEADER_UUID_PATH));
    private static final String TIMESTAMP_PATH = "/timestamp/fresh-timestamp";
    private static final MappingBuilder TIMESTAMP_MAPPING = post(urlEqualTo(TIMESTAMP_PATH));
    private static final String LOCK_PATH = "/lock/current-time-millis";
    private static final MappingBuilder LOCK_MAPPING = post(urlEqualTo(LOCK_PATH));

    private static final String TIMELOCK_TIMESTAMP_PATH = "/" + CLIENT + "/timelock/fresh-timestamp";
    private static final MappingBuilder TIMELOCK_TIMESTAMP_MAPPING = post(urlEqualTo(TIMELOCK_TIMESTAMP_PATH));
    private static final String TIMELOCK_TIMESTAMPS_PATH = "/" + CLIENT + "/timelock/fresh-timestamps?number=1";
    private static final MappingBuilder TIMELOCK_ONE_TIMESTAMP_MAPPING = post(urlEqualTo(TIMELOCK_TIMESTAMPS_PATH));
    private static final String TIMELOCK_LOCK_PATH = "/" + CLIENT + "/timelock/current-time-millis";
    private static final MappingBuilder TIMELOCK_LOCK_MAPPING = post(urlEqualTo(TIMELOCK_LOCK_PATH));


    private static final String TIMELOCK_PING_PATH =  "/" + CLIENT + "/timestamp-management/ping";
    private static final MappingBuilder TIMELOCK_PING_MAPPING = get(urlEqualTo(TIMELOCK_PING_PATH));
    private static final String TIMELOCK_FF_PATH
            = "/" + CLIENT + "/timestamp-management/fast-forward?currentTimestamp=" + EMBEDDED_BOUND;
    private static final MappingBuilder TIMELOCK_FF_MAPPING = post(urlEqualTo(TIMELOCK_FF_PATH));

    private static final AtlasDbConfig REAL_CONFIG = ImmutableAtlasDbConfig.builder()
            .keyValueService(new InMemoryAtlasDbConfig())
            .defaultLockTimeoutSeconds(62)
            .build();

    private final TimeLockMigrator migrator = mock(TimeLockMigrator.class);
    private final TransactionManagers.LockAndTimestampServices lockAndTimestampServices = mock(
            TransactionManagers.LockAndTimestampServices.class);

    private int availablePort;
    private TimeLockClientConfig mockClientConfig;
    private ServerListConfig rawRemoteServerConfig;

    private AtlasDbConfig config;
    private AtlasDbRuntimeConfig runtimeConfig;
    private Consumer<Object> environment;
    private TimestampStoreInvalidator invalidator;
    private Consumer<Runnable> originalAsyncMethod;
    private Supplier<Optional<AtlasDbRuntimeConfig>> configSupplier;

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public WireMockRule availableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public MetricsRule metricsRule = new MetricsRule();

    @Before
    public void setup() throws JsonProcessingException {
        // Change code to run synchronously, but with a timeout in case something's gone horribly wrong
        originalAsyncMethod = TransactionManagers.runAsync;
        TransactionManagers.runAsync = task -> Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(task::run);

        availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(aResponse().withStatus(200).withBody(
                ("\"" + UUID.randomUUID().toString() + "\"").getBytes())));
        availableServer.stubFor(TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("1")));
        availableServer.stubFor(LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("2")));

        availableServer.stubFor(TIMELOCK_TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("3")));
        availableServer.stubFor(TIMELOCK_ONE_TIMESTAMP_MAPPING.willReturn(aResponse()
                .withStatus(200)
                .withBody(new ObjectMapper().writeValueAsString(TimestampRange.createInclusiveRange(88, 88)))));
        availableServer.stubFor(TIMELOCK_LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("4")));

        availableServer.stubFor(TIMELOCK_PING_MAPPING.willReturn(aResponse()
                .withStatus(200)
                .withBody(TimestampManagementService.PING_RESPONSE)
                .withHeader("Content-Type", "text/plain")));
        availableServer.stubFor(TIMELOCK_FF_MAPPING.willReturn(aResponse()
                .withStatus(204)));

        config = mock(AtlasDbConfig.class);
        when(config.leader()).thenReturn(Optional.empty());
        when(config.timestamp()).thenReturn(Optional.empty());
        when(config.lock()).thenReturn(Optional.empty());
        when(config.timelock()).thenReturn(Optional.empty());
        when(config.keyValueService()).thenReturn(new InMemoryAtlasDbConfig());
        when(config.initializeAsync()).thenReturn(false);

        runtimeConfig = mock(AtlasDbRuntimeConfig.class);
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(false));
        when(runtimeConfig.qos()).thenReturn(QosClientConfig.DEFAULT);
        when(runtimeConfig.timelockRuntime()).thenReturn(Optional.empty());

        environment = mock(Consumer.class);

        invalidator = mock(TimestampStoreInvalidator.class);
        when(invalidator.backupAndInvalidate()).thenReturn(EMBEDDED_BOUND);

        availablePort = availableServer.port();
        mockClientConfig = getTimelockConfigForServers(ImmutableList.of(getUriForPort(availablePort)));
        rawRemoteServerConfig = ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort))
                .build();
        configSupplier = () -> Optional.of(runtimeConfig);
    }

    @After
    public void restoreAsyncExecution() {
        TransactionManagers.runAsync = originalAsyncMethod;
    }

    @Test
    public void userAgentsPresentOnRequestsToTimelockServer() {
        setUpTimeLockBlockInInstallConfig();

        availableServer.stubFor(post(urlMatching("/")).willReturn(aResponse().withStatus(200).withBody("3")));
        availableServer.stubFor(TIMELOCK_LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("4")));

        verifyUserAgentOnTimelockTimestampAndLockRequests();
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
    public void remoteCallsStillMadeIfPingableLeader404s() throws IOException, InterruptedException {
        setUpForRemoteServices();
        setUpLeaderBlockInConfig();

        TransactionManagers.LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        availableServer.verify(getRequestedFor(urlMatching(LEADER_UUID_PATH)));

        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.lock().currentTimeMillis();

        availableServer.verify(postRequestedFor(urlMatching(TIMESTAMP_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(postRequestedFor(urlMatching(LOCK_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    @Test
    public void remoteCallsElidedIfTalkingToLocalServer() throws IOException, InterruptedException {
        setUpForLocalServices();
        setUpLeaderBlockInConfig();

        TransactionManagers.LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        availableServer.verify(getRequestedFor(urlMatching(LEADER_UUID_PATH)));

        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.lock().currentTimeMillis();

        availableServer.verify(0, postRequestedFor(urlMatching(TIMESTAMP_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(0, postRequestedFor(urlMatching(LOCK_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    @Test
    public void setsGlobalDefaultLockTimeout() {
        TimeDuration expectedTimeout = SimpleTimeDuration.of(47, TimeUnit.SECONDS);
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .defaultLockTimeoutSeconds((int) expectedTimeout.getTime())
                .build();
        TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent("test")
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build()
                .serializable();

        assertEquals(expectedTimeout, LockRequest.getDefaultLockTimeout());

        LockRequest lockRequest = LockRequest
                .builder(ImmutableSortedMap.of(StringLockDescriptor.of("foo"),
                        LockMode.WRITE)).build();
        assertEquals(expectedTimeout, lockRequest.getLockTimeout());
    }

    @Test
    public void canCreateInMemory() {
        TransactionManagers.createInMemory(GenericTestSchema.getSchema());
    }

    @Test
    public void canCreateInMemoryWithSetOfSchemas() {
        TransactionManagers.createInMemory(ImmutableSet.of(
                GenericTestSchema.getSchema()));
    }

    @Test
    public void batchesRequestsIfBatchingEnabled() throws InterruptedException {
        setUpTimeLockBlockInInstallConfig();
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(true));

        createLockAndTimestampServicesForConfig(config, runtimeConfig).timestamp().getFreshTimestamp();

        availableServer.verify(postRequestedFor(urlEqualTo(TIMELOCK_TIMESTAMPS_PATH)));
    }

    @Test
    public void doesNotBatchRequestsIfBatchingNotEnabled() {
        setUpTimeLockBlockInInstallConfig();
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(false));

        createLockAndTimestampServicesForConfig(config, runtimeConfig).timelock().getFreshTimestamp();

        availableServer.verify(postRequestedFor(urlEqualTo(TIMELOCK_TIMESTAMP_PATH)));
    }

    @Test
    public void runsClosingCallbackOnShutdown() throws Exception {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .defaultLockTimeoutSeconds(120)
                .build();

        Runnable callback = mock(Runnable.class);

        SerializableTransactionManager manager = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent("test")
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

        TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent("test")
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build()
                .serializable();
        assertThat(metricsRule.metrics().getNames().stream()
                .anyMatch(metricName -> metricName.contains(USER_AGENT_NAME)), is(false));
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingLocalService() throws IOException, InterruptedException {
        setUpForLocalServices();
        setUpLeaderBlockInConfig();

        assertThatTimeAndLockMetricsAreRecorded(TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                LOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingRemoteService() throws IOException, InterruptedException {
        setUpForRemoteServices();
        setUpLeaderBlockInConfig();

        assertThatTimeAndLockMetricsAreRecorded(TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                LOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingTimelockService() {
        setUpTimeLockBlockInInstallConfig();

        assertThatTimeAndLockMetricsAreRecorded(TIMELOCK_SERVICE_FRESH_TIMESTAMP_METRIC,
                TIMELOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingTimelockServiceWithRequestBatching() {
        setUpTimeLockBlockInInstallConfig();
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(true));

        assertThatTimeAndLockMetricsAreRecorded(TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                TIMELOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void timeLockMigrationReportsReadyIfMigrationDone() {
        when(migrator.isInitialized()).thenReturn(true);
        when(lockAndTimestampServices.migrator()).thenReturn(Optional.of(migrator));

        assertTrue(TransactionManagers.timeLockMigrationCompleteIfNeeded(lockAndTimestampServices));
    }

    @Test
    public void timeLockMigrationReportsNotReadyIfMigrationNotDone() {
        when(migrator.isInitialized()).thenReturn(false);
        when(lockAndTimestampServices.migrator()).thenReturn(Optional.of(migrator));

        assertFalse(TransactionManagers.timeLockMigrationCompleteIfNeeded(lockAndTimestampServices));
    }

    @Test
    public void timeLockMigrationReportsReadyIfMigrationNotNeeded() {
        when(lockAndTimestampServices.migrator()).thenReturn(Optional.empty());

        assertTrue(TransactionManagers.timeLockMigrationCompleteIfNeeded(lockAndTimestampServices));
    }

    @Test
    public void usesTimeLockIfInstallConfigIsUnspecifiedButInitialRuntimeConfigContainsTimeLockBlock() {
        setUpTimeLockBlockInRuntimeConfig();
        verifyUsingTimeLockByGettingAFreshTimestamp();
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
    public void usesTimeLockIfInstallConfigIsTimeLockAndInitialRuntimeConfigContainsTimeLockBlock() {
        setUpTimeLockBlockInInstallConfig();
        setUpTimeLockBlockInRuntimeConfig();
        verifyUsingTimeLockByGettingAFreshTimestamp();
    }

    @Test
    public void usesTimeLockIfInstallConfigIsTimeLockAndInitialRuntimeConfigDoesNotContainTimeLockBlock() {
        setUpTimeLockBlockInInstallConfig();
        verifyUsingTimeLockByGettingAFreshTimestamp();

        assertTrue("Runtime config was not expected to contain a timelock block",
                !runtimeConfig.timelockRuntime().isPresent());
    }

    private void verifyUsingTimeLockByGettingAFreshTimestamp() {
        when(config.namespace()).thenReturn(Optional.of(CLIENT));
        getLockAndTimestampServices().timelock().getFreshTimestamp();
        availableServer.verify(1, postRequestedFor(urlMatching(TIMELOCK_TIMESTAMP_PATH)));
    }

    private void assertThatTimeAndLockMetricsAreRecorded(String timestampMetric, String lockMetric) {
        assertThat(metricsRule.metrics().timer(timestampMetric).getCount(), is(equalTo(0L)));
        assertThat(metricsRule.metrics().timer(lockMetric).getCount(), is(equalTo(0L)));

        TransactionManagers.LockAndTimestampServices lockAndTimestamp = getLockAndTimestampServices();
        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.timelock().currentTimeMillis();

        assertThat(metricsRule.metrics().timer(timestampMetric).getCount(), is(equalTo(1L)));
        assertThat(metricsRule.metrics().timer(lockMetric).getCount(), is(equalTo(1L)));
    }

    private void setUpForLocalServices() throws IOException {
        doAnswer(invocation -> {
            // Configure our server to reply with the same server ID as the registered PingableLeader.
            PingableLeader localPingableLeader = invocation.getArgument(0);
            availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(aResponse()
                    .withStatus(200)
                    .withBody(("\"" + localPingableLeader.getUUID().toString() + "\"").getBytes())));
            return null;
        }).when(environment).accept(isA(PingableLeader.class));
        setUpLeaderBlockInConfig();
    }

    private void setUpForRemoteServices() throws IOException {
        availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(aResponse().withStatus(404)));
        setUpLeaderBlockInConfig();
    }

    private void setUpTimeLockBlockInInstallConfig() {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));
    }

    private void setUpTimeLockBlockInRuntimeConfig() {
        when(runtimeConfig.timelockRuntime()).thenReturn(
                Optional.of(ImmutableTimeLockRuntimeConfig.builder()
                        .serversList(rawRemoteServerConfig)
                        .build()));
    }

    private void setUpRemoteTimestampAndLockBlocksInConfig() {
        when(config.timestamp()).thenReturn(Optional.of(rawRemoteServerConfig));
        when(config.lock()).thenReturn(Optional.of(rawRemoteServerConfig));
    }

    private void setUpLeaderBlockInConfig() throws IOException {
        when(config.leader()).thenReturn(Optional.of(ImmutableLeaderConfig.builder()
                .localServer(getUriForPort(availablePort))
                .addLeaders(getUriForPort(availablePort))
                .acceptorLogDir(temporaryFolder.newFolder())
                .learnerLogDir(temporaryFolder.newFolder())
                .quorumSize(1)
                .build()));
    }

    private TransactionManagers.LockAndTimestampServices getLockAndTimestampServices() {
        return TransactionManagers.createLockAndTimestampServices(
                config,
                () -> runtimeConfig,
                environment,
                LockServiceImpl::create,
                InMemoryTimestampService::new,
                invalidator,
                USER_AGENT);
    }

    private void verifyUserAgentOnRawTimestampAndLockRequests() {
        verifyUserAgentOnTimestampAndLockRequests(TIMESTAMP_PATH, LOCK_PATH);
    }

    private void verifyUserAgentOnTimelockTimestampAndLockRequests() {
        verifyUserAgentOnTimestampAndLockRequests(TIMELOCK_TIMESTAMP_PATH, TIMELOCK_LOCK_PATH);
        verify(invalidator, times(1)).backupAndInvalidate();
        availableServer.verify(getRequestedFor(urlEqualTo(TIMELOCK_PING_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(postRequestedFor(urlEqualTo(TIMELOCK_FF_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    private void verifyUserAgentOnTimestampAndLockRequests(String timestampPath, String lockPath) {
        TransactionManagers.LockAndTimestampServices lockAndTimestamp =
                TransactionManagers.createLockAndTimestampServices(
                        config,
                        () -> runtimeConfig,
                        environment,
                        LockServiceImpl::create,
                        InMemoryTimestampService::new,
                        invalidator,
                        USER_AGENT);
        lockAndTimestamp.timelock().getFreshTimestamp();
        lockAndTimestamp.timelock().currentTimeMillis();

        availableServer.verify(postRequestedFor(urlMatching(timestampPath))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(postRequestedFor(urlMatching(lockPath))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    private void assertGetLockAndTimestampServicesThrows() {
        String expectedErrorPrefix = "Found a service configured not to use timelock, with a timelock block in"
                + " the runtime config!";
        assertThatThrownBy(this::getLockAndTimestampServices).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(expectedErrorPrefix);
    }

    private static String getUriForPort(int port) {
        return String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }

    private static TimeLockClientConfig getTimelockConfigForServers(List<String> servers) {
        return ImmutableTimeLockClientConfig.builder()
                .client(CLIENT)
                .serversList(ImmutableServerListConfig.builder()
                        .addAllServers(servers)
                        .build())
                .build();
    }

    private TransactionManagers.LockAndTimestampServices createLockAndTimestampServicesForConfig(
            AtlasDbConfig atlasDbConfig, AtlasDbRuntimeConfig atlasDbRuntimeConfig) {
        return TransactionManagers.createLockAndTimestampServices(
                atlasDbConfig,
                () -> atlasDbRuntimeConfig,
                environment,
                LockServiceImpl::create,
                InMemoryTimestampService::new,
                invalidator,
                USER_AGENT);
    }
}
