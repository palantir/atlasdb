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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ImmutableTimestampClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.util.MetricsRule;
import com.palantir.leader.PingableLeader;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
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

public class TransactionManagersTest {
    private static final String CLIENT = "testClient";
    private static final String USER_AGENT = "user-agent (3.14159265)";
    private static final String USER_AGENT_HEADER = "User-Agent";
    private static final long EMBEDDED_BOUND = 3;

    private static final String TIMELOCK_SERVICE_FRESH_TIMESTAMP_METRIC =
            MetricRegistry.name(TimelockService.class, USER_AGENT, "getFreshTimestamp");
    private static final String TIMELOCK_SERVICE_CURRENT_TIME_METRIC =
            MetricRegistry.name(TimelockService.class, USER_AGENT, "currentTimeMillis");
    private static final String REMOTELOCK_SERVICE_CURRENT_TIME_METRIC =
            MetricRegistry.name(RemoteLockService.class, USER_AGENT, "currentTimeMillis");
    private static final String TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC =
            MetricRegistry.name(TimestampService.class, USER_AGENT, "getFreshTimestamp");

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

    private int availablePort;
    private TimeLockClientConfig mockClientConfig;
    private ServerListConfig rawRemoteServerConfig;

    private AtlasDbConfig config;
    private AtlasDbRuntimeConfig runtimeConfig;
    private TransactionManagers.Environment environment;
    private TimestampStoreInvalidator invalidator;
    private Consumer<Runnable> originalAsyncMethod;

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
        TransactionManagers.runAsync = task -> Awaitility.await().atMost(2, TimeUnit.SECONDS).until(task);

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

        runtimeConfig = mock(AtlasDbRuntimeConfig.class);
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(false));

        environment = mock(TransactionManagers.Environment.class);

        invalidator = mock(TimestampStoreInvalidator.class);
        when(invalidator.backupAndInvalidate()).thenReturn(EMBEDDED_BOUND);

        availablePort = availableServer.port();
        mockClientConfig = getTimelockConfigForServers(ImmutableList.of(getUriForPort(availablePort)));
        rawRemoteServerConfig = ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort))
                .build();
    }

    @After
    public void restoreAsyncExecution() {
        TransactionManagers.runAsync = originalAsyncMethod;
    }

    @Test
    public void userAgentsPresentOnRequestsToTimelockServer() {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));

        availableServer.stubFor(post(urlMatching("/")).willReturn(aResponse().withStatus(200).withBody("3")));
        availableServer.stubFor(TIMELOCK_LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("4")));

        verifyUserAgentOnTimelockTimestampAndLockRequests();
    }

    @Test
    public void userAgentsPresentOnRequestsToRemoteTimestampAndLockServices() {
        when(config.timestamp()).thenReturn(Optional.of(rawRemoteServerConfig));
        when(config.lock()).thenReturn(Optional.of(rawRemoteServerConfig));

        verifyUserAgentOnRawTimestampAndLockRequests();
    }

    @Test
    public void userAgentsPresentOnRequestsWithLeaderBlockConfigured() throws IOException {
        setupLeaderBlockInConfig();

        verifyUserAgentOnRawTimestampAndLockRequests();
    }

    @Test
    public void remoteCallsStillMadeIfPingableLeader404s() throws IOException, InterruptedException {
        setUpForRemoteServices();
        setupLeaderBlockInConfig();

        TransactionManagers.LockAndTimestampServices lockAndTimestampServices = getLockAndTimestampServices();
        availableServer.verify(getRequestedFor(urlMatching(LEADER_UUID_PATH)));

        lockAndTimestampServices.timelock().getFreshTimestamp();
        lockAndTimestampServices.lock().currentTimeMillis();

        availableServer.verify(postRequestedFor(urlMatching(TIMESTAMP_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(postRequestedFor(urlMatching(LOCK_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    @Test
    public void remoteCallsElidedIfTalkingToLocalServer() throws IOException, InterruptedException {
        setUpForLocalServices();
        setupLeaderBlockInConfig();

        TransactionManagers.LockAndTimestampServices lockAndTimestampServices = getLockAndTimestampServices();
        availableServer.verify(getRequestedFor(urlMatching(LEADER_UUID_PATH)));

        lockAndTimestampServices.timelock().getFreshTimestamp();
        lockAndTimestampServices.lock().currentTimeMillis();

        availableServer.verify(0, postRequestedFor(urlMatching(TIMESTAMP_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(0, postRequestedFor(urlMatching(LOCK_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    @Test
    public void setsGlobalDefaultLockTimeout() {
        TimeDuration expectedTimeout = SimpleTimeDuration.of(47, TimeUnit.SECONDS);
        AtlasDbConfig realConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .defaultLockTimeoutSeconds((int) expectedTimeout.getTime())
                .build();
        TransactionManagers.create(realConfig, Optional::empty,
                ImmutableSet.of(), environment, false);

        assertEquals(expectedTimeout, LockRequest.getDefaultLockTimeout());

        LockRequest lockRequest = LockRequest
                .builder(ImmutableSortedMap.of(StringLockDescriptor.of("foo"),
                        LockMode.WRITE)).build();
        assertEquals(expectedTimeout, lockRequest.getLockTimeout());
    }

    @Test
    public void batchesRequestsIfBatchingEnabled() throws InterruptedException {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(true));

        createLockAndTimestampServicesForConfig(config, runtimeConfig).timestamp().getFreshTimestamp();

        availableServer.verify(postRequestedFor(urlEqualTo(TIMELOCK_TIMESTAMPS_PATH)));
    }

    @Test
    public void doesNotBatchRequestsIfBatchingNotEnabled() {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(false));

        createLockAndTimestampServicesForConfig(config, runtimeConfig).timelock().getFreshTimestamp();

        availableServer.verify(postRequestedFor(urlEqualTo(TIMELOCK_TIMESTAMP_PATH)));
    }

    @Test
    public void runsClosingCallbackOnShutdown() throws Exception {
        AtlasDbConfig realConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .defaultLockTimeoutSeconds(120)
                .build();

        Runnable callback = mock(Runnable.class);

        SerializableTransactionManager manager = TransactionManagers.create(
                realConfig, Optional::empty, ImmutableSet.of(), environment, false);
        manager.registerClosingCallback(callback);
        manager.close();
        verify(callback, times(1)).run();
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingLocalService() throws IOException, InterruptedException {
        setUpForLocalServices();
        setupLeaderBlockInConfig();

        assertThatTimeAndLockMetricsAreRecorded(TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                REMOTELOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingRemoteService() throws IOException, InterruptedException {
        setUpForRemoteServices();
        setupLeaderBlockInConfig();

        assertThatTimeAndLockMetricsAreRecorded(TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                REMOTELOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingTimelockService() {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));

        assertThatTimeAndLockMetricsAreRecorded(TIMELOCK_SERVICE_FRESH_TIMESTAMP_METRIC,
                TIMELOCK_SERVICE_CURRENT_TIME_METRIC);
    }

    @Test
    public void metricsAreReportedExactlyOnceWhenUsingTimelockServiceWithRequestBatching() {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));
        when(runtimeConfig.timestampClient()).thenReturn(ImmutableTimestampClientConfig.of(true));

        assertThatTimeAndLockMetricsAreRecorded(TIMESTAMP_SERVICE_FRESH_TIMESTAMP_METRIC,
                TIMELOCK_SERVICE_CURRENT_TIME_METRIC);

    }

    private void assertThatTimeAndLockMetricsAreRecorded(String timestampMetric, String lockMetric) {
        assertThat(metricsRule.metrics().timer(timestampMetric).getCount(), is(equalTo(0L)));
        assertThat(metricsRule.metrics().timer(lockMetric).getCount(), is(equalTo(0L)));

        TransactionManagers.LockAndTimestampServices lockAndTimestampServices = getLockAndTimestampServices();
        lockAndTimestampServices.timelock().getFreshTimestamp();
        lockAndTimestampServices.timelock().currentTimeMillis();

        assertThat(metricsRule.metrics().timer(timestampMetric).getCount(), is(equalTo(1L)));
        assertThat(metricsRule.metrics().timer(lockMetric).getCount(), is(equalTo(1L)));
    }

    private void setUpForLocalServices() throws IOException {
        doAnswer(invocation -> {
            // Configure our server to reply with the same server ID as the registered PingableLeader.
            PingableLeader localPingableLeader = invocation.getArgumentAt(0, PingableLeader.class);
            availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(aResponse()
                    .withStatus(200)
                    .withBody(("\"" + localPingableLeader.getUUID().toString() + "\"").getBytes())));
            return null;
        }).when(environment).register(isA(PingableLeader.class));
        setupLeaderBlockInConfig();
    }

    private void setUpForRemoteServices() throws IOException {
        availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(aResponse().withStatus(404)));
        setupLeaderBlockInConfig();
    }

    private void setupLeaderBlockInConfig() throws IOException {
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
                () -> runtimeConfig.timestampClient(),
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
        TransactionManagers.LockAndTimestampServices lockAndTimestampServices =
                TransactionManagers.createLockAndTimestampServices(
                        config,
                        () -> ImmutableTimestampClientConfig.of(false),
                        environment,
                        LockServiceImpl::create,
                        InMemoryTimestampService::new,
                        invalidator,
                        USER_AGENT);
        lockAndTimestampServices.timelock().getFreshTimestamp();
        lockAndTimestampServices.timelock().currentTimeMillis();

        availableServer.verify(postRequestedFor(urlMatching(timestampPath))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(postRequestedFor(urlMatching(lockPath))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
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
                atlasDbRuntimeConfig::timestampClient,
                environment,
                LockServiceImpl::create,
                InMemoryTimestampService::new,
                invalidator,
                USER_AGENT);
    }
}
