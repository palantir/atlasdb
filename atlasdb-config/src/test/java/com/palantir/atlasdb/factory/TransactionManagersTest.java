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

import static org.hamcrest.Matchers.is;
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
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.ws.rs.core.Response;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.qos.config.QosClientConfig;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.leader.PingableLeader;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class TransactionManagersTest {
    private static final String CLIENT = "testClient";
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT = USER_AGENT_NAME + " (3.14159265)";
    private static final String USER_AGENT_HEADER = "User-Agent";
    private static final long EMBEDDED_BOUND = 3;

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
    private static final String TIMELOCK_START_TRANSACTION_PATH
            = "/" + CLIENT + "/timelock/start-atlasdb-transaction";
    private static final MappingBuilder TIMELOCK_START_TRANSACTION_MAPPING
            = post(urlEqualTo(TIMELOCK_START_TRANSACTION_PATH));


    private static final String TIMELOCK_PING_PATH =  "/" + CLIENT + "/timestamp-management/ping";
    private static final MappingBuilder TIMELOCK_PING_MAPPING = get(urlEqualTo(TIMELOCK_PING_PATH));
    private static final String TIMELOCK_FF_PATH
            = "/" + CLIENT + "/timestamp-management/fast-forward?currentTimestamp=" + EMBEDDED_BOUND;
    private static final MappingBuilder TIMELOCK_FF_MAPPING = post(urlEqualTo(TIMELOCK_FF_PATH));

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    private int availablePort;
    private TimeLockClientConfig mockClientConfig;
    private ServerListConfig rawRemoteServerConfig;

    private AtlasDbConfig config;
    private AtlasDbRuntimeConfig runtimeConfig;
    private Consumer<Object> environment;
    private Consumer<Runnable> originalAsyncMethod;
    private Supplier<Optional<AtlasDbRuntimeConfig>> configSupplier;

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public WireMockRule availableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Before
    public void setup() throws JsonProcessingException {
        // Change code to run synchronously, but with a timeout in case something's gone horribly wrong
        originalAsyncMethod = TransactionManagers.runAsync;
        TransactionManagers.runAsync = task -> Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(task::run);

        availableServer.stubFor(LEADER_UUID_MAPPING.willReturn(aResponse().withStatus(200).withBody(
                ("\"" + UUID.randomUUID().toString() + "\"").getBytes())));
        availableServer.stubFor(TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("1")));
        availableServer.stubFor(LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("2")));

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
    public void canCreateInMemory() {
        TransactionManagers.createInMemory(GenericTestSchema.getSchema());
    }

    @Test
    public void canCreateInMemoryWithSetOfSchemas() {
        TransactionManagers.createInMemory(ImmutableSet.of(
                GenericTestSchema.getSchema()));
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

        MetricRegistry metrics = new MetricRegistry();
        TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent("test")
                .globalMetricsRegistry(metrics)
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .build()
                .serializable();
        assertThat(metrics.getNames().stream()
                .anyMatch(metricName -> metricName.contains(USER_AGENT_NAME)), is(false));
    }

    @Test
    public void timelockServiceStatusReturnsHealthyWithoutRequest() {
        TransactionManager tm = TransactionManagers.createInMemory(GenericTestSchema.getSchema());
        assertTrue(tm.getTimelockServiceStatus().isHealthy());
    }

    @Test
    public void timelockServiceStatusReturnsHealthyAfterSuccessfulRequests() {
        TransactionManager tm = TransactionManagers.createInMemory(GenericTestSchema.getSchema());
        tm.getUnreadableTimestamp();
        assertTrue(tm.getTimelockServiceStatus().isHealthy());
    }

    @Test
    public void timelockServiceCanStartTransactionsEvenWithoutStartTransactionEndpoint() {
        availableServer.stubFor(TIMELOCK_START_TRANSACTION_MAPPING.willReturn(
                aResponse().withStatus(Response.Status.NOT_FOUND.getStatusCode())));
        TransactionManager tm = TransactionManagers.createInMemory(GenericTestSchema.getSchema());

        tm.runTaskWithRetry(tx -> {
            RangeScanTestTable testTable = GenericTestSchemaTableFactory.of().getRangeScanTestTable(tx);
            testTable.putColumn1(RangeScanTestTable.RangeScanTestRow.of("foo"), 12345L);
            return null;
        });
    }

    private void setUpForLocalServices() throws IOException {
        doAnswer(invocation -> {
            // Configure our server to reply with the same server ID as the registered PingableLeader.
            PingableLeader localPingableLeader = invocation.getArgumentAt(0, PingableLeader.class);
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
}
