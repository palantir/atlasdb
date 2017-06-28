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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
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
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ImmutableTimestampClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.TimeDuration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.RateLimitedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class TransactionManagersTest {
    private static final String CLIENT = "testClient";
    private static final String USER_AGENT = "user-agent (3.14159265)";
    private static final String USER_AGENT_HEADER = "User-Agent";
    private static final long EMBEDDED_BOUND = 3;

    private static final String TIMESTAMP_PATH = "/timestamp/fresh-timestamp";
    private static final MappingBuilder TIMESTAMP_MAPPING = post(urlEqualTo(TIMESTAMP_PATH));
    private static final String LOCK_PATH = "/lock/current-time-millis";
    private static final MappingBuilder LOCK_MAPPING = post(urlEqualTo(LOCK_PATH));

    private static final String TIMELOCK_TIMESTAMP_PATH = "/" + CLIENT + TIMESTAMP_PATH;
    private static final MappingBuilder TIMELOCK_TIMESTAMP_MAPPING = post(urlEqualTo(TIMELOCK_TIMESTAMP_PATH));
    private static final String TIMELOCK_LOCK_PATH = "/" + CLIENT + LOCK_PATH;
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
    private TransactionManagers.Environment environment;
    private TimestampStoreInvalidator invalidator;

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public WireMockRule availableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Before
    public void setup() {
        availableServer.stubFor(TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("1")));
        availableServer.stubFor(LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("2")));
        availableServer.stubFor(TIMELOCK_TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("3")));
        availableServer.stubFor(TIMELOCK_LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("4")));

        config = mock(AtlasDbConfig.class);
        when(config.leader()).thenReturn(Optional.empty());
        when(config.timestamp()).thenReturn(Optional.empty());
        when(config.lock()).thenReturn(Optional.empty());
        when(config.timelock()).thenReturn(Optional.empty());
        when(config.keyValueService()).thenReturn(new InMemoryAtlasDbConfig());
        when(config.timestampClient()).thenReturn(ImmutableTimestampClientConfig.builder().build());

        environment = mock(TransactionManagers.Environment.class);

        invalidator = mock(TimestampStoreInvalidator.class);
        when(invalidator.backupAndInvalidate()).thenReturn(EMBEDDED_BOUND);

        availablePort = availableServer.port();
        mockClientConfig = getTimelockConfigForServers(ImmutableList.of(getUriForPort(availablePort)));
        rawRemoteServerConfig = ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort))
                .build();
    }

    @Test
    public void userAgentsPresentOnRequestsToTimelockServer() {
        when(config.timelock()).thenReturn(Optional.of(mockClientConfig));

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
        when(config.leader()).thenReturn(Optional.of(ImmutableLeaderConfig.builder()
                .localServer(getUriForPort(availablePort))
                .addLeaders(getUriForPort(availablePort))
                .acceptorLogDir(temporaryFolder.newFolder())
                .learnerLogDir(temporaryFolder.newFolder())
                .quorumSize(1)
                .build()));

        verifyUserAgentOnRawTimestampAndLockRequests();
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
    public void createsRateLimitingTimestampServiceIfBatchingEnabled() {
        AtlasDbConfig configWithBatching = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .timestampClient(ImmutableTimestampClientConfig.builder()
                        .enableTimestampBatching(true)
                        .build())
                .build();

        TransactionManagers.LockAndTimestampServices lockAndTimestampServices =
                createLockAndTimestampServicesForConfig(configWithBatching);
        assertThat(lockAndTimestampServices.time()).isInstanceOf(RateLimitedTimestampService.class);
    }

    @Test
    public void doesNotCreateRateLimitingTimestampServiceIfBatchingDisabled() {
        AtlasDbConfig configWithoutBatching = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .timestampClient(ImmutableTimestampClientConfig.builder()
                        .enableTimestampBatching(false)
                        .build())
                .build();

        TransactionManagers.LockAndTimestampServices lockAndTimestampServices =
                createLockAndTimestampServicesForConfig(configWithoutBatching);
        assertThat(lockAndTimestampServices.time()).isNotInstanceOf(RateLimitedTimestampService.class);
    }

    private void verifyUserAgentOnRawTimestampAndLockRequests() {
        verifyUserAgentOnTimestampAndLockRequests(TIMESTAMP_PATH, LOCK_PATH);
    }

    private void verifyUserAgentOnTimelockTimestampAndLockRequests() {
        availableServer.stubFor(TIMELOCK_PING_MAPPING.willReturn(aResponse()
                .withStatus(200)
                .withBody(TimestampManagementService.PING_RESPONSE)
                .withHeader("Content-Type", "text/plain")));
        availableServer.stubFor(TIMELOCK_FF_MAPPING.willReturn(aResponse()
                .withStatus(204)));

        verifyUserAgentOnTimestampAndLockRequests(TIMELOCK_TIMESTAMP_PATH, TIMELOCK_LOCK_PATH);
        verify(invalidator, times(1)).backupAndInvalidate();
        availableServer.verify(getRequestedFor(urlEqualTo(TIMELOCK_PING_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
        availableServer.verify(postRequestedFor(urlEqualTo(TIMELOCK_FF_PATH))
                .withHeader(USER_AGENT_HEADER, WireMock.equalTo(USER_AGENT)));
    }

    private void verifyUserAgentOnTimestampAndLockRequests(String timestampPath, String lockPath) {
        TransactionManagers.LockAndTimestampServices lockAndTimestampServices =
                createLockAndTimestampServicesForConfig(config);
        lockAndTimestampServices.time().getFreshTimestamp();
        lockAndTimestampServices.lock().currentTimeMillis();

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
            AtlasDbConfig atlasDbConfig) {
        return TransactionManagers.createLockAndTimestampServices(
                atlasDbConfig,
                environment,
                LockServiceImpl::create,
                InMemoryTimestampService::new,
                invalidator,
                USER_AGENT);
    }
}
