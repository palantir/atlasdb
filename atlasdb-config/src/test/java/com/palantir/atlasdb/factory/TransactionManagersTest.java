/**
 * Copyright 2016 Palantir Technologies
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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;

public class TransactionManagersTest {
    private static final String CLIENT = "testClient";
    private static final String SERVER_1 = "http://localhost:8080";
    private static final String SERVER_2 = "http://palantir.com:8080";
    private static final int AVAILABLE_PORT = 8080;
    private static final String USER_AGENT = "user-agent (3.14159265)";
    private static final String USER_AGENT_HEADER = "User-Agent";

    private static final String TIMESTAMP_PATH = "/timestamp/fresh-timestamp";
    private static final MappingBuilder TIMESTAMP_MAPPING = post(urlEqualTo(TIMESTAMP_PATH));
    private static final String LOCK_PATH = "/lock/current-time-millis";
    private static final MappingBuilder LOCK_MAPPING = post(urlEqualTo(LOCK_PATH));

    private static final String TIMELOCK_TIMESTAMP_PATH = "/" + CLIENT + TIMESTAMP_PATH;
    private static final MappingBuilder TIMELOCK_TIMESTAMP_MAPPING = post(urlEqualTo(TIMELOCK_TIMESTAMP_PATH));
    private static final String TIMELOCK_LOCK_PATH = "/" + CLIENT + LOCK_PATH;
    private static final MappingBuilder TIMELOCK_LOCK_MAPPING = post(urlEqualTo(TIMELOCK_LOCK_PATH));

    private static final TimeLockClientConfig MOCK_CLIENT_CONFIG
            = getTimelockConfigForServers(ImmutableList.of(getUriForPort(AVAILABLE_PORT)));
    private static final TimeLockClientConfig MULTIPLE_SERVER_CONFIG
            = getTimelockConfigForServers(ImmutableList.of(SERVER_1, SERVER_2));

    private static final ServerListConfig RAW_REMOTE_SERVICE_CONFIG = ImmutableServerListConfig.builder()
            .addServers(getUriForPort(AVAILABLE_PORT))
            .build();

    private AtlasDbConfig config;
    private TransactionManagers.Environment environment;

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public WireMockRule availableServer = new WireMockRule(AVAILABLE_PORT);

    @Before
    public void setup() {
        availableServer.stubFor(TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("1")));
        availableServer.stubFor(LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("2")));
        availableServer.stubFor(TIMELOCK_TIMESTAMP_MAPPING.willReturn(aResponse().withStatus(200).withBody("3")));
        availableServer.stubFor(TIMELOCK_LOCK_MAPPING.willReturn(aResponse().withStatus(200).withBody("4")));

        config = mock(AtlasDbConfig.class);
        when(config.leader()).thenReturn(Optional.absent());
        when(config.timestamp()).thenReturn(Optional.absent());
        when(config.lock()).thenReturn(Optional.absent());
        when(config.timelock()).thenReturn(Optional.absent());

        environment = mock(TransactionManagers.Environment.class);
    }

    @Test
    public void userAgentsPresentOnRequestsToTimelockServer() {
        when(config.timelock()).thenReturn(Optional.of(MOCK_CLIENT_CONFIG));

        verifyUserAgentOnTimelockTimestampAndLockRequests();
    }

    @Test
    public void userAgentsPresentOnRequestsToRemoteTimestampAndLockServices() {
        when(config.timestamp()).thenReturn(Optional.of(RAW_REMOTE_SERVICE_CONFIG));
        when(config.lock()).thenReturn(Optional.of(RAW_REMOTE_SERVICE_CONFIG));

        verifyUserAgentOnRawTimestampAndLockRequests();
    }

    @Test
    public void userAgentsPresentOnRequestsWithLeaderBlockConfigured() throws IOException {
        when(config.leader()).thenReturn(Optional.of(ImmutableLeaderConfig.builder()
                .localServer(getUriForPort(AVAILABLE_PORT))
                .addLeaders(getUriForPort(AVAILABLE_PORT))
                .acceptorLogDir(temporaryFolder.newFolder())
                .learnerLogDir(temporaryFolder.newFolder())
                .quorumSize(1)
                .build()));

        verifyUserAgentOnRawTimestampAndLockRequests();
    }

    @Test
    public void canGetNamespacedConfigsFromTimelockBlock() {
        ServerListConfig namespacedConfig = TransactionManagers.getNamespacedServerListConfig(MULTIPLE_SERVER_CONFIG);
        assertThat(namespacedConfig.servers(), hasItems(SERVER_1 + "/" + CLIENT, SERVER_2 + "/" + CLIENT));
    }

    private void verifyUserAgentOnRawTimestampAndLockRequests() {
        verifyUserAgentOnTimestampAndLockRequests(TIMESTAMP_PATH, LOCK_PATH);
    }

    private void verifyUserAgentOnTimelockTimestampAndLockRequests() {
        verifyUserAgentOnTimestampAndLockRequests(TIMELOCK_TIMESTAMP_PATH, TIMELOCK_LOCK_PATH);
    }

    private void verifyUserAgentOnTimestampAndLockRequests(String timestampPath, String lockPath) {
        TransactionManagers.LockAndTimestampServices lockAndTimestampServices =
                TransactionManagers.createLockAndTimestampServices(
                        config,
                        environment,
                        LockServiceImpl::create,
                        InMemoryTimestampService::new,
                        USER_AGENT);
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

    private static ImmutableTimeLockClientConfig getTimelockConfigForServers(List<String> servers) {
        return ImmutableTimeLockClientConfig.builder()
                .client(CLIENT)
                .serversList(ImmutableServerListConfig.builder()
                        .addAllServers(servers)
                        .build())
                .build();
    }
}
