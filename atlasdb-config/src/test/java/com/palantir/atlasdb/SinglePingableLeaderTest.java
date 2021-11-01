/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.paxos.SingleLeaderPinger;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SinglePingableLeaderTest {
    private static final UUID LOCAL_UUID = UUID.randomUUID();
    private static final UUID REMOTE_UUID = UUID.randomUUID();
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of("user-agent", "0.27.1"));
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static final String PING = "/leader/ping";
    private static final String PING_V2 = "/leader/pingV2";
    private static final String UUID_PATH = "/leader/uuid";

    private static final MappingBuilder PING_MAPPING = WireMock.get(WireMock.urlEqualTo(PING));
    private static final MappingBuilder PING_V2_MAPPING = WireMock.get(WireMock.urlEqualTo(PING_V2));
    private static final MappingBuilder UUID_MAPPING = WireMock.get(WireMock.urlEqualTo(UUID_PATH));

    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));

    private int availablePort;
    private String serverUri;

    @Rule
    public WireMockRule availableServer =
            new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Before
    public void setup() {
        availableServer.stubFor(UUID_MAPPING.willReturn(aResponse()
                .withStatus(200)
                .withBody(("\"" + REMOTE_UUID.toString() + "\"").getBytes(StandardCharsets.UTF_8))));

        availableServer.stubFor(PING_MAPPING.willReturn(WireMock.aResponse().withStatus(204)));

        availablePort = availableServer.port();
        serverUri = String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, availablePort);
    }

    @Test
    public void doesNotFallbackOnPingWhenPingV2Responds() {
        availableServer.stubFor(
                PING_V2_MAPPING.willReturn(WireMock.aResponse().withStatus(200).withBody("{\"isLeader\":false}")));
        getDefaultLeaderPinger().pingLeaderWithUuid(REMOTE_UUID);
        assertPingV2RequestsMadeAreExactly(1);
        assertLegacyPingRequestsMade(0);
    }

    @Test
    public void fallbackOnPingWhenPingV2DoesNotExist() {
        availableServer.stubFor(PING_V2_MAPPING.willReturn(WireMock.aResponse().withStatus(404)));
        LeaderPinger pinger = getDefaultLeaderPinger();
        pingMultipleTimes(pinger, 5);
        assertPingV2RequestsMadeAreLessThan(5);
        assertLegacyPingRequestsMade(5);
    }

    @Test
    public void fallbackOnPingWhenPingV2ThrowsEvenWhenPingV2Exists() {
        availableServer.stubFor(PING_V2_MAPPING.willReturn(WireMock.aResponse().withStatus(500)));
        LeaderPinger pinger = getDefaultLeaderPinger();
        pingMultipleTimes(pinger, 5);
        assertPingV2RequestsMadeAreLessThan(5);
        assertLegacyPingRequestsMade(5);
    }

    @Test
    public void samplesRequestsToFailingPing2() {
        availableServer.stubFor(PING_V2_MAPPING.willReturn(WireMock.aResponse().withStatus(500)));
        LeaderPinger pinger = getDefaultLeaderPinger();
        pingMultipleTimes(pinger, 100);
        assertPingV2RequestsMadeAreLessThan(100);
        assertLegacyPingRequestsMade(100);
    }

    private void assertPingV2RequestsMadeAreLessThan(int count) {
        List<LoggedRequest> requests = WireMock.findAll(WireMock.getRequestedFor(WireMock.urlMatching(PING_V2)));
        assertThat(requests).hasSizeLessThan(count);
    }

    private void assertPingV2RequestsMadeAreExactly(int count) {
        List<LoggedRequest> requests = WireMock.findAll(WireMock.getRequestedFor(WireMock.urlMatching(PING_V2)));
        assertThat(requests).hasSize(count);
    }

    private void assertLegacyPingRequestsMade(int count) {
        List<LoggedRequest> requests = WireMock.findAll(WireMock.getRequestedFor(WireMock.urlMatching(PING)));
        assertThat(requests).hasSize(count);
    }

    private void pingMultipleTimes(LeaderPinger pinger, int pingFrequency) {
        IntStream.range(0, pingFrequency).forEach(_idx -> pinger.pingLeaderWithUuid(REMOTE_UUID));
    }

    private LeaderPinger getDefaultLeaderPinger() {
        return pingerWithVersion(OrderableSlsVersion.valueOf("1.1.1"));
    }

    private LeaderPinger pingerWithVersion(OrderableSlsVersion version) {
        List<LeaderPingerContext<PingableLeader>> otherLeaders = Leaders.generatePingables(
                ImmutableList.of(serverUri),
                () -> RemotingClientConfigs.DEFAULT,
                ServiceCreator.createTrustContext(Optional.of(SSL_CONFIGURATION)),
                USER_AGENT);
        return new SingleLeaderPinger(
                ImmutableMap.of(otherLeaders.get(0), new CheckedRejectionExecutorService(executorService)),
                Duration.ofSeconds(5),
                LOCAL_UUID,
                true,
                Optional.of(version));
    }
}
