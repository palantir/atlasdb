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

package com.palantir.atlasdb.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MediaType;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.net.HttpHeaders;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.v2.ClientOptionsConstants;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;

public class AtlasDbDialogueServiceProviderTest {
    private static final SslConfiguration SSL_CONFIGURATION
            = SslConfiguration.of(Paths.get("var/security/trustStore.jks"));

    private static final String CLIENT = "tom";
    private static final String TIMESTAMP_PATH = "/tl/ts/" + CLIENT;
    private static final MappingBuilder TIMESTAMP_MAPPING = post(urlEqualTo(TIMESTAMP_PATH));
    private static final String LOCK_PATH = "/tl/l/" + CLIENT;
    private static final MappingBuilder LOCK_MAPPING = post(urlEqualTo(LOCK_PATH));

    private static final DialogueClients.ReloadingFactory DIALOGUE_BASE_FACTORY
            = DialogueClients.create(Refreshable.only(ServicesConfigBlock.builder().build()));
    private static final UserAgent USER_USER_AGENT = UserAgent.of(UserAgent.Agent.of("jeremy", "77.79.12"));

    private int serverPort;
    private ConjureTimelockService conjureTimelockService;

    @Rule
    public WireMockRule server = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Before
    public void setup() {
        setupServerToGiveOutTimestamps();

        serverPort = server.port();
        ServerListConfig serverListConfig = ImmutableServerListConfig.builder()
                .addServers(getUriForPort(serverPort))
                .sslConfiguration(SSL_CONFIGURATION)
                .build();

        AtlasDbDialogueServiceProvider provider = AtlasDbDialogueServiceProvider.create(
                Refreshable.only(serverListConfig),
                DIALOGUE_BASE_FACTORY,
                USER_USER_AGENT,
                MetricsManagers.createForTests().getTaggedRegistry());
        conjureTimelockService = provider.getConjureTimelockService();
    }


    @Test
    public void canMakeRequestsThroughDialogue() {
        ConjureGetFreshTimestampsResponse response = makeTimestampsRequest();
        assertThat(response.getInclusiveLower()).isEqualTo(58);
        assertThat(response.getInclusiveUpper()).isEqualTo(70);
    }

    @Test
    public void requestsAreIdentifiedWithTheUserProvidedUserAgent() {
        makeTimestampsRequest();

        server.verify(postRequestedFor(urlMatching(TIMESTAMP_PATH))
                .withHeader(HttpHeaders.USER_AGENT, WireMock.containing(
                        String.format("%s/%s",
                                USER_USER_AGENT.primary().name(),
                                USER_USER_AGENT.primary().version()))));
    }

    @Test
    public void atlasDbHttpClientVersionProvidedAsAnInformationalAgent() {
        makeTimestampsRequest();

        server.verify(postRequestedFor(urlMatching(TIMESTAMP_PATH))
                .withHeader(HttpHeaders.USER_AGENT, WireMock.containing(
                        String.format("%s/%s",
                                AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT,
                                AtlasDbRemotingConstants.CURRENT_CLIENT_PROTOCOL_VERSION.getProtocolVersionString()))));
    }

    @Test
    public void resilientToRepeatedRedirects() {
        server.stubFor(TIMESTAMP_MAPPING.willReturn(aResponse()
                .withStatus(308)
                .withHeader(HttpHeaders.LOCATION, getUriForPort(serverPort))));

        Instant start = Instant.now();
        ExecutorService ex = PTExecutors.newSingleThreadExecutor(true);
        ex.submit(this::scheduleServerRecoveryAfterFiveSeconds);

        assertThatCode(this::makeTimestampsRequest).doesNotThrowAnyException();
        assertThat(Instant.now())
                .as("should recover in a second after things are good again")
                .isBefore(start.plus(Duration.ofSeconds(6)));
        ex.shutdown();
    }

    @Test
    public void lockRequestsBlockingLongerThanShortReadTimeoutAllowed() {
        int lockBlockingMillis = Ints.checkedCast(
                ClientOptionsConstants.SHORT_READ_TIMEOUT.toJavaDuration().plusSeconds(1).toMillis());
        server.stubFor(LOCK_MAPPING.willReturn(aResponse()
                .withFixedDelay(lockBlockingMillis)
                .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .withBody("{\"type\":\"unsuccessful\",\"unsuccessful\":{}}")));
        ConjureLockResponse lockResponse = conjureTimelockService.lock(
                AuthHeader.valueOf("Bearer unused"),
                CLIENT,
                ConjureLockRequest.builder()
                        .acquireTimeoutMs(0) // Doesn't really matter: point is server takes a long time to return
                        .clientDescription("I am a client")
                        .lockDescriptors(ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("lock/lock"))))
                        .requestId(UUID.randomUUID())
                        .build());
        assertThat(lockResponse).isEqualTo(ConjureLockResponse.unsuccessful(UnsuccessfulLockResponse.of()));
    }

    private void scheduleServerRecoveryAfterFiveSeconds() {
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        setupServerToGiveOutTimestamps();
    }

    private void setupServerToGiveOutTimestamps() {
        server.stubFor(TIMESTAMP_MAPPING.willReturn(aResponse()
                .withStatus(200)
                .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .withBody("{\"inclusiveLower\": 58, \"inclusiveUpper\": 70}")));
    }

    private ConjureGetFreshTimestampsResponse makeTimestampsRequest() {
        return conjureTimelockService.getFreshTimestamps(
                AuthHeader.valueOf("Bearer unused"), CLIENT, ConjureGetFreshTimestampsRequest.of(96));
    }

    private static String getUriForPort(int port) {
        return String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }
}
