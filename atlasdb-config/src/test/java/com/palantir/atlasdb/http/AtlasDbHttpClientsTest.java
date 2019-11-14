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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableRemotingClientConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.TrustContext;

public class AtlasDbHttpClientsTest {
    private static final Optional<TrustContext> NO_SSL = Optional.empty();
    private static final String GET_ENDPOINT = "/number";
    private static final String POST_ENDPOINT = "/post";
    private static final MappingBuilder GET_MAPPING = get(urlEqualTo(GET_ENDPOINT));
    private static final MappingBuilder POST_MAPPING = post(urlEqualTo(POST_ENDPOINT));
    private static final int TEST_NUMBER_1 = 12;
    private static final int TEST_NUMBER_2 = 123;

    private static final String ATLASDB_HTTP_CLIENT = "atlasdb-http-client";
    private static final UserAgent.Agent ATLASDB_CLIENT_V1_AGENT = UserAgent.Agent.of(ATLASDB_HTTP_CLIENT, "1.0");
    private static final UserAgent.Agent ATLASDB_CLIENT_V2_AGENT = UserAgent.Agent.of(ATLASDB_HTTP_CLIENT, "2.0");
    private static final String ATLASDB_CLIENT_V2_AGENT_STRING
            = ATLASDB_HTTP_CLIENT + "/" + ATLASDB_CLIENT_V2_AGENT.version();

    private static final UserAgent BASE_USER_AGENT = UserAgent.of(UserAgent.Agent.of("bla", "0.1.2"));
    private static final UserAgent LEGACY_USER_AGENT = BASE_USER_AGENT.addAgent(ATLASDB_CLIENT_V1_AGENT);
    private static final String LEGACY_USER_AGENT_STRING = UserAgents.format(LEGACY_USER_AGENT);
    private static final AuxiliaryRemotingParameters AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT
            = AuxiliaryRemotingParameters.builder()
            .shouldLimitPayload(false)
            .userAgent(BASE_USER_AGENT)
            .shouldRetry(true)
            .build();
    private static final AuxiliaryRemotingParameters AUXILIARY_REMOTING_PARAMETERS_WITH_PAYLOAD_LIMIT
            = AuxiliaryRemotingParameters.builder()
            .shouldLimitPayload(true)
            .userAgent(BASE_USER_AGENT)
            .shouldRetry(true)
            .build();

    private static final SslConfiguration SSL_CONFIG = SslConfiguration.of(Paths.get("var/security/keyStore.jks"));

    private int availablePort1;
    private int availablePort2;
    private int unavailablePort;
    private int proxyPort;
    private Set<String> bothUris;

    @Rule
    public WireMockRule availableServer1 = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public WireMockRule availableServer2 = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public WireMockRule unavailableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public WireMockRule proxyServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    public interface TestResource {
        @GET
        @Path(GET_ENDPOINT)
        @Produces(MediaType.APPLICATION_JSON)
        int getTestNumber();

        @POST
        @Path(POST_ENDPOINT)
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        boolean postRequest(byte[] content);
    }

    @Before
    public void setup() {
        setupAvailableServer(Integer.toString(TEST_NUMBER_1), availableServer1);
        setupAvailableServer(Integer.toString(TEST_NUMBER_2), availableServer2);
        proxyServer.stubFor(
                GET_MAPPING.willReturn(aResponse().withStatus(200).withBody(Integer.toString(TEST_NUMBER_1))));

        availablePort1 = availableServer1.port();
        availablePort2 = availableServer2.port();
        unavailablePort = unavailableServer.port();
        proxyPort = proxyServer.port();

        bothUris = ImmutableSet.of(
                getUriForPort(unavailablePort),
                getUriForPort(availablePort1));
    }

    private void setupAvailableServer(String testNumberAsString, WireMockRule availableServer) {
        availableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(200).withBody(testNumberAsString)));
        availableServer.stubFor(POST_MAPPING.willReturn(aResponse().withStatus(200).withBody(Boolean.toString(true))));
    }

    @Test
    public void payloadLimitingClientThrowsOnRequestThatIsTooLarge() {
        TestResource client = AtlasDbHttpClients.createProxy(
                MetricsManagers.createForTests(),
                NO_SSL,
                getUriForPort(availablePort1),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_WITH_PAYLOAD_LIMIT);
        assertThat(client.postRequest(new byte[AtlasDbInterceptors.MAX_PAYLOAD_SIZE / 2]))
                .as("Request with payload size below limit succeeds")
                .isTrue();
        assertThatThrownBy(() -> client.postRequest(new byte[AtlasDbInterceptors.MAX_PAYLOAD_SIZE]))
                .as("Request with payload size exceeding limit throws")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Request too large");
    }

    @Test
    public void regularClientDoesNotThrowOnRequestThatIsTooLarge() {
        TestResource client
                = AtlasDbHttpClients.createProxy(
                MetricsManagers.createForTests(),
                NO_SSL,
                getUriForPort(availablePort1),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);
        assertThat(client.postRequest(new byte[AtlasDbInterceptors.MAX_PAYLOAD_SIZE]))
                .as("Request with payload size exceeding limit succeeds when not limiting payload size")
                .isTrue();
    }

    @Test
    public void ifOneServerResponds503WithNoRetryHeaderTheRequestIsRerouted() {
        unavailableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(503)));

        TestResource client = AtlasDbHttpClients.createProxyWithFailover(
                MetricsManagers.createForTests(),
                ImmutableServerListConfig.builder().addAllServers(bothUris).build(),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);
        int response = client.getTestNumber();

        assertThat(response, equalTo(TEST_NUMBER_1));
        unavailableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void userAgentIsPresentOnClientRequests() {
        TestResource client = AtlasDbHttpClients.createProxy(
                MetricsManagers.createForTests(),
                NO_SSL,
                getUriForPort(availablePort1),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);
        client.getTestNumber();

        availableServer1.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER, WireMock.equalTo(LEGACY_USER_AGENT_STRING)));
    }

    @Test
    public void directProxyIsConfigurableOnClientRequests() {
        TestResource clientWithDirectCall = AtlasDbHttpClients.createProxyWithFailover(
                MetricsManagers.createForTests(),
                ImmutableServerListConfig.builder()
                        .addServers(getUriForPort(availablePort1))
                        .proxyConfiguration(ProxyConfiguration.DIRECT)
                        .build(),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);
        clientWithDirectCall.getTestNumber();

        availableServer1.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER, WireMock.equalTo(LEGACY_USER_AGENT_STRING)));
    }

    @Test
    public void httpProxyIsConfigurableOnClientRequests() {
        TestResource clientWithHttpProxy = AtlasDbHttpClients.createProxyWithFailover(
                MetricsManagers.createForTests(),
                ImmutableServerListConfig.builder()
                        .addServers(getUriForPort(availablePort1))
                        .proxyConfiguration(ProxyConfiguration.of(getHostAndPort(proxyPort)))
                        .build(),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);
        clientWithHttpProxy.getTestNumber();

        proxyServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER, WireMock.equalTo(LEGACY_USER_AGENT_STRING)));
        availableServer1.verify(0, getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void canLiveReloadServersList() {
        unavailableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(503)));

        List<String> servers = Lists.newArrayList(getUriForPort(unavailablePort));

        TestResource client = AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                MetricsManagers.createForTests(),
                () -> ImmutableServerListConfig.builder()
                        .servers(servers)
                        .sslConfiguration(SSL_CONFIG)
                        .build(),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);

        // actually a Feign RetryableException but that's not on our classpath
        assertThatThrownBy(client::getTestNumber).isInstanceOf(RuntimeException.class);

        servers.add(getUriForPort(availablePort1));
        Uninterruptibles.sleepUninterruptibly(
                PollingRefreshable.DEFAULT_REFRESH_INTERVAL.getSeconds() + 1, TimeUnit.SECONDS);

        int response = client.getTestNumber();
        assertThat(response, equalTo(TEST_NUMBER_1));
        unavailableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void canConnectViaConjureJavaRuntime() {
        List<String> servers = Lists.newArrayList(getUriForPort(availablePort1));
        TestResource client = AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                MetricsManagers.createForTests(),
                () -> ImmutableServerListConfig.builder()
                        .servers(servers)
                        .sslConfiguration(SSL_CONFIG)
                        .build(),
                TestResource.class,
                AuxiliaryRemotingParameters.builder()
                        .shouldLimitPayload(false)
                        .shouldRetry(true)
                        .userAgent(BASE_USER_AGENT)
                        .remotingClientConfig(() -> ImmutableRemotingClientConfig.builder()
                                .maximumConjureRemotingProbability(1.0)
                                .build())
                        .build());
        int response = client.getTestNumber();
        assertThat(response, equalTo(TEST_NUMBER_1));
        availableServer1.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER,
                        WireMock.containing(ATLASDB_CLIENT_V2_AGENT_STRING)));
    }

    @Test
    public void httpProxyCanBeCommissionedAndDecommissionedIfNodeAvailabilityChanges() {
        AtomicReference<ServerListConfig> config = new AtomicReference<>(ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort1))
                .sslConfiguration(SSL_CONFIG)
                .build());

        TestResource testResource = AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                MetricsManagers.createForTests(),
                config::get,
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS_NO_PAYLOAD_LIMIT);

        assertThat(testResource.getTestNumber(), equalTo(TEST_NUMBER_1));

        config.set(ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort2))
                .sslConfiguration(SSL_CONFIG)
                .build());
        Uninterruptibles.sleepUninterruptibly(
                PollingRefreshable.DEFAULT_REFRESH_INTERVAL.getSeconds() + 1, TimeUnit.SECONDS);
        assertThat(testResource.getTestNumber(), equalTo(TEST_NUMBER_2));
    }

    private static String getUriForPort(int port) {
        return String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }

    private static String getHostAndPort(int port) {
        return String.format("%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }

}
