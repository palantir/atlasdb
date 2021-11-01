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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.net.SocketTimeoutException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class AtlasDbHttpClientsTest {

    private static final String LOCALHOST = "localhost";
    private static final String GET_ENDPOINT = "/number";
    private static final String POST_ENDPOINT = "/post";
    private static final MappingBuilder GET_MAPPING = get(urlEqualTo(GET_ENDPOINT));
    private static final MappingBuilder POST_MAPPING = post(urlEqualTo(POST_ENDPOINT));
    private static final int TEST_NUMBER_1 = 12;
    private static final int TEST_NUMBER_2 = 123;

    private static final String USER_AGENT_HEADER = "User-Agent";
    private static final String ATLASDB_HTTP_CLIENT = "atlasdb-http-client";
    private static final UserAgent.Agent ATLASDB_CLIENT_V2_AGENT = UserAgent.Agent.of(ATLASDB_HTTP_CLIENT, "2.0");
    private static final String ATLASDB_CLIENT_V2_AGENT_STRING =
            ATLASDB_HTTP_CLIENT + "/" + ATLASDB_CLIENT_V2_AGENT.version();

    private static final UserAgent BASE_USER_AGENT = UserAgent.of(UserAgent.Agent.of("bla", "0.1.2"));
    private static final AuxiliaryRemotingParameters AUXILIARY_REMOTING_PARAMETERS =
            AuxiliaryRemotingParameters.builder()
                    .shouldLimitPayload(false)
                    .userAgent(BASE_USER_AGENT)
                    .shouldRetry(true)
                    .remotingClientConfig(() -> RemotingClientConfigs.DEFAULT)
                    .build();

    private static final SslConfiguration SSL_CONFIG = SslConfiguration.of(
            Paths.get("var/security/trustStore.jks"), Paths.get("var/security/keyStore.jks"), "keystore");
    private static final Optional<TrustContext> TRUST_CONTEXT =
            Optional.of(SslSocketFactories.createTrustContext(SSL_CONFIG));
    private static final RetryOtherFirstResponseTransformer RETRY_OTHER_FIRST_RESPONSE_TRANSFORMER =
            new RetryOtherFirstResponseTransformer();
    private static final WireMockConfiguration WIRE_MOCK_CONFIGURATION = WireMockConfiguration.wireMockConfig()
            .dynamicPort()
            .dynamicHttpsPort()
            .keystorePath("var/security/keyStore.jks")
            .keystorePassword("keystore")
            .keyManagerPassword("keystore")
            .trustStorePath("var/security/keyStore.jks")
            .extensions(RETRY_OTHER_FIRST_RESPONSE_TRANSFORMER)
            .trustStorePassword("keystore");

    private int availablePort1;
    private int availablePort2;
    private int unavailablePort;

    @Rule
    public WireMockRule availableServer1 = new WireMockRule(WIRE_MOCK_CONFIGURATION);

    @Rule
    public WireMockRule availableServer2 = new WireMockRule(WIRE_MOCK_CONFIGURATION);

    @Rule
    public WireMockRule unavailableServer = new WireMockRule(WIRE_MOCK_CONFIGURATION);

    @Rule
    public WireMockRule proxyServer =
            new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

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

        availablePort1 = availableServer1.httpsPort();
        availablePort2 = availableServer2.httpsPort();
        unavailablePort = unavailableServer.httpsPort();
    }

    private static void setupAvailableServer(String testNumberAsString, WireMockRule availableServer) {
        availableServer.stubFor(
                GET_MAPPING.willReturn(aResponse().withStatus(200).withBody(testNumberAsString)));
        availableServer.stubFor(
                POST_MAPPING.willReturn(aResponse().withStatus(200).withBody(Boolean.toString(true))));
    }

    @Test
    public void ifOneServerResponds503WithNoRetryHeaderTheRequestIsRerouted() {
        RETRY_OTHER_FIRST_RESPONSE_TRANSFORMER.registerUrl(getUriForPort(availablePort1));
        RETRY_OTHER_FIRST_RESPONSE_TRANSFORMER.registerUrl(getUriForPort(availablePort2));

        TestResource client = AtlasDbHttpClients.createProxyWithFailover(
                MetricsManagers.createForTests(),
                ImmutableServerListConfig.builder()
                        .addServers(getUriForPort(availablePort1))
                        .addServers(getUriForPort(availablePort2))
                        .sslConfiguration(SSL_CONFIG)
                        .build(),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS);
        int response = client.getTestNumber();

        RequestPattern request = getRequestedFor(urlMatching(GET_ENDPOINT)).build();
        assertThat(availableServer1.findRequestsMatching(request).getRequests()).hasSizeGreaterThanOrEqualTo(1);

        assertThat(availableServer2.findRequestsMatching(request).getRequests()).hasSizeGreaterThanOrEqualTo(1);

        assertThat(response).isIn(TEST_NUMBER_1, TEST_NUMBER_2);

        RETRY_OTHER_FIRST_RESPONSE_TRANSFORMER.reset();
    }

    @Test
    public void userAgentIsPresentOnClientRequests() {
        TestResource client = AtlasDbHttpClients.createProxy(
                TRUST_CONTEXT, getUriForPort(availablePort1), TestResource.class, AUXILIARY_REMOTING_PARAMETERS);
        client.getTestNumber();

        availableServer1.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(USER_AGENT_HEADER, WireMock.containing(ATLASDB_CLIENT_V2_AGENT_STRING)));
    }

    @Test
    public void directProxyIsConfigurableOnClientRequests() {
        TestResource clientWithDirectCall = AtlasDbHttpClients.createProxyWithFailover(
                MetricsManagers.createForTests(),
                ImmutableServerListConfig.builder()
                        .addServers(getUriForPort(availablePort1))
                        .sslConfiguration(SSL_CONFIG)
                        .proxyConfiguration(ProxyConfiguration.DIRECT)
                        .build(),
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS);
        clientWithDirectCall.getTestNumber();

        availableServer1.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(USER_AGENT_HEADER, WireMock.containing(ATLASDB_CLIENT_V2_AGENT_STRING)));
    }

    @Test
    public void canLiveReloadServersList() {
        unavailableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(503)));

        SettableRefreshable<ServerListConfig> serverListRefreshable =
                Refreshable.create(ImmutableServerListConfig.builder()
                        .addServers(getUriForPort(unavailablePort))
                        .sslConfiguration(SSL_CONFIG)
                        .build());
        TestResource client = AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                MetricsManagers.createForTests(),
                serverListRefreshable,
                TestResource.class,
                AUXILIARY_REMOTING_PARAMETERS);

        // actually a Feign RetryableException but that's not on our classpath
        assertThatThrownBy(client::getTestNumber).isInstanceOf(RuntimeException.class);

        serverListRefreshable.update(ImmutableServerListConfig.builder()
                .addServers(getUriForPort(unavailablePort))
                .addServers(getUriForPort(availablePort1))
                .sslConfiguration(SSL_CONFIG)
                .build());

        int response = client.getTestNumber();
        assertThat(response).isEqualTo(TEST_NUMBER_1);
        unavailableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void httpProxyCanBeCommissionedAndDecommissionedIfNodeAvailabilityChanges() {
        SettableRefreshable<ServerListConfig> config = Refreshable.create(ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort1))
                .sslConfiguration(SSL_CONFIG)
                .build());

        TestResource testResource = AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                MetricsManagers.createForTests(), config, TestResource.class, AUXILIARY_REMOTING_PARAMETERS);

        assertThat(testResource.getTestNumber()).isEqualTo(TEST_NUMBER_1);

        config.update(ImmutableServerListConfig.builder()
                .addServers(getUriForPort(availablePort2))
                .sslConfiguration(SSL_CONFIG)
                .build());
        assertThat(testResource.getTestNumber()).isEqualTo(TEST_NUMBER_2);
    }

    private static String getUriForPort(int port) {
        return String.format("https://%s:%s", LOCALHOST, port);
    }

    private static final class RetryOtherFirstResponseTransformer extends ResponseTransformer {

        private final List<String> urls = new ArrayList<>();

        private final AtomicBoolean firstRequestDone = new AtomicBoolean(false);

        @Override
        public Response transform(Request request, Response response, FileSource _files, Parameters _parameters) {
            if (urls.isEmpty()) {
                return response;
            }

            if (firstRequestDone.get()) {
                return response;
            }

            String urlToRedirectTo = urls.stream()
                    .filter(url -> !request.getAbsoluteUrl().startsWith(url))
                    .skip(ThreadLocalRandom.current().nextInt(urls.size() - 1))
                    .findFirst()
                    .get();

            firstRequestDone.set(true);

            return Response.response()
                    .status(308)
                    .headers(new HttpHeaders(HttpHeader.httpHeader("Location", urlToRedirectTo)))
                    .build();
        }

        @Override
        public String getName() {
            return "retry-other-first-response-transformer";
        }

        void registerUrl(String url) {
            this.urls.add(url);
        }

        void reset() {
            urls.clear();
        }
    }

    @Test
    public void testIsPossiblyOkHttpTimeoutBug() {
        assertThat(AtlasDbHttpClients.isPossiblyOkHttpTimeoutBug(
                        new RuntimeException(new UncheckedExecutionException(new SocketTimeoutException()))))
                .isTrue();
        assertThat(AtlasDbHttpClients.isPossiblyOkHttpTimeoutBug(new RuntimeException(new RuntimeException())))
                .isFalse();
    }
}
