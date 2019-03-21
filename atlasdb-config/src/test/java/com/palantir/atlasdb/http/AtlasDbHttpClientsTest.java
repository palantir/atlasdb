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

import java.net.ProxySelector;
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

import com.codahale.metrics.MetricRegistry;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;

public class AtlasDbHttpClientsTest {
    private static final Optional<TrustContext> NO_SSL = Optional.empty();
    private static final String GET_ENDPOINT = "/number";
    private static final String POST_ENDPOINT = "/post";
    private static final MappingBuilder GET_MAPPING = get(urlEqualTo(GET_ENDPOINT));
    private static final MappingBuilder POST_MAPPING = post(urlEqualTo(POST_ENDPOINT));
    private static final int TEST_NUMBER = 12;

    private int availablePort;
    private int unavailablePort;
    private int proxyPort;
    private Set<String> bothUris;

    @Rule
    public WireMockRule availableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

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
        String testNumberAsString = Integer.toString(TEST_NUMBER);
        availableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(200).withBody(testNumberAsString)));
        availableServer.stubFor(POST_MAPPING.willReturn(aResponse().withStatus(200).withBody(Boolean.toString(true))));
        proxyServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(200).withBody(testNumberAsString)));

        availablePort = availableServer.port();
        unavailablePort = unavailableServer.port();
        proxyPort = proxyServer.port();

        bothUris = ImmutableSet.of(
                getUriForPort(unavailablePort),
                getUriForPort(availablePort));
    }

    @Test
    public void payloadLimitingClientThrowsOnRequestThatIsTooLarge() {
        TestResource client = AtlasDbHttpClients.createProxy(new MetricRegistry(), NO_SSL, getUriForPort(availablePort),
                TestResource.class, UserAgents.DEFAULT_USER_AGENT, true);
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
        TestResource client = AtlasDbHttpClients.createProxy(new MetricRegistry(), NO_SSL, getUriForPort(availablePort),
                TestResource.class);
        assertThat(client.postRequest(new byte[AtlasDbInterceptors.MAX_PAYLOAD_SIZE]))
                .as("Request with payload size exceeding limit succeeds when not limiting payload size")
                .isTrue();
    }

    @Test
    public void ifOneServerResponds503WithNoRetryHeaderTheRequestIsRerouted() {
        unavailableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(503)));

        TestResource client = AtlasDbHttpClients.createProxyWithFailover(new MetricRegistry(),
                NO_SSL,
                Optional.empty(), bothUris, TestResource.class);
        int response = client.getTestNumber();

        assertThat(response, equalTo(TEST_NUMBER));
        unavailableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void userAgentIsPresentOnClientRequests() {
        TestResource client =
                AtlasDbHttpClients.createProxy(
                        new MetricRegistry(), NO_SSL, getUriForPort(availablePort), TestResource.class);
        client.getTestNumber();

        String defaultUserAgent = UserAgents.fromStrings(UserAgents.DEFAULT_VALUE, UserAgents.DEFAULT_VALUE);
        availableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER, WireMock.equalTo(defaultUserAgent)));
    }

    @Test
    public void directProxyIsConfigurableOnClientRequests() {
        Optional<ProxySelector> directProxySelector = Optional.of(
                ServiceCreator.createProxySelector(ProxyConfiguration.DIRECT));
        TestResource clientWithDirectCall = AtlasDbHttpClients.createProxyWithFailover(
                new MetricRegistry(), NO_SSL,
                directProxySelector, ImmutableSet.of(getUriForPort(availablePort)), TestResource.class);
        clientWithDirectCall.getTestNumber();
        String defaultUserAgent = UserAgents.fromStrings(UserAgents.DEFAULT_VALUE, UserAgents.DEFAULT_VALUE);

        availableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER, WireMock.equalTo(defaultUserAgent)));
    }

    @Test
    public void httpProxyIsConfigurableOnClientRequests() {
        Optional<ProxySelector> httpProxySelector = Optional.of(
                ServiceCreator.createProxySelector(ProxyConfiguration.of(getHostAndPort(proxyPort))));
        TestResource clientWithHttpProxy = AtlasDbHttpClients.createProxyWithFailover(
                new MetricRegistry(),
                NO_SSL,
                httpProxySelector, ImmutableSet.of(getUriForPort(availablePort)), TestResource.class);
        clientWithHttpProxy.getTestNumber();
        String defaultUserAgent = UserAgents.fromStrings(UserAgents.DEFAULT_VALUE, UserAgents.DEFAULT_VALUE);

        proxyServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT))
                .withHeader(AtlasDbInterceptors.USER_AGENT_HEADER, WireMock.equalTo(defaultUserAgent)));
        availableServer.verify(0, getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void canLiveReloadServersList() {
        unavailableServer.stubFor(GET_MAPPING.willReturn(aResponse().withStatus(503)));

        List<String> servers = Lists.newArrayList(getUriForPort(unavailablePort));

        TestResource client = AtlasDbHttpClients.createLiveReloadingProxyWithQuickFailoverForTesting(
                new MetricRegistry(),
                () -> ImmutableServerListConfig.builder()
                        .servers(servers)
                        .build(),
                SslSocketFactories::createTrustContext,
                unused -> ProxySelector.getDefault(),
                TestResource.class,
                "user (123)");

        // actually a Feign RetryableException but that's not on our classpath
        assertThatThrownBy(client::getTestNumber).isInstanceOf(RuntimeException.class);

        servers.add(getUriForPort(availablePort));
        Uninterruptibles.sleepUninterruptibly(
                PollingRefreshable.DEFAULT_REFRESH_INTERVAL.getSeconds() + 1, TimeUnit.SECONDS);

        int response = client.getTestNumber();
        assertThat(response, equalTo(TEST_NUMBER));
        unavailableServer.verify(getRequestedFor(urlMatching(GET_ENDPOINT)));
    }

    @Test
    public void httpProxyThrowsServiceNotAvailableExceptionIfConfiguredWithZeroNodes() {
        TestResource testResource = AtlasDbHttpClients.createLiveReloadingProxyWithQuickFailoverForTesting(
                new MetricRegistry(),
                () -> ImmutableServerListConfig.builder().build(),
                SslSocketFactories::createTrustContext,
                proxyConfiguration -> ProxySelector.getDefault(),
                TestResource.class,
                UserAgents.DEFAULT_VALUE);

        assertThatThrownBy(testResource::getTestNumber).isInstanceOf(ServiceNotAvailableException.class);
    }

    @Test
    public void httpProxyCanBeCommissionedAndDecommissionedIfNodeAvailabilityChanges() {
        AtomicReference<ServerListConfig> config = new AtomicReference<>(ImmutableServerListConfig.builder().build());

        TestResource testResource = AtlasDbHttpClients.createLiveReloadingProxyWithQuickFailoverForTesting(
                new MetricRegistry(),
                config::get,
                SslSocketFactories::createTrustContext,
                proxyConfiguration -> ProxySelector.getDefault(),
                TestResource.class,
                UserAgents.DEFAULT_VALUE);

        // At this point, there are zero nodes in the config, so we should get ServiceNotAvailable.
        assertThatThrownBy(testResource::getTestNumber).isInstanceOf(ServiceNotAvailableException.class);

        config.set(ImmutableServerListConfig.builder().addServers(getUriForPort(availablePort)).build());
        Uninterruptibles.sleepUninterruptibly(
                PollingRefreshable.DEFAULT_REFRESH_INTERVAL.getSeconds() + 1, TimeUnit.SECONDS);
        assertThat(testResource.getTestNumber(), equalTo(TEST_NUMBER));

        config.set(ImmutableServerListConfig.builder().build());
        Uninterruptibles.sleepUninterruptibly(
                PollingRefreshable.DEFAULT_REFRESH_INTERVAL.getSeconds() + 1, TimeUnit.SECONDS);
        assertThatThrownBy(testResource::getTestNumber).isInstanceOf(ServiceNotAvailableException.class);
    }

    private static String getUriForPort(int port) {
        return String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }

    private static String getHostAndPort(int port) {
        return String.format("%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }
}
