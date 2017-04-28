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
package com.palantir.atlasdb.http;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import java.util.Set;

import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.GET;
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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class AtlasDbHttpClientsTest {
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final String TEST_ENDPOINT = "/number";
    private static final MappingBuilder ENDPOINT_MAPPING = get(urlEqualTo(TEST_ENDPOINT));
    private static final int TEST_NUMBER = 12;

    private int availablePort;
    private int unavailablePort;
    private Set<String> bothUris;

    @Rule
    public WireMockRule availableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public WireMockRule unavailableServer = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    public interface TestResource {
        @GET
        @Path(TEST_ENDPOINT)
        @Produces(MediaType.APPLICATION_JSON)
        int getTestNumber();
    }

    @Before
    public void setup() {
        String testNumberAsString = Integer.toString(TEST_NUMBER);
        availableServer.stubFor(ENDPOINT_MAPPING.willReturn(aResponse().withStatus(200).withBody(testNumberAsString)));

        availablePort = availableServer.port();
        unavailablePort = unavailableServer.port();
        bothUris = ImmutableSet.of(
                getUriForPort(unavailablePort),
                getUriForPort(availablePort));
    }

    @Test
    public void ifOneServerResponds503WithNoRetryHeaderTheRequestIsRerouted() {
        unavailableServer.stubFor(ENDPOINT_MAPPING.willReturn(aResponse().withStatus(503)));

        TestResource client = AtlasDbHttpClients.createProxyWithFailover(NO_SSL, bothUris, TestResource.class);
        int response = client.getTestNumber();

        assertThat(response, equalTo(TEST_NUMBER));
        unavailableServer.verify(getRequestedFor(urlMatching(TEST_ENDPOINT)));
    }

    @Test
    public void userAgentIsPresentOnClientRequests() {
        TestResource client =
                AtlasDbHttpClients.createProxy(NO_SSL, getUriForPort(availablePort), TestResource.class);
        client.getTestNumber();

        String defaultUserAgent = UserAgents.fromStrings(UserAgents.DEFAULT_VALUE, UserAgents.DEFAULT_VALUE);
        availableServer.verify(getRequestedFor(urlMatching(TEST_ENDPOINT))
                .withHeader(FeignOkHttpClients.USER_AGENT_HEADER, WireMock.equalTo(defaultUserAgent)));
    }

    private static String getUriForPort(int port) {
        return String.format("http://%s:%s", WireMockConfiguration.DEFAULT_BIND_ADDRESS, port);
    }
}
