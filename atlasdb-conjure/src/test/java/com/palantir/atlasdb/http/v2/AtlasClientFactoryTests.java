/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.atlasdb.http.v2;

import static org.assertj.core.api.Assertions.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;
import com.palantir.refreshable.DefaultRefreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@RunWith(Parameterized.class)
public final class AtlasClientFactoryTests {
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of("Test", "1.0"));
    public static final SslConfiguration SSL_CONFIGURATION = SslConfiguration.of(
            Paths.get("../atlasdb-ete-tests/var/security/trustStore.jks"),
            Paths.get("../atlasdb-ete-tests/var/security/keyStore.jks"),
            "keystore");

    private WireMockServer wiremock = new WireMockServer(options()
            .httpDisabled(true)
            .dynamicHttpsPort()
            .keystorePath("../atlasdb-ete-tests/var/security/keyStore.jks")
            .keystorePassword("keystore")
            .trustStorePath("../atlasdb-ete-tests/var/security/trustStore.jks")
            .trustStorePassword("palantir"));

    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
    private final HostMetricsRegistry hostMetrics = new HostMetricsRegistry();
    private final JaxrsImplementation jaxrsImplementation;

    private AtlasClientFactory factory;
    private DefaultRefreshable<ServerListConfig> serverListConfig;
    private String wireMockUrl;

    public enum JaxrsImplementation {
        CJR,
        DIALOGUE
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {JaxrsImplementation.CJR}, {JaxrsImplementation.DIALOGUE} });
    }

    public AtlasClientFactoryTests(JaxrsImplementation jaxrsImplementation) {

        this.jaxrsImplementation = jaxrsImplementation;
    }

    @Before
    public void before() {
        wiremock.start();
        wiremock.stubFor(get(urlMatching("/string")).willReturn(okJson("\"body\"")));

        wireMockUrl = "https://localhost:" + wiremock.httpsPort();
        serverListConfig = new DefaultRefreshable<>(toServerList(wireMockUrl));
        factory = new AtlasClientFactory(
                serverListConfig,
                taggedMetricRegistry,
                USER_AGENT,
                hostMetrics,
                JaxrsImplementation.DIALOGUE == jaxrsImplementation);
    }

    @Test
    public void testCreateClient_hasConfig() {
        TestService client = factory.client(TestService.class);

        assertThat(client.string()).isEqualTo("body");
    }

    @Test
    public void testCreateClient_missingConfig() {
        updateUris();
        TestService client = factory.client(TestService.class);

        assertIsNotConfigured(client);
    }

    @Test
    public void testCreateClient_configAddedAndRemoved() {
        TestService client = factory.client(TestService.class);
        assertThat(client.string()).isEqualTo("body");

        updateUris();
        assertIsNotConfigured(client);

        updateUris(wireMockUrl);
        assertThat(client.string()).isEqualTo("body");
    }

    @Test
    public void passing_in_a_dialogue_interface_returns_a_real_dialogue_client() {
        ConjureTimelockServiceBlocking client = factory.client(ConjureTimelockServiceBlocking.class);
        assertThat(client).isNotNull();
    }

    private static void assertIsNotConfigured(TestService testService) {
        assertThatLoggableExceptionThrownBy(() -> testService.string())
                .hasLogMessage("Service not configured");
    }

    private void updateUris(String... uris) {
        serverListConfig.update(toServerList(uris));
    }

    private ServerListConfig toServerList(String... uris) {
        return ImmutableServerListConfig.builder()
                .addServers(uris)
                .sslConfiguration(SSL_CONFIGURATION)
                .build();
    }
}
