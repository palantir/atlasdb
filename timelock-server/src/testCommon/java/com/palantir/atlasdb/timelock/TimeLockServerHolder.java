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
package com.palantir.atlasdb.timelock;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.logsafe.Preconditions;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.DropwizardTestSupport;

public class TimeLockServerHolder extends ExternalResource {

    static final String ALL_NAMESPACES = "/[a-zA-Z0-9_-]+/.*";

    static {
        Http2Agent.install();
    }

    static final UserAgent WIREMOCK_USER_AGENT = UserAgent.of(UserAgent.Agent.of("wiremock", "1.1.1"));

    private static final WireMockConfiguration WIRE_MOCK_CONFIGURATION = WireMockConfiguration.wireMockConfig()
            .dynamicPort()
            .dynamicHttpsPort()
            .keystorePath("var/security/keyStore.jks")
            .keystorePassword("keystore")
            .trustStorePath("var/security/keyStore.jks")
            .trustStorePassword("keystore");

    private final Supplier<String> configFilePathSupplier;

    private final WireMockServer wireMockServer = new WireMockServer(WIRE_MOCK_CONFIGURATION);
    private final WireMock wireMock = new WireMock(wireMockServer);
    private DropwizardTestSupport<CombinedTimeLockServerConfiguration> timelockServer;
    private boolean isRunning = false;
    private boolean initialised = false;
    private int timelockPort;

    TimeLockServerHolder(Supplier<String> configFilePathSupplier) {
        this.configFilePathSupplier = configFilePathSupplier;
    }

    @Override
    protected void before() throws Exception {
        if (isRunning) {
            return;
        }

        timelockPort = readTimelockPort();

        wireMockServer.start();

        timelockServer = new DropwizardTestSupport<>(TimeLockServerLauncher.class, configFilePathSupplier.get());
        timelockServer.before();
        isRunning = true;
        initialised = true;


        StubMapping catchAll = any(urlMatching(ALL_NAMESPACES))
                .willReturn(aResponse().proxiedFrom(getTimelockUri())
                        .withAdditionalRequestHeader("User-Agent", UserAgents.format(WIREMOCK_USER_AGENT)))
                .atPriority(Integer.MAX_VALUE)
                .build();
        wireMock.register(catchAll);
    }

    @Override
    protected void after() {
        if (isRunning) {
            wireMockServer.stop();
            timelockServer.after();
            isRunning = false;
        }
    }

    public int getTimelockPort() {
        checkTimelockInitialised();
        return timelockPort;
    }

    public int getTimelockWiremockPort() {
        checkTimelockInitialised();
        return wireMockServer.httpsPort();
    }

    public WireMock wireMock() {
        return wireMock;
    }

    String getTimelockUri() {
        checkTimelockInitialised();
        // TODO(nziebart): hack
        return "https://localhost:" + timelockPort;
    }

    public TaggedMetricRegistry getTaggedMetricsRegistry() {
        return ((TimeLockServerLauncher) timelockServer.getApplication()).taggedMetricRegistry();
    }

    private void checkTimelockInitialised() {
        Preconditions.checkState(initialised, "timelock server isn't running yet, bad initialisation?");
    }

    synchronized ListenableFuture<Void> kill() {
        if (!isRunning) {
            return Futures.immediateFailedFuture(new RuntimeException("timelock hasn't started"));
        }
        after();
        return getShutdownFuture();
    }

    private ListenableFuture<Void> getShutdownFuture() {
        return ((TimeLockServerLauncher) timelockServer.getApplication()).shutdownFuture();
    }

    synchronized void start() {
        try {
            before();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int readTimelockPort() throws IOException {
        return new ObjectMapper(new YAMLFactory())
                .readTree(new File(configFilePathSupplier.get()))
                .get("server")
                .get("applicationConnectors")
                .get(0)
                .get("port")
                .intValue();
    }

    TimeLockInstallConfiguration installConfig() {
        checkTimelockInitialised();
        ObjectMapper mapper = Jackson.newObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(new File(configFilePathSupplier.get()), CombinedTimeLockServerConfiguration.class)
                    .install();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
