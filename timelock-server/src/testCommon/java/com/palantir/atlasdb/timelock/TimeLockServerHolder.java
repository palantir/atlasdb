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

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.logsafe.Preconditions;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.DropwizardTestSupport;

public class TimeLockServerHolder extends ExternalResource {

    static {
        Http2Agent.install();
    }

    private final Supplier<String> configFilePathSupplier;
    private DropwizardTestSupport<CombinedTimeLockServerConfiguration> timelockServer;
    private boolean isRunning = false;
    private boolean portInitialised = false;
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

        timelockServer = new DropwizardTestSupport<>(TimeLockServerLauncher.class, configFilePathSupplier.get());
        timelockServer.before();
        isRunning = true;
        portInitialised = true;
    }

    @Override
    protected void after() {
        if (isRunning) {
            timelockServer.after();
            isRunning = false;
        }
    }

    public int getTimelockPort() {
        checkTimelockPortInitialised();
        return timelockPort;
    }

    String getTimelockUri() {
        checkTimelockPortInitialised();
        // TODO(nziebart): hack
        return "https://localhost:" + timelockPort;
    }

    public TaggedMetricRegistry getTaggedMetricsRegistry() {
        return ((TimeLockServerLauncher) timelockServer.getApplication()).taggedMetricRegistry();
    }

    private void checkTimelockPortInitialised() {
        Preconditions.checkState(portInitialised, "timelock server isn't running yet, bad initialisation?");
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
        checkTimelockPortInitialised();
        ObjectMapper mapper = Jackson.newObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(new File(configFilePathSupplier.get()), CombinedTimeLockServerConfiguration.class)
                    .install();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
