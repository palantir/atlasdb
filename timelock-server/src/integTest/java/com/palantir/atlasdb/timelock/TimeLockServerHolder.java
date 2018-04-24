/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.remoting3.http2.Http2Agent;

import io.dropwizard.testing.DropwizardTestSupport;

public class TimeLockServerHolder extends ExternalResource {

    static {
        Http2Agent.install();
    }

    private Supplier<String> configFilePathSupplier;
    private DropwizardTestSupport<TimeLockServerConfiguration> timelockServer;
    private boolean isRunning = false;
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
    }

    @Override
    protected void after() {
        if (isRunning) {
            timelockServer.after();
            isRunning = false;
        }
    }

    public int getTimelockPort() {
        return timelockPort;
    }

    public String getTimelockUri() {
        // TODO(nziebart): hack
        return "https://localhost:" + timelockPort;
    }

    public synchronized void kill() {
        after();
    }

    public synchronized void start() {
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

    public int getAdminPort() {
        return timelockServer.getAdminPort();
    }
}
