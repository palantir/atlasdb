/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.dropwizard.testing.DropwizardTestSupport;

public class TimeLockServerHolder extends ExternalResource {
    private Supplier<String> configFilePathSupplier;
    private DropwizardTestSupport<TimeLockServerConfiguration> timelockServer;

    TimeLockServerHolder(Supplier<String> configFilePathSupplier) {
        this.configFilePathSupplier = configFilePathSupplier;
    }

    @Override
    protected void before() throws Exception {
        timelockServer = new DropwizardTestSupport<>(TimeLockServerLauncher.class, configFilePathSupplier.get());
        timelockServer.before();
    }

    @Override
    protected void after() {
        timelockServer.after();
    }

    public int getTimelockPort() {
        return timelockServer.getLocalPort();
    }

    public int getAdminPort() {
        return timelockServer.getAdminPort();
    }
}
