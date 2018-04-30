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

package com.palantir.atlasdb.timelock.benchmarks.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.dropwizard.Configuration;

public class TimelockBenchmarkServerConfig extends Configuration {

    private final TimeLockInstallConfiguration install;
    private final TimeLockRuntimeConfiguration runtime;

    public TimelockBenchmarkServerConfig(
            @JsonProperty(value = "install", required = true) TimeLockInstallConfiguration install,
            @JsonProperty(value = "runtime", required = true) TimeLockRuntimeConfiguration runtime) {
        this.install = install;
        this.runtime = runtime;
    }

    public TimeLockInstallConfiguration install() {
        return install;
    }

    public TimeLockRuntimeConfiguration runtime() {
        return runtime;
    }

}
