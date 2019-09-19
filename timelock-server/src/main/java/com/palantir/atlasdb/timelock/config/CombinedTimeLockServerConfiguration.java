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
package com.palantir.atlasdb.timelock.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import io.dropwizard.Configuration;

public class CombinedTimeLockServerConfiguration extends Configuration {
    private final TimeLockInstallConfiguration install;
    private final TimeLockRuntimeConfiguration runtime;

    public CombinedTimeLockServerConfiguration(
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

    public static int threadPoolSize() {
        return 128;
    }

    public static long blockingTimeoutMs() {
        return (long) (HumanReadableDuration.minutes(1).toMilliseconds() * 0.8);
    }
}
