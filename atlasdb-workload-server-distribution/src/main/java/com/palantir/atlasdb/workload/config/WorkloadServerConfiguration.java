/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class WorkloadServerConfiguration extends Configuration {

    private final WorkloadServerRuntimeConfiguration runtime;
    private final WorkloadServerInstallConfiguration install;

    public WorkloadServerConfiguration(
            @JsonProperty(value = "install", required = true) WorkloadServerInstallConfiguration install,
            @JsonProperty(value = "runtime", required = true) WorkloadServerRuntimeConfiguration runtime) {
        this.install = install;
        this.runtime = runtime;
    }

    public WorkloadServerRuntimeConfiguration runtime() {
        return runtime;
    }

    public WorkloadServerInstallConfiguration install() {
        return install;
    }
}
