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
package com.palantir.atlasdb.qos.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.qos.config.QosServiceInstallConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;

import io.dropwizard.Configuration;

public class QosServerConfig extends Configuration {
    private final QosServiceRuntimeConfig runtime;
    private final QosServiceInstallConfig install;

    public QosServerConfig(@JsonProperty("runtime") QosServiceRuntimeConfig runtime,
            @JsonProperty("install") QosServiceInstallConfig install) {
        this.runtime = runtime;
        this.install = install;
    }

    public QosServiceRuntimeConfig runtime() {
        return runtime;
    }

    public QosServiceInstallConfig install() {
        return install;
    }
}
