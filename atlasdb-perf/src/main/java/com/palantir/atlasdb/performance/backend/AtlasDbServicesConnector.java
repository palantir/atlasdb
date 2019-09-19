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
package com.palantir.atlasdb.performance.backend;

import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.Closeable;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class AtlasDbServicesConnector implements Closeable {

    /**
     * Edit this instance variable name ("uri") with care as it must match {@code BenchmarkParam.URI}.getKey().
     */
    @Param("")
    private String uri;

    private AtlasDbServices services;

    public AtlasDbServices connect() {
        if (services != null) {
            throw new SafeIllegalStateException("connect() has already been called");
        }

        DockerizedDatabaseUri dburi = DockerizedDatabaseUri.fromUriString(uri);
        KeyValueServiceConfig config = dburi.getKeyValueServiceInstrumentation()
                .getKeyValueServiceConfig(dburi.getAddress());
        ImmutableAtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder().keyValueService(config).build();
        ImmutableAtlasDbRuntimeConfig runtimeConfig = ImmutableAtlasDbRuntimeConfig.defaultRuntimeConfig();
        ServicesConfigModule servicesConfigModule = ServicesConfigModule.create(atlasDbConfig, runtimeConfig);

        services = DaggerAtlasDbServices.builder()
                .servicesConfigModule(servicesConfigModule)
                .build();

        return services;
    }

    @Override public void close() {
        if (services != null) {
            services.close();
        }
    }

}
