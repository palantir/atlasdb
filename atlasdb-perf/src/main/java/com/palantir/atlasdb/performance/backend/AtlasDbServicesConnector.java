/**
 * Copyright 2016 Palantir Technologies
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

package com.palantir.atlasdb.performance.backend;

import java.io.Closeable;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

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
            throw new IllegalStateException("connect() has already been called");
        }

        DockerizedDatabaseUri dburi = DockerizedDatabaseUri.fromUriString(uri);
        KeyValueServiceConfig config = dburi.getKeyValueServiceInstrumentation()
                .getKeyValueServiceConfig(dburi.getAddress());
        services = DaggerAtlasDbServices.builder()
                .servicesConfigModule(
                        ServicesConfigModule.create(
                                ImmutableAtlasDbConfig.builder()
                                        .keyValueService(config)
                                        .build()))
                .build();
        return services;
    }

    public void close() {
        if (services != null) {
            services.close();
        }
    }

}
