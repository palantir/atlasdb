/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
import java.net.InetSocketAddress;
import java.nio.file.Paths;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockRuntimeConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.remoting.api.config.ssl.SslConfiguration;

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

        InetSocketAddress first = InetSocketAddress.createUnresolved("il-pg-alpha-2822832.use1.palantir.global", 9160);
        InetSocketAddress second = InetSocketAddress.createUnresolved("il-pg-alpha-2822833.use1.palantir.global", 9160);
        InetSocketAddress third = InetSocketAddress.createUnresolved("il-pg-alpha-2822834.use1.palantir.global", 9160);


        SslConfiguration ssl = SslConfiguration.builder()
                .keyStorePath(Paths.get("/opt/palantir/services/.1378/var/security/keystore.jks"))
                .trustStorePath(Paths.get("/opt/palantir/services/.1378/var/security/truststore.jks"))
                .keyStorePassword("upVkPSqf4heABZXA")
                .build();

        CassandraKeyValueServiceConfig cassandraConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .servers(ImmutableSet.of(first, second, third))
                .replicationFactor(3)
                .sslConfiguration(ssl)
                .build();

        AtlasDbConfig installConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(cassandraConfig)
                .namespace("benchmarks")
                .build();

        ServerListConfig servers = ImmutableServerListConfig.builder()
                .servers(ImmutableSet.of(
                    "https://il-pg-alpha-2822832.use1.palantir.global:8421/timelock/api",
                    "https://il-pg-alpha-2822833.use1.palantir.global:8421/timelock/api",
                    "https://il-pg-alpha-2822834.use1.palantir.global:8421/timelock/api"))
                .sslConfiguration(ssl)
                .build();

        AtlasDbRuntimeConfig runtimeConfig = ImmutableAtlasDbRuntimeConfig.builder()
                .timelockRuntime(ImmutableTimeLockRuntimeConfig.builder().serversList(servers).build())
                .build();

        ServicesConfigModule servicesConfigModule = ServicesConfigModule.create(installConfig, runtimeConfig);

        services = DaggerAtlasDbServices.builder()
                .servicesConfigModule(servicesConfigModule)
                .build();

        return services;
    }

    public void close() {
        if (services != null) {
            services.close();
        }
    }

}
