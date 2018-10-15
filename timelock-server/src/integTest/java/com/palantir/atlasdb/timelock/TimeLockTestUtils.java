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

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.remoting.api.config.ssl.SslConfiguration;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public final class TimeLockTestUtils {
    private TimeLockTestUtils() {
        // Utility class
    }

    static TransactionManager createTransactionManager(TestableTimelockCluster cluster) {
        List<String> serverUris = cluster.servers().stream()
                .map(server -> server.serverHolder().getTimelockUri())
                .collect(Collectors.toList());
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace("test")
                .keyValueService(new InMemoryAtlasDbConfig())
                .timelock(ImmutableTimeLockClientConfig.builder()
                        .serversList(ImmutableServerListConfig.builder()
                                .servers(serverUris)
                                .sslConfiguration(SslConfiguration.of(Paths.get("var/security/trustStore.jks")))
                                .build())
                        .build())
                .build();
        return TransactionManagers.builder()
                .config(config)
                .userAgent("test")
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .build()
                .serializable();
    }

}
