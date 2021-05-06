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

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.debug.LockDiagnosticComponents;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class TimeLockTestUtils {
    private TimeLockTestUtils() {
        // Utility class
    }

    static TransactionManager createTransactionManager(TestableTimelockCluster cluster) {
        return createTransactionManager(cluster, UUID.randomUUID().toString());
    }

    static TransactionManager createTransactionManager(TestableTimelockCluster cluster, String agent) {
        return createTransactionManager(cluster, agent, AtlasDbRuntimeConfig.defaultRuntimeConfig(), Optional.empty())
                .transactionManager();
    }

    static TransactionManagerContext createTransactionManager(
            TestableTimelockCluster cluster,
            String agent,
            AtlasDbRuntimeConfig runtimeConfigTemplate,
            Optional<LockDiagnosticComponents> diagnosticComponents,
            Schema... schemas) {
        List<String> serverUris = cluster.servers().stream()
                .map(server -> server.serverHolder().getTimelockUri())
                .collect(Collectors.toList());
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .namespace(agent)
                .keyValueService(new InMemoryAtlasDbConfig())
                .timelock(ImmutableTimeLockClientConfig.builder()
                        .serversList(ImmutableServerListConfig.builder()
                                .servers(serverUris)
                                .sslConfiguration(SslConfiguration.of(Paths.get("var/security/trustStore.jks")))
                                .build())
                        .build())
                .build();

        AtlasDbRuntimeConfig runtimeConfig = ImmutableAtlasDbRuntimeConfig.copyOf(runtimeConfigTemplate)
                .withRemotingClient(RemotingClientConfigs.DEFAULT);

        TransactionManager transactionManager = TransactionManagers.builder()
                .config(config)
                .userAgent(UserAgent.of(UserAgent.Agent.of("u" + agent, "0.0.0")))
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .runtimeConfigSupplier(() -> Optional.of(runtimeConfig))
                .lockDiagnosticComponents(diagnosticComponents)
                .addSchemas(schemas)
                .build()
                .serializable();

        return ImmutableTransactionManagerContext.builder()
                .transactionManager(transactionManager)
                .install(config)
                .runtime(runtimeConfig)
                .build();
    }

    @Value.Immutable
    interface TransactionManagerContext {
        AtlasDbConfig install();

        AtlasDbRuntimeConfig runtime();

        TransactionManager transactionManager();
    }
}
