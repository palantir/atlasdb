/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.memory.InMemoryAsyncAtlasDbConfig;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.exception.NotInitializedException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.awaitility.Awaitility;
import org.junit.Test;

public class TransactionManagersAsyncInitializationTest {
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT_VERSION = "3.1415926.5358979";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(USER_AGENT_NAME, USER_AGENT_VERSION));

    private final Consumer<Object> environment = mock(Consumer.class);

    @Test
    public void asyncInitializationEventuallySucceeds() {
        AtlasDbConfig atlasDbConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAsyncAtlasDbConfig())
                .initializeAsync(true)
                .build();

        TransactionManager manager = TransactionManagers.builder()
                .config(atlasDbConfig)
                .userAgent(USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment)
                .addSchemas(GenericTestSchema.getSchema())
                .build()
                .serializable();

        assertThat(manager.isInitialized()).isFalse();
        assertThatThrownBy(() -> manager.runTaskWithRetry(_unused -> null)).isInstanceOf(NotInitializedException.class);

        Awaitility.await().atMost(Duration.ofSeconds(12)).until(manager::isInitialized);

        performTransaction(manager);
    }

    private static void performTransaction(TransactionManager manager) {
        RangeScanTestTable.RangeScanTestRow testRow = RangeScanTestTable.RangeScanTestRow.of("foo");
        manager.runTaskWithRetry(tx -> {
            GenericTestSchemaTableFactory.of().getRangeScanTestTable(tx).putColumn1(testRow, 12345L);
            return null;
        });
        Map<RangeScanTestTable.RangeScanTestRow, Long> result = manager.runTaskWithRetry(tx ->
                GenericTestSchemaTableFactory.of().getRangeScanTestTable(tx).getColumn1s(ImmutableSet.of(testRow)));

        assertThat(Iterables.getOnlyElement(result.entrySet()).getValue()).isEqualTo(12345L);
    }
}
