/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.tritium.metrics.registry.MetricName;

public class CassandraClientPoolMetricsTest {
    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    @Test
    public void metricsProducible() {
        CassandraClientPoolMetrics metrics = new CassandraClientPoolMetrics(metricsManager);
        AtomicLong poolOne = new AtomicLong();
        AtomicLong poolTwo = new AtomicLong();
        AtomicLong poolThree = new AtomicLong();
        metrics.registerPoolMetric(CassandraClientPoolHostLevelMetric.CREATED, poolOne::get, 1);
        metrics.registerPoolMetric(CassandraClientPoolHostLevelMetric.CREATED, poolTwo::get, 2);
        metrics.registerPoolMetric(CassandraClientPoolHostLevelMetric.CREATED, poolThree::get, 3);

        poolOne.set(3);
        poolTwo.set(4);
        poolThree.set(100);

        assertThat(metricsManager.getTaggedRegistry().getMetrics())
                .containsKey(MetricName.builder()
                        .safeName(MetricRegistry.name(CassandraClientPoolingContainer.class, CassandraClientPoolHostLevelMetric.CREATED.metricName))
                        .safeTags(ImmutableMap.of("pool", "pool1"))
                        .build());

        assertThat(metricsManager.getPublishableMetrics().getMetrics())
                .containsKey(MetricName.builder()
                        .safeName(MetricRegistry.name(CassandraClientPoolingContainer.class, CassandraClientPoolHostLevelMetric.CREATED.metricName))
                        .safeTags(ImmutableMap.of("pool", "pool1"))
                        .build());
    }
}
