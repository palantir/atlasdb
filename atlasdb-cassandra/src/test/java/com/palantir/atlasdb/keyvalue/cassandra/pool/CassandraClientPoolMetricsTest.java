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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class CassandraClientPoolMetricsTest {
    private final MetricsManager metricsManager =
            MetricsManagers.of(new MetricRegistry(), new DefaultTaggedMetricRegistry(), Refreshable.only(true));

    @Test
    public void metricsAreProducedAndFiltered() {
        CassandraClientPoolMetrics metrics = new CassandraClientPoolMetrics(metricsManager);
        AtomicLong poolOne = new AtomicLong(3);
        AtomicLong poolTwo = new AtomicLong(4);
        AtomicLong poolThree = new AtomicLong(20);

        metrics.registerPoolMetric(CassandraClientPoolHostLevelMetric.MEAN_ACTIVE_TIME_MILLIS, poolOne::get, 1);
        metrics.registerPoolMetric(CassandraClientPoolHostLevelMetric.MEAN_ACTIVE_TIME_MILLIS, poolTwo::get, 2);
        metrics.registerPoolMetric(CassandraClientPoolHostLevelMetric.MEAN_ACTIVE_TIME_MILLIS, poolThree::get, 3);

        assertThat(metricsManager.getTaggedRegistry().getMetrics())
                .containsKey(createMeanActiveTimeMillisMetric("pool1"))
                .containsKey(createMeanActiveTimeMillisMetric("pool2"))
                .containsKey(createMeanActiveTimeMillisMetric("pool3"))
                .containsKey(createMeanActiveTimeMillisMetric("mean"));

        assertThat(metricsManager.getPublishableMetrics().getMetrics())
                .doesNotContainKey(createMeanActiveTimeMillisMetric("pool1"))
                .doesNotContainKey(createMeanActiveTimeMillisMetric("pool2"))
                .containsKey(createMeanActiveTimeMillisMetric("pool3"))
                .containsKey(createMeanActiveTimeMillisMetric("mean"));
    }

    private static MetricName createMeanActiveTimeMillisMetric(String pool) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(
                        CassandraClientPoolingContainer.class,
                        CassandraClientPoolHostLevelMetric.MEAN_ACTIVE_TIME_MILLIS.metricName))
                .safeTags(ImmutableMap.of("pool", pool))
                .build();
    }
}
