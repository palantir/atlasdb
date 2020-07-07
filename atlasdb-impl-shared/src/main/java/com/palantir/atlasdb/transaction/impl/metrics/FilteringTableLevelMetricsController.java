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

package com.palantir.atlasdb.transaction.impl.metrics;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.TopNMetricPublicationController;

public final class FilteringTableLevelMetricsController implements TableLevelMetricsController {
    private static final int DEFAULT_MAX_TABLES_TO_PUBLISH_METRICS = 10;
    private static final String CONTROLLER_GENERATED = "controllerGenerated";
    private static final String TRUE = "true";

    @VisibleForTesting
    static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final Map<String, TopNMetricPublicationController<Long>> metricNameToPublicationController;
    private final MetricsManager metricsManager;
    private final int maximumNumberOfTables;
    private final Clock clock;

    @VisibleForTesting
    FilteringTableLevelMetricsController(
            MetricsManager metricsManager,
            int maximumNumberOfTables,
            Clock clock) {
        this.metricNameToPublicationController = Maps.newConcurrentMap();
        this.metricsManager = metricsManager;
        this.maximumNumberOfTables = maximumNumberOfTables;
        this.clock = clock;
    }

    public static TableLevelMetricsController create(MetricsManager metricsManager) {
        return new FilteringTableLevelMetricsController(
                metricsManager,
                DEFAULT_MAX_TABLES_TO_PUBLISH_METRICS,
                Clock.defaultClock());
    }

    @Override
    public <T> Counter createAndRegisterCounter(Class<T> clazz, String metricName, TableReference tableReference) {
        Counter counter = metricsManager.registerOrGetTaggedCounter(
                clazz,
                metricName,
                getTagsForTableReference(tableReference));
        metricsManager.addMetricFilter(
                clazz,
                metricName,
                getTagsForTableReference(tableReference),
                MetricPublicationFilter.NEVER_PUBLISH);


        Gauge<Long> gauge = new ZeroBasedDeltaGauge(counter::getCount);
        Gauge<Long> memoizedGauge = new CachedGauge<Long>(clock, REFRESH_INTERVAL.toNanos(), TimeUnit.NANOSECONDS) {
            @Override
            protected Long loadValue() {
                return gauge.getValue();
            }
        };

        MetricPublicationFilter filter = metricNameToPublicationController.computeIfAbsent(metricName,
                _name -> TopNMetricPublicationController.create(maximumNumberOfTables))
                .registerAndCreateFilter(memoizedGauge);
        metricsManager.addMetricFilter(
                clazz,
                metricName,
                metricsManager.getTableNameTagFor(tableReference),
                filter);
        metricsManager.registerOrGet(
                clazz,
                metricName,
                gauge,
                metricsManager.getTableNameTagFor(tableReference));

        return counter;
    }

    private Map<String, String> getTagsForTableReference(TableReference tableReference) {
        return ImmutableMap.<String, String>builder()
                .putAll(metricsManager.getTableNameTagFor(tableReference))
                .put(CONTROLLER_GENERATED, TRUE)
                .build();
    }
}
