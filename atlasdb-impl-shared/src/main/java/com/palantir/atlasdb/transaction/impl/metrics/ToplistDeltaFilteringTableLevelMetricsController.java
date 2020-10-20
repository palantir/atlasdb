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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.atlasdb.util.MetricsManager;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This controller makes decisions based on deltas (in an attempt to be able to detect load spikes) measured over the
 * last {@code REFRESH_INTERVAL} period.
 */
public final class ToplistDeltaFilteringTableLevelMetricsController implements TableLevelMetricsController {
    private static final String CONTROLLER_GENERATED = "controllerGenerated";
    private static final String TRUE = "true";

    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final MetricsFilterEvaluationContext metricsFilterEvaluationContext;
    private final MetricsManager metricsManager;
    private final Clock clock;

    @VisibleForTesting
    ToplistDeltaFilteringTableLevelMetricsController(
            MetricsFilterEvaluationContext metricsFilterEvaluationContext, MetricsManager metricsManager, Clock clock) {
        this.metricsFilterEvaluationContext = metricsFilterEvaluationContext;
        this.metricsManager = metricsManager;
        this.clock = clock;
    }

    public static TableLevelMetricsController create(
            MetricsManager metricsManager, MetricsFilterEvaluationContext metricsFilterEvaluationContext) {
        return new ToplistDeltaFilteringTableLevelMetricsController(
                metricsFilterEvaluationContext, metricsManager, Clock.defaultClock());
    }

    /**
     * Warning: This method is potentially expensive, possibly registering filters multiple times.
     * It should not be called regularly.
     */
    @Override
    public <T> Counter createAndRegisterCounter(Class<T> clazz, String metricName, TableReference tableReference) {
        metricsManager.addMetricFilter(
                clazz, metricName, getTagsForTableReference(tableReference), MetricPublicationFilter.NEVER_PUBLISH);
        Counter counter =
                metricsManager.registerOrGetTaggedCounter(clazz, metricName, getTagsForTableReference(tableReference));

        Gauge<Long> gauge = new ZeroBasedDeltaGauge(counter::getCount);
        Gauge<Long> memoizedGauge = new CachedGauge<Long>(clock, REFRESH_INTERVAL.toNanos(), TimeUnit.NANOSECONDS) {
            @Override
            protected Long loadValue() {
                return gauge.getValue();
            }
        };

        MetricPublicationFilter filter =
                metricsFilterEvaluationContext.registerAndCreateTopNFilter(metricName, memoizedGauge);
        metricsManager.addMetricFilter(clazz, metricName, metricsManager.getTableNameTagFor(tableReference), filter);
        metricsManager.registerOrGet(clazz, metricName, gauge, metricsManager.getTableNameTagFor(tableReference));

        return counter;
    }

    private Map<String, String> getTagsForTableReference(TableReference tableReference) {
        return ImmutableMap.<String, String>builder()
                .putAll(metricsManager.getTableNameTagFor(tableReference))
                .put(CONTROLLER_GENERATED, TRUE)
                .build();
    }
}
