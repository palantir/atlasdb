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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;

public final class CacheMetrics {
    private final AccumulatingValueMetric hits;
    private final AccumulatingValueMetric misses;
    private final AccumulatingValueMetric cacheSize;
    private final CurrentValueMetric<Integer> eventCacheValidationFailures;
    private final CurrentValueMetric<Integer> valueCacheValidationFailures;
    private final MetricsManager metricsManager;

    private CacheMetrics(
            AccumulatingValueMetric hits,
            AccumulatingValueMetric misses,
            AccumulatingValueMetric cacheSize,
            CurrentValueMetric<Integer> eventCacheValidationFailures,
            CurrentValueMetric<Integer> valueCacheValidationFailures,
            MetricsManager metricsManager) {
        this.hits = hits;
        this.misses = misses;
        this.cacheSize = cacheSize;
        this.eventCacheValidationFailures = eventCacheValidationFailures;
        this.valueCacheValidationFailures = valueCacheValidationFailures;
        this.metricsManager = metricsManager;
    }

    public static CacheMetrics create(MetricsManager metricsManager) {
        return new CacheMetrics(
                metricsManager.registerOrGetGauge(
                        CacheMetrics.class, AtlasDbMetricNames.CACHE_HITS, AccumulatingValueMetric::new),
                metricsManager.registerOrGetGauge(
                        CacheMetrics.class, AtlasDbMetricNames.CACHE_MISSES, AccumulatingValueMetric::new),
                metricsManager.registerOrGetGauge(
                        CacheMetrics.class, AtlasDbMetricNames.CACHE_SIZE, AccumulatingValueMetric::new),
                metricsManager.registerOrGetGauge(
                        CacheMetrics.class,
                        AtlasDbMetricNames.EVENT_CACHE_VALIDATION_FAILURES,
                        CurrentValueMetric::new),
                metricsManager.registerOrGetGauge(
                        CacheMetrics.class,
                        AtlasDbMetricNames.VALUE_CACHE_VALIDATION_FAILURES,
                        CurrentValueMetric::new),
                metricsManager);
    }

    public void registerHits(long number) {
        hits.accumulateValue(number);
    }

    public void registerMisses(long number) {
        misses.accumulateValue(number);
    }

    public void increaseCacheSize(long added) {
        cacheSize.accumulateValue(added);
    }

    public void decreaseCacheSize(long removed) {
        cacheSize.accumulateValue(-removed);
    }

    public void registerEventCacheValidationFailure() {
        eventCacheValidationFailures.setValue(1);
    }

    public void registerValueCacheValidationFailure() {
        valueCacheValidationFailures.setValue(1);
    }

    public void setMaximumCacheSize(long maximumCacheSize) {
        metricsManager.registerOrGetGauge(
                CacheMetrics.class,
                AtlasDbMetricNames.CACHE_RATIO_USED,
                () -> () -> cacheSize.getValue() / (double) maximumCacheSize);
    }
}
