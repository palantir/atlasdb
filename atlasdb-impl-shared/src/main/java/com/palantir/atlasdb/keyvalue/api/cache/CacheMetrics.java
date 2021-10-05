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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;

public final class CacheMetrics {
    private final Counter hits;
    private final Counter misses;
    private final Counter cacheSize;
    private final Counter getRowsCellsHit;
    private final Counter getRowsCellLookups;
    private final Counter getRowsRowLookups;
    private final CurrentValueMetric<Integer> eventCacheValidationFailures;
    private final CurrentValueMetric<Integer> valueCacheValidationFailures;
    private final MetricsManager metricsManager;

    private CacheMetrics(
            Counter hits,
            Counter misses,
            Counter cacheSize,
            Counter getRowsCellsHit,
            Counter getRowsCellLookups,
            Counter getRowsRowLookups,
            CurrentValueMetric<Integer> eventCacheValidationFailures,
            CurrentValueMetric<Integer> valueCacheValidationFailures,
            MetricsManager metricsManager) {
        this.hits = hits;
        this.misses = misses;
        this.cacheSize = cacheSize;
        this.getRowsCellsHit = getRowsCellsHit;
        this.getRowsCellLookups = getRowsCellLookups;
        this.getRowsRowLookups = getRowsRowLookups;
        this.eventCacheValidationFailures = eventCacheValidationFailures;
        this.valueCacheValidationFailures = valueCacheValidationFailures;
        this.metricsManager = metricsManager;
    }

    public static CacheMetrics create(MetricsManager metricsManager) {
        return new CacheMetrics(
                metricsManager.registerOrGetCounter(CacheMetrics.class, AtlasDbMetricNames.LW_CACHE_HITS),
                metricsManager.registerOrGetCounter(CacheMetrics.class, AtlasDbMetricNames.LW_CACHE_MISSES),
                metricsManager.registerOrGetCounter(CacheMetrics.class, AtlasDbMetricNames.LW_CACHE_SIZE),
                metricsManager.registerOrGetCounter(CacheMetrics.class, AtlasDbMetricNames.LW_CACHE_GET_ROWS_HITS),
                metricsManager.registerOrGetCounter(
                        CacheMetrics.class, AtlasDbMetricNames.LW_CACHE_GET_ROWS_CELLS_LOADED),
                metricsManager.registerOrGetCounter(
                        CacheMetrics.class, AtlasDbMetricNames.LW_CACHE_GET_ROWS_ROWS_LOADED),
                registerCurrentValueMetric(metricsManager, AtlasDbMetricNames.LW_EVENT_CACHE_FALLBACK_COUNT),
                registerCurrentValueMetric(metricsManager, AtlasDbMetricNames.LW_VALUE_CACHE_FALLBACK_COUNT),
                metricsManager);
    }

    public void registerHits(long number) {
        hits.inc(number);
    }

    public void registerMisses(long number) {
        misses.inc(number);
    }

    public void increaseCacheSize(long added) {
        cacheSize.inc(added);
    }

    public void decreaseCacheSize(long removed) {
        cacheSize.dec(removed);
    }

    public void resetCacheSize() {
        cacheSize.dec(cacheSize.getCount());
    }

    public void increaseGetRowsHits(long number) {
        getRowsCellsHit.inc(number);
    }

    public void increaseGetRowsCellLookups(long number) {
        getRowsCellLookups.inc(number);
    }

    public void increaseGetRowsRowLookups(long number) {
        getRowsRowLookups.inc(number);
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
                AtlasDbMetricNames.LW_CACHE_RATIO_USED,
                () -> () -> cacheSize.getCount() / (double) maximumCacheSize);
    }

    public void setTransactionCacheInstanceCountGauge(Gauge<Integer> getCacheMapCount) {
        metricsManager.registerOrGetGauge(
                CacheMetrics.class, AtlasDbMetricNames.LW_TRANSACTION_CACHE_INSTANCE_COUNT, () -> getCacheMapCount);
    }

    public void setEventsHeldInMemory(Gauge<Integer> eventsGauge) {
        metricsManager.registerOrGetGauge(
                CacheMetrics.class, AtlasDbMetricNames.LW_EVENTS_HELD_IN_MEMORY, () -> eventsGauge);
    }

    public void setSnapshotsHeldInMemory(Gauge<Integer> snapshotGauge) {
        metricsManager.registerOrGetGauge(
                CacheMetrics.class, AtlasDbMetricNames.LW_SNAPSHOTS_HELD_IN_MEMORY, () -> snapshotGauge);
    }

    public void setSequenceDifference(Gauge<Long> differenceGauge) {
        metricsManager.registerOrGetGauge(
                CacheMetrics.class, AtlasDbMetricNames.LW_SEQUENCE_DIFFERENCE, () -> differenceGauge);
    }

    private static CurrentValueMetric<Integer> registerCurrentValueMetric(
            MetricsManager metricsManager, String lwEventCacheFallbackCount) {
        return metricsManager.registerOrGetGauge(
                CacheMetrics.class, lwEventCacheFallbackCount, CurrentValueMetric::new);
    }
}
