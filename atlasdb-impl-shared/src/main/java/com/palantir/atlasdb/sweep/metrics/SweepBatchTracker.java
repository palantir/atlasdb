/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.metrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.MetricsManager;

/**
 * Tracks the size of batches of cells that are read to be swept, and reports metrics accordingly (mean, p05, p01).
 * Note that we unfortunately can't use a histogram directly because of limitations of the internal logging framework
 * which expects users to be interested in high percentiles.
 *
 * If batches are very small and Targeted Sweep is not already up to date, it is likely to be incurring unnecessary
 * overhead in alternating between sleeping and deleting values, and can probably afford to read from more partitions
 * at a time. Note that batches may safely be very small when Targeted Sweep is up to date (because there is simply
 * nothing more to sweep).
 */
final class SweepBatchTracker {
    private final MetricsManager metricsManager;
    private final Histogram sweepBatchSizes;
    private final Supplier<Snapshot> snapshotSupplier;
    private final Map<String, String> tags;

    private SweepBatchTracker(
            MetricsManager metricsManager,
            Histogram sweepBatchSizes,
            Supplier<Snapshot> snapshotSupplier,
            Map<String, String> tags) {
        this.metricsManager = metricsManager;
        this.sweepBatchSizes = sweepBatchSizes;
        this.snapshotSupplier = snapshotSupplier;
        this.tags = tags;
    }

    static SweepBatchTracker create(MetricsManager metricsManager, Map<String, String> tags) {
        Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
        Supplier<Snapshot> memoizedSupplier = Suppliers.memoizeWithExpiration(
                histogram::getSnapshot,
                30,
                TimeUnit.SECONDS);

        SweepBatchTracker tracker = new SweepBatchTracker(metricsManager, histogram, memoizedSupplier, tags);
        tracker.registerMetrics();
        return tracker;
    }

    private void registerMetrics() {
        registerSweepBatchTrackerMetric(AtlasDbMetricNames.SWEEP_BATCH_SIZE_MEAN, Snapshot::getMean);
        registerSweepBatchTrackerMetric(AtlasDbMetricNames.SWEEP_BATCH_SIZE_P05, snapshot -> snapshot.getValue(0.05));
        registerSweepBatchTrackerMetric(AtlasDbMetricNames.SWEEP_BATCH_SIZE_P01, snapshot -> snapshot.getValue(0.01));
    }

    private void registerSweepBatchTrackerMetric(String metricName, Function<Snapshot, Double> metricExtractor) {
        metricsManager.registerOrGet(
                SweepBatchTracker.class,
                metricName,
                () -> metricExtractor.apply(snapshotSupplier.get()),
                tags);
    }
}
