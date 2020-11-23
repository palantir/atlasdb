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

package com.palantir.atlasdb.timelock.adjudicate;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.SlidingWindowWeightedMeanGauge;
import com.palantir.timelock.feedback.LeaderElectionStatistics;
import java.util.concurrent.TimeUnit;

public final class LeaderElectionMetricAggregator {
    private static final int CONFIDENCE_THRESHOLD = 10;
    private final SlidingWindowWeightedMeanGauge weightedGaugeP99;
    private final SlidingWindowWeightedMeanGauge weightedGaugeP95;
    private final SlidingWindowWeightedMeanGauge weightedGaugeMean;
    private final LeaderElectionDurationAccumulator leaderElectionDurationAccumulator;

    public LeaderElectionMetricAggregator(MetricsManager metricsManager) {
        weightedGaugeP99 = SlidingWindowWeightedMeanGauge.create();
        weightedGaugeP95 = SlidingWindowWeightedMeanGauge.create();
        weightedGaugeMean = SlidingWindowWeightedMeanGauge.create();
        metricsManager.registerMetric(
                LeaderElectionMetricAggregator.class, "leaderElectionImpactMean", weightedGaugeMean);
        metricsManager.registerMetric(
                LeaderElectionMetricAggregator.class, "leaderElectionImpactP95", weightedGaugeP95);
        metricsManager.registerMetric(
                LeaderElectionMetricAggregator.class, "leaderElectionImpactP99", weightedGaugeP99);

        Histogram leaderElectionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(5, TimeUnit.MINUTES));
        leaderElectionDurationAccumulator =
                new LeaderElectionDurationAccumulator(leaderElectionHistogram::update, CONFIDENCE_THRESHOLD);
        metricsManager.registerMetric(
                LeaderElectionMetricAggregator.class,
                "leaderElectionEstimateMin",
                () -> leaderElectionHistogram.getSnapshot().getMin());
        metricsManager.registerMetric(
                LeaderElectionMetricAggregator.class,
                "leaderElectionEstimateMedian",
                () -> leaderElectionHistogram.getSnapshot().getMedian());
    }

    void report(LeaderElectionStatistics statistics) {
        long count = statistics.getCount().longValue();
        weightedGaugeMean.update(statistics.getMean(), count);
        weightedGaugeP95.update(statistics.getP95(), count);
        weightedGaugeP99.update(statistics.getP99(), count);
        statistics.getDurationEstimate().ifPresent(leaderElectionDurationAccumulator::add);
    }
}
