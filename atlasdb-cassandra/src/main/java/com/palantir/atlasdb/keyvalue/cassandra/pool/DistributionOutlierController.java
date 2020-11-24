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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Given a series of gauge metrics, allows for the retrieval for the mean. Furthermore allows for the generation of
 * {@link MetricPublicationFilter}s that identify whether any specific values exceed a tolerance value from the mean.
 */
class DistributionOutlierController {
    @VisibleForTesting
    static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final Set<Gauge<Long>> gauges;
    private final double minimumMeanMultiple;
    private final double maximumMeanMultiple;
    private final Gauge<Double> meanGauge;

    @VisibleForTesting
    DistributionOutlierController(Clock clock, double minimumMeanMultiple, double maximumMeanMultiple) {
        this.gauges = ConcurrentHashMap.newKeySet();
        this.minimumMeanMultiple = minimumMeanMultiple;
        this.maximumMeanMultiple = maximumMeanMultiple;
        this.meanGauge = new CachedGauge<Double>(clock, REFRESH_INTERVAL.toNanos(), TimeUnit.NANOSECONDS) {
            @Override
            protected Double loadValue() {
                List<Long> gaugeValues = gauges.stream()
                        .map(Gauge::getValue)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                if (gaugeValues.isEmpty()) {
                    return null;
                }
                return gaugeValues.stream()
                        .mapToLong(x -> x)
                        .average()
                        .orElseThrow(() -> new SafeIllegalStateException("Improperly handled average of 0 values"));
            }
        };
    }

    static DistributionOutlierController create(double minimumMeanMultiple, double maximumMeanMultiple) {
        return new DistributionOutlierController(Clock.defaultClock(), minimumMeanMultiple, maximumMeanMultiple);
    }

    Gauge<Double> getMeanGauge() {
        return meanGauge;
    }

    MetricPublicationFilter registerAndCreateFilter(Gauge<Long> gauge) {
        gauges.add(gauge);
        return () -> shouldPublishIndividualGaugeMetric(gauge);
    }

    private boolean shouldPublishIndividualGaugeMetric(Gauge<Long> constituentGauge) {
        long value = constituentGauge.getValue();
        Double mean = meanGauge.getValue();
        return mean != null && (value > mean * maximumMeanMultiple || value < mean * minimumMeanMultiple);
    }
}
