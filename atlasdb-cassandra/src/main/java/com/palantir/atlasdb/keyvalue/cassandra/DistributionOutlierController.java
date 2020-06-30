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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * Given a series of gauge metrics, allows for the retrieval for the mean. Furthermore allows for the generation of
 * {@link MetricPublicationFilter}s that identify whether any specific values exceed a tolerance value from the mean.
 */
public class DistributionOutlierController {
    private final Set<Gauge<Long>> gauges;
    private final double minimumMeanMultiple;
    private final double maximumMeanMultiple;
    private final Gauge<Double> meanGauge;

    public DistributionOutlierController(
            double minimumMeanMultiple,
            double maximumMeanMultiple) {
        this.gauges = new HashSet<>();
        this.minimumMeanMultiple = minimumMeanMultiple;
        this.maximumMeanMultiple = maximumMeanMultiple;
        this.meanGauge = new CachedGauge<Double>(30, TimeUnit.SECONDS) {
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

    public Gauge<Double> getMeanGauge() {
        return meanGauge;
    }

    public MetricPublicationFilter registerAndCreateFilter(Gauge<Long> gauge) {
        gauges.add(gauge);
        return () -> shouldPublishIndividualGaugeMetric(gauge);
    }

    private boolean shouldPublishIndividualGaugeMetric(Gauge<Long> constituentGauge) {
        long value = constituentGauge.getValue();
        double mean = meanGauge.getValue();
        return value > mean * maximumMeanMultiple || value < mean * minimumMeanMultiple;
    }
}
