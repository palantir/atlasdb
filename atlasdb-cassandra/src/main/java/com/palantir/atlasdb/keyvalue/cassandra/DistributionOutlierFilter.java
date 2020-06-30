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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.google.common.cache.LoadingCache;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * Given a series of gauge metrics, publishes only the average of these values.
 * If any values are more than the provided maximum threshold or minimum threshold
 */
public class DistributionOutlierFilter {
    private final List<Gauge<Long>> gaugeList;
    private final double maximumMeanMultiple;
    private final double minimumMeanMultiple;
    private final Gauge<Double> meanGauge;

    public DistributionOutlierFilter(
            List<Gauge<Long>> gaugeList,
            double maximumMeanMultiple,
            double minimumMeanMultiple) {
        this.gaugeList = gaugeList;
        this.maximumMeanMultiple = maximumMeanMultiple;
        this.minimumMeanMultiple = minimumMeanMultiple;
        this.meanGauge = new CachedGauge<Double>(30, TimeUnit.SECONDS) {
            @Override
            protected Double loadValue() {
                List<Long> gaugeValues = gaugeList.stream()
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

    public MetricPublicationFilter createFilter(Gauge<Long> gauge) {
        return () -> shouldPublishIndividualGaugeMetric(gauge);
    }

    private boolean shouldPublishIndividualGaugeMetric(Gauge<Long> constituentGauge) {
        long value = constituentGauge.getValue();
        double mean = meanGauge.getValue();
        return value > mean * maximumMeanMultiple || value < mean * minimumMeanMultiple;
    }
}
