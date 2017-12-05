/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.metrics;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.immutables.value.Value;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MeanValueMetric;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@Value.Immutable
public abstract class SweepMetricAdapter<M extends Metric> {
    public abstract String getNameSuffix();
    public abstract BiFunction<TaggedMetricRegistry, MetricName, M> getMetricConstructor();
    public abstract BiConsumer<M, Long> getUpdateMethod();

    public void updateMetric(TaggedMetricRegistry taggedMetricRegistry, MetricName metricName, Long value) {
        getUpdateMethod().accept(getMetricConstructor().apply(taggedMetricRegistry, metricName), value);
    }

    public static final SweepMetricAdapter<Meter> METER_ADAPTER =
            ImmutableSweepMetricAdapter.<Meter>builder()
                    .nameSuffix("Meter")
                    .metricConstructor(TaggedMetricRegistry::meter)
                    .updateMethod(Meter::mark)
                    .build();

    public static final SweepMetricAdapter<Histogram> HISTOGRAM_ADAPTER =
            ImmutableSweepMetricAdapter.<Histogram>builder()
                    .nameSuffix("Histogram")
                    .metricConstructor(TaggedMetricRegistry::histogram)
                    .updateMethod(Histogram::update)
                    .build();

    public static final SweepMetricAdapter<CurrentValueMetric> CURRENT_VALUE_ADAPTER =
            ImmutableSweepMetricAdapter.<CurrentValueMetric>builder()
                    .nameSuffix("CurrentValue")
                    .metricConstructor((taggedMetricRegistry, metricName) ->
                            (CurrentValueMetric) taggedMetricRegistry.gauge(metricName, new CurrentValueMetric()))
                    .updateMethod(CurrentValueMetric::setValue)
                    .build();

    public static final SweepMetricAdapter<CurrentValueMetric.MaximumValueMetric> MAXIMUM_VALUE_ADAPTER =
            ImmutableSweepMetricAdapter.<CurrentValueMetric.MaximumValueMetric>builder()
                    .nameSuffix("MaximumValue")
                    .metricConstructor((taggedMetricRegistry, metricName) -> (CurrentValueMetric.MaximumValueMetric)
                            taggedMetricRegistry.gauge(metricName, new CurrentValueMetric.MaximumValueMetric()))
                    .updateMethod(CurrentValueMetric.MaximumValueMetric::setValue)
                    .build();

    public static final SweepMetricAdapter<MeanValueMetric> MEAN_VALUE_ADAPTER =
            ImmutableSweepMetricAdapter.<MeanValueMetric>builder()
                    .nameSuffix("MeanValue")
                    .metricConstructor((taggedMetricRegistry, metricName) ->
                            (MeanValueMetric) taggedMetricRegistry.gauge(metricName, new MeanValueMetric()))
                    .updateMethod(MeanValueMetric::addEntry)
                    .build();
}
