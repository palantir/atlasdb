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
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@Value.Immutable
public abstract class SweepMetricAdapter<M extends Metric> {
    public abstract String getNameComponent();
    public abstract BiFunction<MetricRegistry, String, M> getMetricConstructor();
    public abstract BiFunction<TaggedMetricRegistry, MetricName, M> getTaggedMetricConstructor();
    public abstract BiConsumer<M, Long> getUpdateMethod();

    public void updateNonTaggedMetric(MetricRegistry metricRegistry, String name, Long value) {
        getUpdateMethod().accept(getMetricConstructor().apply(metricRegistry, name), value);
    }

    public void updateTaggedMetric(TaggedMetricRegistry taggedMetricRegistry, MetricName metricName, Long value) {
        getUpdateMethod().accept(getTaggedMetricConstructor().apply(taggedMetricRegistry, metricName), value);
    }

    public static final SweepMetricAdapter<Meter> METER_ADAPTER =
            ImmutableSweepMetricAdapter.<Meter>builder()
                    .nameComponent("meter")
                    .metricConstructor(MetricRegistry::meter)
                    .taggedMetricConstructor(TaggedMetricRegistry::meter)
                    .updateMethod(Meter::mark)
                    .build();

    public static final SweepMetricAdapter<Histogram> HISTOGRAM_ADAPTER =
            ImmutableSweepMetricAdapter.<Histogram>builder()
                    .nameComponent("histogram")
                    .metricConstructor((metricRegistry, name) ->
                            metricRegistry.histogram(name, () -> new Histogram(new HdrHistogramReservoir())))
                    .taggedMetricConstructor(TaggedMetricRegistry::histogram)
                    .updateMethod(Histogram::update)
                    .build();

    public static final SweepMetricAdapter<CurrentValueMetric> CURRENT_VALUE_ADAPTER =
            ImmutableSweepMetricAdapter.<CurrentValueMetric>builder()
                    .nameComponent("currentValue")
                    .metricConstructor((metricRegistry, name) ->
                            (CurrentValueMetric) metricRegistry.gauge(name, CurrentValueMetric::new))
                    .taggedMetricConstructor((taggedMetricRegistry, metricName) ->
                            (CurrentValueMetric) taggedMetricRegistry.gauge(metricName, new CurrentValueMetric()))
                    .updateMethod(CurrentValueMetric::setValue)
                    .build();
}
