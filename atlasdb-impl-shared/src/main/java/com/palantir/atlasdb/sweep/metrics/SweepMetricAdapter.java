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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@Value.Immutable
public abstract class SweepMetricAdapter<M extends Metric, T> {
    public abstract String getNameComponent();
    public abstract BiFunction<MetricRegistry, String, M> getMetricConstructor();
    public abstract BiFunction<TaggedMetricRegistry, MetricName, M> getTaggedMetricConstructor();
    public abstract BiConsumer<M, T> getUpdateMethod();

    public void updateNonTaggedMetric(MetricRegistry metricRegistry, String name, T value) {
        getUpdateMethod().accept(getMetricConstructor().apply(metricRegistry, name), value);
    }

    public void updateTaggedMetric(TaggedMetricRegistry taggedMetricRegistry, MetricName metricName, T value) {
        getUpdateMethod().accept(getTaggedMetricConstructor().apply(taggedMetricRegistry, metricName), value);
    }

    public static final SweepMetricAdapter<Meter, Long> METER_ADAPTER =
            ImmutableSweepMetricAdapter.<Meter, Long>builder()
                    .nameComponent("meter")
                    .metricConstructor(MetricRegistry::meter)
                    .taggedMetricConstructor(TaggedMetricRegistry::meter)
                    .updateMethod(Meter::mark)
                    .build();

    public static final SweepMetricAdapter<CurrentValueMetric<Long>, Long> CURRENT_VALUE_ADAPTER_LONG =
            getCurrentValueAdapterForClass();

    public static final SweepMetricAdapter<CurrentValueMetric<String>, String> CURRENT_VALUE_ADAPTER_STRING =
            getCurrentValueAdapterForClass();

    // We know that the unchecked casts will be fine.
    @SuppressWarnings("unchecked")
    private static <T> SweepMetricAdapter<CurrentValueMetric<T>, T> getCurrentValueAdapterForClass() {
        return ImmutableSweepMetricAdapter.<CurrentValueMetric<T>, T>builder()
                .nameComponent("currentValue")
                .metricConstructor((metricRegistry, name) ->
                        (CurrentValueMetric<T>) metricRegistry.gauge(name, CurrentValueMetric::new))
                .taggedMetricConstructor((taggedMetricRegistry, metricName) -> (CurrentValueMetric<T>)
                        taggedMetricRegistry.gauge(metricName, new CurrentValueMetric<T>()))
                .updateMethod(CurrentValueMetric::setValue)
                .build();
    }
}
