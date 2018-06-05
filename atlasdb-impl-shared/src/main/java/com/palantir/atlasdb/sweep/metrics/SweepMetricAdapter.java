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
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.CurrentValueMetric;

@Value.Immutable
public abstract class SweepMetricAdapter<M extends Metric, T> {
    public abstract BiFunction<MetricRegistry, String, M> getMetricConstructor();
    abstract BiConsumer<M, T> getSetMethod();

    @Value.Default
    BiConsumer<M, T> getAccumulateMethod() {
        return getSetMethod();
    }

    void setValue(MetricRegistry metricRegistry, String name, T value) {
        getSetMethod().accept(getMetricConstructor().apply(metricRegistry, name), value);
    }

    void accumulateValue(MetricRegistry metricRegistry, String name, T value) {
        getAccumulateMethod().accept(getMetricConstructor().apply(metricRegistry, name), value);
    }

    static final SweepMetricAdapter<Meter, Long> METER_ADAPTER =
            ImmutableSweepMetricAdapter.<Meter, Long>builder()
                    .metricConstructor(MetricRegistry::meter)
                    .setMethod(Meter::mark)
                    .build();

    // We know that the unchecked casts will be fine.
    @SuppressWarnings("unchecked")
    static final SweepMetricAdapter<CurrentValueMetric<Long>, Long> CURRENT_VALUE_ADAPTER_LONG =
            ImmutableSweepMetricAdapter.<CurrentValueMetric<Long>, Long>builder()
                    .metricConstructor((metricRegistry, name) ->
                            (CurrentValueMetric<Long>) metricRegistry.gauge(name, CurrentValueMetric::new))
                    .setMethod(CurrentValueMetric::setValue)
                    .build();

    // We know that the unchecked casts will be fine.
    @SuppressWarnings("unchecked")
    static final SweepMetricAdapter<CurrentValueMetric<String>, String> CURRENT_VALUE_ADAPTER_STRING =
            ImmutableSweepMetricAdapter.<CurrentValueMetric<String>, String>builder()
                    .metricConstructor((metricRegistry, name) ->
                            (CurrentValueMetric<String>) metricRegistry.gauge(name, CurrentValueMetric::new))
                    .setMethod(CurrentValueMetric::setValue)
                    .build();

}
