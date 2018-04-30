/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;

public class SweepMetricImpl<T> implements SweepMetric<T> {
    private final String name;
    private final MetricRegistry metricRegistry;
    private final SweepMetricAdapter<?, T> sweepMetricAdapter;

    SweepMetricImpl(String name, MetricRegistry metricRegistry, SweepMetricAdapter<?, T> sweepMetricAdapter) {
        this.name = MetricRegistry.name(SweepMetric.class, name);
        this.metricRegistry = metricRegistry;
        this.sweepMetricAdapter = sweepMetricAdapter;
    }

    @Override
    public void set(T value) {
        sweepMetricAdapter.setValue(metricRegistry, name, value);
    }

    @Override
    public void accumulate(T value) {
        sweepMetricAdapter.accumulateValue(metricRegistry, name, value);
    }
}
