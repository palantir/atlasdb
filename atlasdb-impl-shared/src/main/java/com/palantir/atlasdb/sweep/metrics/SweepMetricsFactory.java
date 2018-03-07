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

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.MetricsManager;

public class SweepMetricsFactory {
    private final MetricRegistry metricRegistry = new MetricsManager().getRegistry();

    SweepMetric<Long> simpleLong(String namePrefix) {
        return createMetric(namePrefix, SweepMetricAdapter.CURRENT_VALUE_ADAPTER_LONG);
    }

    SweepMetric<String> simpleString(String namePrefix) {
        return createMetric(namePrefix, SweepMetricAdapter.CURRENT_VALUE_ADAPTER_STRING);
    }

    SweepMetric<Long> accumulatingLong(String namePrefix) {
        return createMetric(namePrefix, SweepMetricAdapter.ACCUMULATING_VALUE_ADAPTER);
    }

    SweepMetric<Long> simpleMeter(String namePrefix) {
        return createMetric(namePrefix, SweepMetricAdapter.METER_ADAPTER);
    }

    private <T> SweepMetric<T> createMetric(String name, SweepMetricAdapter<?, T> metricAdapter) {
        return new SweepMetricImpl<>(name, metricRegistry, metricAdapter);
    }
}
