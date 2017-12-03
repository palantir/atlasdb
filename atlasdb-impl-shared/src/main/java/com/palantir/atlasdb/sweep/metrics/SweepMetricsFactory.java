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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricsFactory {
    private final TaggedMetricRegistry metricRegistry = new MetricsManager().getTaggedRegistry();

    SweepMetric createDefault(String namePrefix) {
        return new SweepMetricsFactory.ListOfMetrics(
                createMeter(namePrefix, UpdateEvent.ONE_ITERATION, false),
                createHistogram(namePrefix, UpdateEvent.ONE_ITERATION, true),
                createHistogram(namePrefix, UpdateEvent.FULL_TABLE, false));
    }

    /**
     * Creates a SweepMetric backed by a Meter.
     *
     * @param namePrefix Determines the prefix of the metric name. The metric name will be namePrefix + "Meter"
     * @param updateEvent Determines on which type of event the metric should be updated. Metric will be tagged
     *                    with tag defined by the event
     * @param tagWithTableName If true, metric will also be tagged with the table name
     * @return SweepMetric backed by a Meter
     */
    SweepMetric createMeter(String namePrefix, UpdateEvent updateEvent, boolean tagWithTableName) {
        return new SweepMetricImpl(ImmutableSweepMetricConfig.builder()
                .namePrefix(namePrefix)
                .taggedMetricRegistry(metricRegistry)
                .updateEvent(updateEvent)
                .tagWithTableName(tagWithTableName)
                .metricAdapter(SweepMetricAdapter.METER_ADAPTER)
                .build());
    }

    /**
     * Creates a SweepMetric backed by a Histogram.
     *
     * @param namePrefix Determines the prefix of the metric name. The metric name will be namePrefix + "Histogram"
     * @param updateEvent Determines on which type of event the metric should be updated. Metric will be tagged
     *                    with tag defined by the event
     * @param tagWithTableName If true, metric will also be tagged with the table name
     * @return SweepMetric backed by a Histogram
     */
    SweepMetric createHistogram(String namePrefix, UpdateEvent updateEvent, boolean tagWithTableName) {
        return new SweepMetricImpl(ImmutableSweepMetricConfig.builder()
                .namePrefix(namePrefix)
                .taggedMetricRegistry(metricRegistry)
                .updateEvent(updateEvent)
                .tagWithTableName(tagWithTableName)
                .metricAdapter(SweepMetricAdapter.HISTOGRAM_ADAPTER)
                .build());
    }

    /**
     * Creates a SweepMetric backed by a CurrentValueMetric.
     *
     * @param namePrefix Determines the prefix of the metric name. The metric name will be namePrefix + "CurrentValue"
     * @param updateEvent Determines on which type of event the metric should be updated. Metric will be tagged
     *                    with tag defined by the event.
     * @param tagWithTableName If true, metric will also be tagged with the table name.
     * @return SweepMetric backed by a CurrentValueMetric
     */
    SweepMetric createCurrentValue(String namePrefix, UpdateEvent updateEvent, boolean tagWithTableName) {
        return new SweepMetricImpl(ImmutableSweepMetricConfig.builder()
                .namePrefix(namePrefix)
                .taggedMetricRegistry(metricRegistry)
                .updateEvent(updateEvent)
                .tagWithTableName(tagWithTableName)
                .metricAdapter(SweepMetricAdapter.CURRENT_VALUE_ADAPTER)
                .build());
    }

    static class ListOfMetrics implements SweepMetric {
        private final List<SweepMetric> metricsList;

        ListOfMetrics(SweepMetric... metrics) {
            this.metricsList = Arrays.stream(metrics).collect(Collectors.toList());
        }

        @Override
        public void update(long value, TableReference tableRef, UpdateEvent updateEvent) {
            metricsList.forEach(metric -> metric.update(value, tableRef, updateEvent));
        }
    }
}
