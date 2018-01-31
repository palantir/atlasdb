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

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricsFactory {
    private final MetricRegistry metricRegistry = new MetricsManager().getRegistry();
    private final TaggedMetricRegistry taggedMetricRegistry = new MetricsManager().getTaggedRegistry();

    SweepMetric createDefault(String namePrefix) {
        return new SweepMetricsFactory.ListOfMetrics(
                createMeter(namePrefix, UpdateEventType.ONE_ITERATION, false),
                createHistogram(namePrefix, UpdateEventType.ONE_ITERATION, true),
                createHistogram(namePrefix, UpdateEventType.FULL_TABLE, false));
    }

    SweepMetric createMetricsForTimeElapsed(String namePrefix) {
        return new SweepMetricsFactory.ListOfMetrics(
                createCurrentValueLong(namePrefix, UpdateEventType.ONE_ITERATION, false),
                createHistogram(namePrefix, UpdateEventType.FULL_TABLE, false));
    }

    public SweepMetric createGaugeForTableBeingSwept(String namePrefix) {
        return createCurrentValueString(namePrefix, UpdateEventType.ONE_ITERATION, false);
    }

    /**
     * Creates a SweepMetric backed by a Meter. The name of the metric is the concatenation
     * SweepMetric.class.getName() + namePrefix + "Meter" + updateEvent.nameComponent().
     *
     * @param namePrefix Determines the prefix of the metric name.
     * @param updateEvent Determines on which type of event the metric should be updated and determines the suffix of
     *                    the metric name.
     * @param tagWithTableName If true, metric will also be tagged with the table name. If false, the metric will not be
     *                         tagged.
     * @return SweepMetric backed by a Meter
     */
    SweepMetric createMeter(String namePrefix, UpdateEventType updateEvent, boolean tagWithTableName) {
        return createMetric(namePrefix, updateEvent, tagWithTableName, SweepMetricAdapter.METER_ADAPTER);
    }

    /**
     * Creates a SweepMetric backed by a Histogram. The name of the metric is the concatenation
     * SweepMetric.class.getName() + namePrefix + "Histogram" + updateEvent.nameComponent().
     * The tagged histogram is backed by an {@link com.codahale.metrics.ExponentiallyDecayingReservoir}, while the
     * non-tagged histogram uses an {@link org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir}.
     *
     * @param namePrefix Determines the prefix of the metric name.
     * @param updateEvent Determines on which type of event the metric should be updated and determines the suffix of
     *                    the metric name.
     * @param tagWithTableName If true, metric will also be tagged with the table name. If false, the metric will not be
     *                         tagged, and will use an HdrHistogramReservoir.
     * @return SweepMetric backed by a Histogram
     */
    SweepMetric createHistogram(String namePrefix, UpdateEventType updateEvent, boolean tagWithTableName) {
        return createMetric(namePrefix, updateEvent, tagWithTableName, SweepMetricAdapter.HISTOGRAM_ADAPTER);
    }

    /**
     * Creates a SweepMetric backed by a CurrentValueMetric. The name of the metric is the concatenation
     * SweepMetric.class.getName() + namePrefix + "CurrentValue" + updateEvent.nameComponent().
     *
     * @param namePrefix Determines the prefix of the metric name.
     * @param updateEvent Determines on which type of event the metric should be updated and determines the suffix of
     *                    the metric name.
     * @param tagWithTableName If true, metric will also be tagged with the table name. If false, the metric will not be
     *                         tagged.
     * @return SweepMetric backed by a CurrentValueMetric
     */
    SweepMetric createCurrentValueLong(String namePrefix, UpdateEventType updateEvent, boolean tagWithTableName) {
        return createMetric(namePrefix, updateEvent, tagWithTableName, SweepMetricAdapter.CURRENT_VALUE_ADAPTER_LONG);
    }

    SweepMetric createCurrentValueString(String namePrefix, UpdateEventType updateEvent, boolean tagWithTableName) {
        return createMetric(namePrefix, updateEvent, tagWithTableName, SweepMetricAdapter.CURRENT_VALUE_ADAPTER_STRING);
    }

    private <T> SweepMetric<T> createMetric(String namePrefix, UpdateEventType updateEvent, boolean tagWithTableName,
            SweepMetricAdapter<?, T> metricAdapter) {
        return new SweepMetricImpl<>(ImmutableSweepMetricConfig.<T>builder()
                .namePrefix(namePrefix)
                .metricRegistry(metricRegistry)
                .taggedMetricRegistry(taggedMetricRegistry)
                .updateEvent(updateEvent)
                .tagWithTableName(tagWithTableName)
                .metricAdapter(metricAdapter)
                .build());
    }

    static class ListOfMetrics implements SweepMetric<Long> {
        private final List<SweepMetric<Long>> metricsList;

        @SafeVarargs
        ListOfMetrics(SweepMetric<Long>... metrics) {
            this.metricsList = Arrays.asList(metrics);
        }

        @Override
        public void update(Long value, TableReference tableRef, UpdateEventType updateEvent) {
            metricsList.forEach(metric -> metric.update(value, tableRef, updateEvent));
        }
    }
}
