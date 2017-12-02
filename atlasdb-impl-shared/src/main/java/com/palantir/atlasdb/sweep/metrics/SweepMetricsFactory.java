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
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricsFactory {
    private final TaggedMetricRegistry metricRegistry = new MetricsManager().getTaggedRegistry();

    SweepMetric createDefault(String name) {
        return new SweepMetricsFactory.ListOfMetrics(
                createMeter(name, UpdateEvent.ONE_ITERATION, false),
                createHistogram(name, UpdateEvent.ONE_ITERATION, true),
                createHistogram(name, UpdateEvent.FULL_TABLE, false));
    }

    SweepMetric createMeter(String name, UpdateEvent updateEvent, Boolean taggedWithTableName) {
        return new SweepMetric.SweepMetricForEvent(updateEvent, new SweepMeterMetric(name, taggedWithTableName));
    }

    SweepMetric createHistogram(String name, UpdateEvent updateEvent, Boolean taggedWithTableName) {
        return new SweepMetric.SweepMetricForEvent(updateEvent, new SweepHistogramMetric(name, taggedWithTableName));
    }

    SweepMetric createCurrentValue(String name, UpdateEvent updateEvent, Boolean taggedWithTableName) {
        return new SweepMetric.SweepMetricForEvent(updateEvent, new SweepCurrentValueMetric(name, taggedWithTableName));
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

    private class SweepMeterMetric extends SweepTaggedMetric implements SweepMetric {
        SweepMeterMetric(String name, Boolean taggedWithTableName) {
            super(name, taggedWithTableName);
        }

        @Override
        public void update(long value, TableReference tableRef, UpdateEvent updateEvent) {
            metricRegistry.meter(getMetricName(tableRef, updateEvent)).mark(value);
        }

        @Override
        public MetricType getMetricType() {
            return MetricType.METER;
        }
    }

    private class SweepHistogramMetric extends SweepTaggedMetric implements SweepMetric {

        SweepHistogramMetric(String name, Boolean taggedWithTableName) {
            super(name, taggedWithTableName);
        }

        @Override
        public void update(long value, TableReference tableRef, UpdateEvent updateEvent) {
            metricRegistry.histogram(getMetricName(tableRef, updateEvent)).update(value);
        }

        @Override
        public MetricType getMetricType() {
            return MetricType.HISTOGRAM;
        }

    }

    private class SweepCurrentValueMetric extends SweepTaggedMetric implements SweepMetric {
        SweepCurrentValueMetric(String name, Boolean taggedWithTableName) {
            super(name, taggedWithTableName);
        }

        @Override
        public void update(long value, TableReference tableRef, UpdateEvent updateEvent) {
            ((CurrentValueMetric) metricRegistry.gauge(getMetricName(tableRef, updateEvent), new CurrentValueMetric()))
                    .setValue(value);
        }

        @Override
        public MetricType getMetricType() {
            return MetricType.CURRENT_VALUE;
        }
    }
}
