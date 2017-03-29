/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.sweep;

import java.util.Arrays;
import java.util.function.Consumer;

import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

final class SweepMetrics {
    private enum TableAndAggregateMetric {
        STALE_VALUES_DELETED_METRIC(STALE_VALUES_DELETED),
        CELLS_EXAMINED_METRIC(CELLS_EXAMINED);

        private final String name;

        TableAndAggregateMetric(String name) {
            this.name = name;
        }

        void registerAggregateMetric() {
            registerMetricWithHdrHistogram(aggregateMetric());
        }

        void registerForTable(TableReference tableRef) {
            registerMetricWithHdrHistogram(getTableSpecificName(name, tableRef));
        }

        void recordMetric(TableReference tableRef, long value) {
            metricRegistry.histogram(getTableSpecificName(name, tableRef)).update(value);
            metricRegistry.histogram(aggregateMetric()).update(value);
        }

        private String aggregateMetric() {
            return MetricRegistry.name(SweepMetrics.class, name);
        }

        private String getTableSpecificName(String root, TableReference tableRef) {
            return MetricRegistry.name(SweepMetrics.class, root, tableRef.getQualifiedName());
        }

        private void registerMetricWithHdrHistogram(String metric) {
            registerMetricIfNotExists(metric, new Histogram(new HdrHistogramReservoir()));
        }

        private void registerMetricIfNotExists(String metricName, Metric metric) {
            if (!metricRegistry.getMetrics().containsKey(metricName)) {
                metricRegistry.register(metricName, metric);
            }
        }
    }

    static final String STALE_VALUES_DELETED = "staleValuesDeleted";
    static final String CELLS_EXAMINED = "cellsExamined";

    private static final MetricRegistry metricRegistry = AtlasDbMetrics.getMetricRegistry();

    public static SweepMetrics create() {
        SweepMetrics sweepMetrics = new SweepMetrics();
        sweepMetrics.registerAggregateMetrics();
        return sweepMetrics;
    }

    private SweepMetrics() {
        // To prevent instantiation
    }

    private void registerAggregateMetrics() {
        forAllMetrics(TableAndAggregateMetric::registerAggregateMetric);
    }

    void registerMetricsIfNecessary(TableReference tableRef) {
        forAllMetrics(metric -> metric.registerForTable(tableRef));
    }

    void recordMetrics(TableReference tableRef, SweepResults results) {
        TableAndAggregateMetric.CELLS_EXAMINED_METRIC.recordMetric(tableRef, results.getCellsExamined());
        TableAndAggregateMetric.STALE_VALUES_DELETED_METRIC.recordMetric(tableRef, results.getCellsDeleted());
    }

    private void forAllMetrics(Consumer<TableAndAggregateMetric> action) {
        Arrays.asList(TableAndAggregateMetric.values()).stream().forEach(
                action);
    }
}
