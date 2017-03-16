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

import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

class SweepMetrics {
    private static final String AGGREGATE_DELETES = "totalDeletes";
    private static final String AGGREGATE_DELETES_METRIC = MetricRegistry.name(SweepMetrics.class, AGGREGATE_DELETES);
    private static final String PER_TABLE_DELETES = "deletes";

    private final MetricRegistry metricRegistry;

    public static SweepMetrics create() {
        SweepMetrics sweepMetrics = new SweepMetrics(AtlasDbMetrics.getMetricRegistry());
        sweepMetrics.registerAggregateMetrics();
        return sweepMetrics;
    }

    protected SweepMetrics(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    private void registerAggregateMetrics() {
        Histogram histogram = new Histogram(new HdrHistogramReservoir());

        registerMetricIfNotExists(AGGREGATE_DELETES_METRIC, histogram);
    }

    void registerMetricsIfNecessary(TableReference tableRef) {
        Histogram histogram = new Histogram(new HdrHistogramReservoir());
        String deletesMetric = getPerTableDeletesMetric(tableRef);

        registerMetricIfNotExists(deletesMetric, histogram);
    }

    void recordMetrics(TableReference tableRef, SweepResults results) {
        long cellsDeleted = results.getCellsDeleted();
        String deletesMetric = getPerTableDeletesMetric(tableRef);
        metricRegistry.histogram(deletesMetric).update(cellsDeleted);

        metricRegistry.histogram(AGGREGATE_DELETES_METRIC).update(cellsDeleted);
    }

    private String getPerTableDeletesMetric(TableReference tableRef) {
        return MetricRegistry.name(SweepMetrics.class, PER_TABLE_DELETES, tableRef.getQualifiedName());
    }

    private void registerMetricIfNotExists(String name, Metric metric) {
        if (!metricRegistry.getMetrics().containsKey(name)) {
            metricRegistry.register(name, metric);
        }
    }
}
