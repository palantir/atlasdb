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
package com.palantir.atlasdb.sweep;

import java.util.Map;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.MetricName;

@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetrics {
    private final MetricsManager metricsManager = new MetricsManager();

    private final MeterMetric cellsSweptMeter = new MeterMetric("cellsSwept");
    private final MeterMetric cellsDeletedMeter = new MeterMetric("cellsDeleted");
    private final MeterMetric sweepTimeMeter = new MeterMetric("sweepTime");
    private final MeterMetric sweepErrorMeter = new MeterMetric("sweepError");

    private final HistogramMetric cellsSweptHistogram = new HistogramMetric("cellTimestampPairsExamined");
    private final HistogramMetric cellsDeletedHistogram = new HistogramMetric("staleValuesDeleted");
    private final HistogramMetric sweepTimeHistogram = new HistogramMetric("sweepTimeInMillis");

    private final TableSpecificMeterMetric cellsSweptForTable = new TableSpecificMeterMetric("cellsSweptForTable");
    private final TableSpecificMeterMetric cellsDeletedForTable = new TableSpecificMeterMetric("cellsDeletedForTable");
    private final TableSpecificMeterMetric sweepTimeForTable = new TableSpecificMeterMetric("sweepTimeForTable");

    private final TableSpecificHistogramMetric cellsSweptHistogramForTable =
            new TableSpecificHistogramMetric("cellTimestampPairsExaminedForTable");
    private final TableSpecificHistogramMetric cellsDeletedHistogramForTable =
            new TableSpecificHistogramMetric("staleValuesDeleted");
    private final TableSpecificHistogramMetric sweepTimeHistogramForTable =
            new TableSpecificHistogramMetric("sweepTimeInMillisForTable");

    private class MeterMetric {
        private final Meter meter;

        MeterMetric(String name) {
            this.meter = metricsManager.registerMeter(SweepMetrics.class, null, name);
        }

        void update(long value) {
            this.meter.mark(value);
        }
    }

    private class HistogramMetric {
        private final Histogram histogram;

        HistogramMetric(String name) {
            this.histogram = metricsManager.getRegistry()
                    .histogram(MetricRegistry.name(SweepMetrics.class, name));
        }

        void update(long value) {
            this.histogram.update(value);
        }

    }

    private class TableSpecificHistogramMetric {
        private final String name;

        TableSpecificHistogramMetric(String name) {
            this.name = name;
        }
        void update(long value, TableReference tableRef) {
            metricsManager.getTaggedRegistry().histogram(getTaggedMetric(name, tableRef)).update(value);
        }
    }

    private class TableSpecificMeterMetric {
        private final String name;

        TableSpecificMeterMetric(String name) {
            this.name = name;
        }

        void update(long value, TableReference tableRef) {
            metricsManager.getTaggedRegistry().meter(getTaggedMetric(name, tableRef)).mark(value);
        }
    }

    private MetricName getTaggedMetric(String name, TableReference tableRef) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(getTag(tableRef))
                .build();
    }

    private Map<String,String> getTag(TableReference tableRef) {
        // todo(gmaretic) Tag with table name once we can use tagged metrics
        return ImmutableMap.of();
    }

    void examinedCellsOneIteration(long numExamined) {
        cellsSweptHistogram.update(numExamined);
        cellsSweptMeter.update(numExamined);
    }

    void examinedCellsFullTable(long numExamined, TableReference tableRef) {
        cellsSweptForTable.update(numExamined, tableRef);
        cellsSweptHistogramForTable.update(numExamined, tableRef);
    }

    void deletedCellsOneIteration(long numDeleted) {
        cellsDeletedHistogram.update(numDeleted);
        cellsDeletedMeter.update(numDeleted);
    }

    void deletedCellsFullTable(long numDeleted, TableReference tableRef) {
        cellsDeletedForTable.update(numDeleted, tableRef);
        cellsDeletedHistogramForTable.update(numDeleted, tableRef);
    }

    void sweepTimeOneIteration(long timeSweeping){
        sweepTimeHistogram.update(timeSweeping);
        sweepTimeMeter.update(timeSweeping);
    }

    void sweepTimeForTable(long timeSweeping, TableReference tableRef) {
        sweepTimeForTable.update(timeSweeping, tableRef);
        sweepTimeHistogramForTable.update(timeSweeping, tableRef);
    }

    void sweepError() {
        sweepErrorMeter.update(1);
    }
}
