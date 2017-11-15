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

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Arg;
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

    private class HistogramMetric {
        private final String name;

        HistogramMetric(String name) {
            this.name = name;
        }

        void update(long value, TableReference tableRef) {
            metricsManager.getTaggedRegistry().histogram(getTaggedMetric(name, tableRef)).update(value);
        }

        void update(long value) {
            metricsManager.getTaggedRegistry().histogram(getNonTaggedMetric(name)).update(value);
        }
    }

    private class MeterMetric {
        private final String name;

        MeterMetric(String name) {
            this.name = name;
        }

        void update(long value, TableReference tableRef) {
            metricsManager.getTaggedRegistry().meter(getTaggedMetric(name, tableRef)).mark(value);
        }

        void update(long value) {
            metricsManager.getTaggedRegistry().meter(getNonTaggedMetric(name)).mark(value);
        }
    }

    private MetricName getTaggedMetric(String name, TableReference tableRef) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(getTag(tableRef))
                .build();
    }

    private Map<String, String> getTag(TableReference tableRef) {
        Arg<String> safeOrUnsafeTableRef = LoggingArgs.tableRef(tableRef);
        if (safeOrUnsafeTableRef.isSafeForLogging()) {
            return ImmutableMap.of(safeOrUnsafeTableRef.getName(), safeOrUnsafeTableRef.getValue());
        } else {
            // todo(gmaretic) Do something smarter?
            return ImmutableMap.of("unsafeTableRef", "unsafe");
        }
    }

    @VisibleForTesting
    static MetricName getNonTaggedMetric(String name) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(ImmutableMap.of())
                .build();
    }

    void examinedCellsOneIteration(long numExamined) {
        cellsSweptHistogram.update(numExamined);
        cellsSweptMeter.update(numExamined);
    }

    void deletedCellsOneIteration(long numDeleted) {
        cellsDeletedHistogram.update(numDeleted);
        cellsDeletedMeter.update(numDeleted);
    }

    void sweepTimeOneIteration(long timeSweeping) {
        sweepTimeHistogram.update(timeSweeping);
        sweepTimeMeter.update(timeSweeping);
    }

    void examinedCellsFullTable(long numExamined, TableReference tableRef) {
        cellsSweptHistogram.update(numExamined, tableRef);
        cellsSweptMeter.update(numExamined, tableRef);
    }

    void deletedCellsFullTable(long numDeleted, TableReference tableRef) {
        cellsDeletedHistogram.update(numDeleted, tableRef);
        cellsDeletedMeter.update(numDeleted, tableRef);
    }

    void sweepTimeForTable(long timeSweeping, TableReference tableRef) {
        sweepTimeHistogram.update(timeSweeping, tableRef);
        sweepTimeMeter.update(timeSweeping, tableRef);
    }

    void sweepError() {
        sweepErrorMeter.update(1);
    }
}
