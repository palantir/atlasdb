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
import java.util.Optional;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Arg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetrics {
    private final TaggedMetricRegistry metricRegistry = new MetricsManager().getTaggedRegistry();

    private final SweepMetric cellsSweptMetric = new SweepMetric("cellTimestampPairsExamined");
    private final SweepMetric cellsDeletedMetric = new SweepMetric("staleValuesDeleted");
    private final SweepMetric sweepTimeSweepingMetric = new SweepMetric("sweepTimeSweeping");
    private final SweepMetric sweepTimeElapsedMetric = new SweepMetric("sweepTimeElapsedSinceStart");
    private final SweepMetric sweepErrorMetric = new SweepMetric("sweepError");

    private class SweepMetric {
        private final String histogram;
        private final String meter;
        private final String gauge;

        SweepMetric(String name) {
            this.histogram = name + "H";
            this.meter = name + "M";
            this.gauge = name + "G";
        }

        void update(long value, Optional<TableReference> tableRef) {
            metricRegistry.histogram(getTaggedMetric(histogram, tableRef)).update(value);
            metricRegistry.meter(getTaggedMetric(meter, tableRef)).mark(value);
        }

        void updateAggregate(long value) {
            metricRegistry.histogram(getTaggedMetric(histogram, Optional.empty())).update(value);
            ((Current) metricRegistry.gauge(getTaggedMetric(gauge, Optional.empty()), new Current())).setValue(value);
        }

    }

    @VisibleForTesting
    static class Current implements Gauge<Long> {
        private long value = 0;

        @Override
        public Long getValue() {
            return value;
        }

        public void setValue(long newValue) {
            value = newValue;
        }
    }

    private MetricName getTaggedMetric(String name, Optional<TableReference> tableRef) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(tableRef.map(SweepMetrics::getTag).orElse(ImmutableMap.of()))
                .build();
    }

    private static Map<String, String> getTag(TableReference tableRef) {
        Arg<String> safeOrUnsafeTableRef = LoggingArgs.tableRef(tableRef);
        if (safeOrUnsafeTableRef.isSafeForLogging()) {
            return ImmutableMap.of(safeOrUnsafeTableRef.getName(), safeOrUnsafeTableRef.getValue());
        } else {
            // todo(gmaretic) Do something smarter?
            return ImmutableMap.of("unsafeTableRef", "unsafe");
        }
    }

    void updateMetricsOneIteration(SweepResults sweepResults) {
        cellsSweptMetric.update(sweepResults.getCellTsPairsExamined(), Optional.empty());
        cellsDeletedMetric.update(sweepResults.getStaleValuesDeleted(), Optional.empty());
        sweepTimeSweepingMetric.update(sweepResults.getTimeInMillis(), Optional.empty());
        sweepTimeElapsedMetric.updateAggregate(sweepResults.getTimeElapsedSinceStartedSweeping());
    }

    void updateMetricsFullTable(SweepResults sweepResults, TableReference tableRef) {
        cellsSweptMetric.update(sweepResults.getCellTsPairsExamined(), Optional.of(tableRef));
        cellsDeletedMetric.update(sweepResults.getStaleValuesDeleted(), Optional.of(tableRef));
        sweepTimeSweepingMetric.update(sweepResults.getTimeInMillis(), Optional.of(tableRef));
        sweepTimeElapsedMetric.update(sweepResults.getTimeElapsedSinceStartedSweeping(), Optional.of(tableRef));
    }

    void sweepError() {
        sweepErrorMetric.update(1, Optional.empty());
    }
}
