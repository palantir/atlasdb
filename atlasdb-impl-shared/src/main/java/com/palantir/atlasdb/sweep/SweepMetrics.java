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

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Arg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetrics {
    @VisibleForTesting
    final Map<TableReference, Arg<String>> tableRefArgs = new HashMap<>();
    private final TaggedMetricRegistry metricRegistry = new MetricsManager().getTaggedRegistry();

    private final SweepMetric cellsSweptMetric = new SweepMetric("cellTimestampPairsExamined");
    private final SweepMetric cellsDeletedMetric = new SweepMetric("staleValuesDeleted");
    private final SweepMetric sweepTimeMetric = new SweepMetric("sweepTimeInMillis");
    private final SweepMetric sweepErrorMetric = new SweepMetric("sweepError");

    private class SweepMetric {
        private final String histogram;
        private final String meter;

        SweepMetric(String name) {
            this.histogram = name + "H";
            this.meter = name + "M";
        }

        void update(long value, TableReference tableRef) {
            Map<String, String> tag = getTag(tableRef);
            metricRegistry.histogram(getTaggedMetric(histogram, tag)).update(value);
            metricRegistry.meter(getTaggedMetric(meter, tag)).mark(value);
        }

        void update(long value) {
            metricRegistry.histogram(getNonTaggedMetric(histogram)).update(value);
            metricRegistry.meter(getNonTaggedMetric(meter)).mark(value);
        }
    }

    private MetricName getTaggedMetric(String name, Map<String, String> tag) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(tag)
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
        cellsSweptMetric.update(numExamined);
    }

    void deletedCellsOneIteration(long numDeleted) {
        cellsDeletedMetric.update(numDeleted);
    }

    void sweepTimeOneIteration(long timeSweeping) {
        sweepTimeMetric.update(timeSweeping);
    }

    void examinedCellsFullTable(long numExamined, TableReference tableRef) {
        cellsSweptMetric.update(numExamined, tableRef);
    }

    void deletedCellsFullTable(long numDeleted, TableReference tableRef) {
        cellsDeletedMetric.update(numDeleted, tableRef);
    }

    void sweepTimeForTable(long timeSweeping, TableReference tableRef) {
        sweepTimeMetric.update(timeSweeping, tableRef);
    }

    void sweepError() {
        sweepErrorMetric.update(1);
    }
}
