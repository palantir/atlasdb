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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.MetricsManager;

@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetrics {
    private final MetricsManager metricsManager = new MetricsManager();

    private final TableSpecificHistogramMetric cellsSweptHistogram =
            new TableSpecificHistogramMetric("cellTimestampPairsExamined");
    private final TableSpecificHistogramMetric cellsDeletedHistogram =
            new TableSpecificHistogramMetric("staleValuesDeleted");

    private final MeterMetric cellsSweptMeter = new MeterMetric("cellsSwept");
    private final MeterMetric cellsDeletedMeter = new MeterMetric("cellsDeleted");
    private final MeterMetric sweepErrorMeter = new MeterMetric("sweepError");

    private class TableSpecificHistogramMetric {
        private final String name;

        TableSpecificHistogramMetric(String name) {
            this.name = name;
        }

        void update(long value) {
            metricsManager.getRegistry().histogram(aggregateMetric()).update(value);
        }

        private String aggregateMetric() {
            return MetricRegistry.name(SweepMetrics.class, name);
        }
    }

    private class MeterMetric {
        private final Meter meter;

        MeterMetric(String name) {
            this.meter = metricsManager.registerOrGetMeter(SweepMetrics.class, null, name);
        }

        void update(long value) {
            this.meter.mark(value);
        }
    }

    void examinedCells(long numExamined) {
        cellsSweptHistogram.update(numExamined);
        cellsSweptMeter.update(numExamined);
    }

    void deletedCells(long numDeleted) {
        cellsDeletedHistogram.update(numDeleted);
        cellsDeletedMeter.update(numDeleted);
    }

    void sweepError() {
        sweepErrorMeter.update(1);
    }
}
