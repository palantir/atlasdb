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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class SweepMetricsTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final TableReference OTHER_TABLE = TableReference.createFromFullyQualifiedName("test.other_table");

    private static final long DELETED = 10L;
    private static final long EXAMINED = 15L;
    private static final SweepResults SWEEP_RESULTS_FOR_TABLE = SweepResults.builder()
            .cellsDeleted(DELETED)
            .cellsExamined(EXAMINED)
            .sweptTimestamp(1337L)
            .build();

    private static final long OTHER_DELETED = 12L;
    private static final long OTHER_EXAMINED = 4L;
    private static final SweepResults SWEEP_RESULTS_FOR_OTHER_TABLE = SweepResults.builder()
            .cellsDeleted(OTHER_DELETED)
            .cellsExamined(OTHER_EXAMINED)
            .sweptTimestamp(1338L)
            .build();

    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();

    private SweepMetrics sweepMetrics;

    @Before
    public void setUp() {
        sweepMetrics = SweepMetrics.create();
        sweepMetrics.registerMetricsIfNecessary(TABLE);
    }

    @After
    public void tearDown() {
        METRIC_REGISTRY.removeMatching((name, metric) -> true);
    }

    @Test
    public void cellsDeletedAreRecorded() {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);

        assertCellsDeleted(TABLE, DELETED);
    }

    @Test
    public void cellsDeletedAreRecordedSeparatelyAndAggregated() {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);
        sweepMetrics.recordMetrics(OTHER_TABLE, SWEEP_RESULTS_FOR_OTHER_TABLE);

        assertCellsDeleted(TABLE, DELETED);
        assertCellsDeleted(OTHER_TABLE, OTHER_DELETED);
        assertValuesRecorded(SweepMetrics.STALE_VALUES_DELETED, DELETED, OTHER_DELETED);
    }

    @Test
    public void cellsExaminedAreRecorded() {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);

        assertCellsExamined(TABLE, EXAMINED);
    }

    @Test
    public void cellsExaminedAreRecordedSeparatelyAndAggregated() {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);
        sweepMetrics.recordMetrics(OTHER_TABLE, SWEEP_RESULTS_FOR_OTHER_TABLE);

        assertCellsExamined(TABLE, EXAMINED);
        assertCellsExamined(OTHER_TABLE, OTHER_EXAMINED);
        assertValuesRecorded(SweepMetrics.CELLS_EXAMINED, EXAMINED, OTHER_EXAMINED);
    }

    @Ignore // This is just for me to run locally and check out what the metrics reports look like
    @Test
    public void testReporting() throws InterruptedException {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);
        sweepMetrics.recordMetrics(OTHER_TABLE, SWEEP_RESULTS_FOR_OTHER_TABLE);

        ConsoleReporter reporter = ConsoleReporter.forRegistry(METRIC_REGISTRY)
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        Thread.sleep(4000);
    }

    private void assertValuesRecorded(String aggregateMetric, Long... values) {
        Histogram histogram = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class, aggregateMetric));
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertCellsDeleted(TableReference table, long deleted) {
        Histogram deleteMetric = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class, "staleValuesDeleted",
                table.getQualifiedName()));
        assertArrayEquals(new long[] { deleted }, deleteMetric.getSnapshot().getValues());
    }

    private void assertCellsExamined(TableReference table, long examined) {
        Histogram examinedMetric = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class, "cellsExamined",
                table.getQualifiedName()));
        assertArrayEquals(new long[] { examined }, examinedMetric.getSnapshot().getValues());
    }
}
