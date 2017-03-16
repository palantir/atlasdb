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

import static org.junit.Assert.assertArrayEquals;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class SweepMetricsTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final TableReference OTHER_TABLE = TableReference.createFromFullyQualifiedName("test.other_table");
    private static final String DELETES_METRIC = MetricRegistry.name(SweepMetrics.class, "deletes",
            TABLE.getQualifiedName());
    private static final SweepResults SWEEP_RESULTS_FOR_TABLE = SweepResults.builder()
            .cellsDeleted(10L)
            .cellsExamined(15L)
            .sweptTimestamp(1337L)
            .build();
    private static final SweepResults SWEEP_RESULTS_FOR_OTHER_TABLE = SweepResults.builder()
            .cellsDeleted(12L)
            .cellsExamined(4L)
            .sweptTimestamp(1338L)
            .build();

    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();
    private SweepMetrics sweepMetrics;

    @Before
    public void setUp() {
        sweepMetrics = new SweepMetrics(METRIC_REGISTRY);
        sweepMetrics.registerMetricsIfNecessary(TABLE);
    }

    @After
    public void tearDown() {
        METRIC_REGISTRY.removeMatching((name, metric) -> true);
    }

    @Test
    public void cellsExaminedAreRecorded() {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);

        Histogram examinedMetric = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class, "cellsExamined",
                TABLE.getQualifiedName()));

        assertArrayEquals(new long[] {15L}, examinedMetric.getSnapshot().getValues());
    }

    @Test
    public void cellsExaminedAreRecordedSeparatelyAndAggregated() {
        sweepMetrics.recordMetrics(TABLE, SWEEP_RESULTS_FOR_TABLE);
        sweepMetrics.recordMetrics(OTHER_TABLE, SWEEP_RESULTS_FOR_OTHER_TABLE);

        Histogram tableExamined = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class, "cellsExamined",
                TABLE.getQualifiedName()));
        assertArrayEquals(new long[] {15L}, tableExamined.getSnapshot().getValues());

        Histogram otherTableExamined = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class,
                "cellsExamined",
                OTHER_TABLE.getQualifiedName()));
        assertArrayEquals(new long[] {4L}, otherTableExamined.getSnapshot().getValues());

        Histogram totalExamined = METRIC_REGISTRY.histogram(MetricRegistry.name(SweepMetrics.class,
                "totalCellsExamined"));
        assertArrayEquals(new long[] {4L, 15L}, totalExamined.getSnapshot().getValues());
    }

    @Test
    public void singleDeleteIsRecorded() {
        recordCellsDeleted(10);

        Histogram deleteMetric = METRIC_REGISTRY.histogram(DELETES_METRIC);

        assertArrayEquals(new long[] {10L}, deleteMetric.getSnapshot().getValues());
    }

    private void recordCellsDeleted(int cellsDeleted) {
        SweepResults sweepResults = SweepResults.builder()
                .cellsDeleted(cellsDeleted)
                .cellsExamined(0)
                .sweptTimestamp(0)
                .build();
        sweepMetrics.recordMetrics(TABLE, sweepResults);
    }

    @Test
    public void multipleDeletesAreRecorded() {
        recordCellsDeleted(10);
        recordCellsDeleted(15);

        Histogram deleteMetric = METRIC_REGISTRY.histogram(DELETES_METRIC);

        assertArrayEquals(new long[] {10L, 15L}, deleteMetric.getSnapshot().getValues());
    }

    @Ignore // This is just for me to run locally and check out what the metrics reports look like
    @Test
    public void testReporting() throws InterruptedException {
        recordCellsDeleted(10);
        recordCellsDeleted(15);

        ConsoleReporter reporter = ConsoleReporter.forRegistry(METRIC_REGISTRY)
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        Thread.sleep(4000);
    }

}
