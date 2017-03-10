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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class SweepMetricsTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final String DELETES_METRIC = MetricRegistry.name(SweepMetrics.class, "deletes",
            TABLE.getQualifiedName());
    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();

    private SweepMetrics sweepMetrics;

    @Before
    public void setUp() {
        sweepMetrics = SweepMetrics.create();
        sweepMetrics.registerMetricsIfNecessary(TABLE);
    }

    @After
    public void tearDown() {
        METRIC_REGISTRY.remove(DELETES_METRIC);
    }

    @Test
    public void singleDeleteIsRecorded() {
        sweepMetrics.recordMetrics(TABLE, 10); // cells deleted

        Histogram deleteMetric = METRIC_REGISTRY.histogram(DELETES_METRIC);

        assertArrayEquals(new long[] {10L}, deleteMetric.getSnapshot().getValues());
    }

    @Test
    public void multipleDeletesAreRecorded() {
        sweepMetrics.recordMetrics(TABLE, 10); // cells deleted
        sweepMetrics.recordMetrics(TABLE, 15);

        Histogram deleteMetric = METRIC_REGISTRY.histogram(DELETES_METRIC);

        assertArrayEquals(new long[] {10L, 15L}, deleteMetric.getSnapshot().getValues());
    }

    @Ignore // This is just for me to run locally and check out what the metrics reports look like
    @Test
    public void testReporting() throws InterruptedException {
        sweepMetrics.recordMetrics(TABLE, 10); // cells deleted
        sweepMetrics.recordMetrics(TABLE, 15);

        ConsoleReporter reporter = ConsoleReporter.forRegistry(METRIC_REGISTRY)
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        Thread.sleep(4000);
    }

}
