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
package com.palantir.atlasdb.keyvalue.impl;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.MetricRegistries;

public class InstrumentedKeyValueServiceTest extends AbstractKeyValueServiceTest {

    private static final String METRIC_PREFIX = "test.instrumented." + KeyValueService.class.getName();

    private static MetricRegistry previousMetrics;
    private final MetricRegistry metrics = MetricRegistries.createWithHdrHistogramReservoirs();

    @Override
    protected KeyValueService getKeyValueService() {
        return AtlasDbMetrics.instrument(KeyValueService.class,
                new InMemoryKeyValueService(false),
                METRIC_PREFIX);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        previousMetrics = AtlasDbMetrics.getMetricRegistry();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        AtlasDbMetrics.setMetricRegistry(previousMetrics);
    }

    @Override
    public void setUp() throws Exception {
        AtlasDbMetrics.setMetricRegistry(metrics);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        tearDownKvs();
        AtlasDbMetrics.setMetricRegistry(previousMetrics);
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();
        reporter.report();
        reporter.close();
    }

}
