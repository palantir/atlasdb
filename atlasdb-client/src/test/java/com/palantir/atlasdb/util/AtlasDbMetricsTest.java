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
package com.palantir.atlasdb.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.palantir.tritium.metrics.MetricRegistries;

public class AtlasDbMetricsTest {

    @Before
    public void before() throws Exception {
        AtlasDbMetrics.metrics = null;
    }

    @After
    public void after() throws Exception {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(AtlasDbMetrics.getMetricRegistry()).build();
        reporter.report();
        reporter.close();
    }

    @Test
    public void metricsIsDefaultWhenNotSet() {
        AtlasDbMetrics.metrics = null;
        assertThat(AtlasDbMetrics.getMetricRegistry(),
                is(equalTo(SharedMetricRegistries.getOrCreate(AtlasDbMetrics.DEFAULT_REGISTRY_NAME))));
    }

    @Test
    public void metricsIsNotDefaultWhenSet() {
        MetricRegistry metricRegistry = mock(MetricRegistry.class);
        AtlasDbMetrics.setMetricRegistry(metricRegistry);
        assertThat(AtlasDbMetrics.getMetricRegistry(), is(equalTo(metricRegistry)));
    }

    @Test(expected = NullPointerException.class)
    public void nullMetricsCannotBeSet() {
        AtlasDbMetrics.setMetricRegistry(null);
    }

    @Test
    public void instrumentWithDefaultName() throws Exception {
        MetricRegistry metrics = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metrics);
        TestService service = AtlasDbMetrics.instrument(TestService.class, () -> "pong");

        String methodTimerName = MetricRegistry.name(TestService.class, "ping");

        assertThat(metrics.timer(methodTimerName).getCount(), is(equalTo(0L)));

        assertThat(service.ping(), is(equalTo("pong")));

        assertThat(metrics.timer(methodTimerName).getCount(), is(equalTo(1L)));
    }

    @Test
    public void instrumentWithCustomName() throws Exception {
        MetricRegistry metrics = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metrics);
        TestService service = AtlasDbMetrics.instrument(TestService.class, () -> "pong", "foo");

        String methodTimerName = "foo.ping";

        assertThat(metrics.timer(methodTimerName).getCount(), is(equalTo(0L)));

        assertThat(service.ping(), is(equalTo("pong")));

        assertThat(metrics.timer(methodTimerName).getCount(), is(equalTo(1L)));
    }

    public interface TestService {
        String ping();
    }

}
