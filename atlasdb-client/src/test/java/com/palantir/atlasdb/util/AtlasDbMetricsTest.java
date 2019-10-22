/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.metrics.Timed;

public class AtlasDbMetricsTest {

    private static final String CUSTOM_METRIC_NAME = "foo";
    private static final String PING_REQUEST = "ping";
    private static final String PING_NOT_TIMED_REQUEST = "ping"
    private static final String PING_RESPONSE = "pong";

    private final MetricRegistry metrics = new MetricRegistry();
    private final TestService testService = new TestService() {
        @Override
        public String ping() {
            return PING_RESPONSE;
        }

        @Override
        public String pingNotTimed() {
            return PING_RESPONSE;
        }
    };

    @Test
    public void instrumentWithDefaultName() {
        TestService service = AtlasDbMetrics.instrumentTimed(metrics, TestService.class, testService);

        assertMetricCountIncrementsAfterPing(metrics, service, MetricRegistry.name(TestService.class, PING_REQUEST));
    }

    @Test
    public void instrumentWithCustomName() {
        TestService service = AtlasDbMetrics.instrument(
                metrics, TestService.class, testService, CUSTOM_METRIC_NAME);

        assertMetricCountIncrementsAfterPing(metrics, service, MetricRegistry.name(CUSTOM_METRIC_NAME, PING_REQUEST));
    }

    private void assertMetricCountIncrementsAfterPing(
            MetricRegistry metrics,
            TestService service,
            String methodTimerName,
            Runnable runnable) {
        assertThat(metrics.timer(methodTimerName).getCount(), is(equalTo(0L)));

        assertThat(service.ping(), is(equalTo(PING_RESPONSE)));

        assertThat(metrics.timer(methodTimerName).getCount(), is(equalTo(1L)));
    }

    public interface TestService {
        @Timed
        String ping();

        String pingNotTimed();
    }
}
