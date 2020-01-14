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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.metrics.Timed;

public final class AtlasDbMetricsTest {

    private static final String CUSTOM_METRIC_NAME = "foo";
    private static final String PING_METHOD = "ping";
    private static final String PING_NOT_TIMED_METHOD = "pingNotTimed";
    private static final String PING_REQUEST_METRIC = MetricRegistry.name(TestService.class, PING_METHOD);
    private static final String PING_NOT_TIMED_METRIC = MetricRegistry.name(TestService.class, PING_NOT_TIMED_METHOD);
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
    private final TestService testServiceDelegate = (TestServiceAutoDelegate) () -> testService;

    @Test
    public void instrumentWithDefaultNameTimedDoesNotWorkWithDelegates() {
        TestService service = AtlasDbMetrics.instrumentTimed(metrics, TestService.class, testServiceDelegate);

        assertMethodNotInstrumented(PING_REQUEST_METRIC, service::ping);
        assertMethodNotInstrumented(PING_NOT_TIMED_METRIC, service::pingNotTimed);
    }

    @Test
    public void instrumentWithDefaultNameTimed() {
        TestService service = AtlasDbMetrics.instrumentTimed(metrics, TestService.class, testService);

        assertMethodInstrumented(PING_REQUEST_METRIC, service::ping);
        assertMethodNotInstrumented(PING_NOT_TIMED_METRIC, service::pingNotTimed);
    }

    @Test
    public void instrumentWithDefaultNameAll() {
        TestService service = AtlasDbMetrics.instrument(metrics, TestService.class, testService);

        assertMethodInstrumented(PING_REQUEST_METRIC, service::ping);
        assertMethodInstrumented(PING_NOT_TIMED_METRIC, service::pingNotTimed);
    }

    @Test
    public void instrumentWithCustomNameTimed() {
        TestService service = AtlasDbMetrics.instrumentTimed(
                metrics, TestService.class, testService, CUSTOM_METRIC_NAME);

        assertMethodInstrumented(
                MetricRegistry.name(CUSTOM_METRIC_NAME, PING_METHOD), service::ping);
        assertMethodNotInstrumented(
                MetricRegistry.name(CUSTOM_METRIC_NAME, PING_NOT_TIMED_METHOD), service::pingNotTimed);
    }

    @Test
    public void instrumentWithCustomNameAll() {
        TestService service = AtlasDbMetrics.instrument(
                metrics, TestService.class, testService, CUSTOM_METRIC_NAME);

        assertMethodInstrumented(MetricRegistry.name(CUSTOM_METRIC_NAME, PING_METHOD), service::ping);
        assertMethodInstrumented(MetricRegistry.name(CUSTOM_METRIC_NAME, PING_NOT_TIMED_METHOD), service::pingNotTimed);
    }

    private void assertMethodInstrumented(
            String methodTimerName,
            Supplier<String> invocation) {
        assertMethodInstrumentation(methodTimerName, invocation, true);
    }

    private void assertMethodNotInstrumented(
            String methodTimerName,
            Supplier<String> invocation) {
        assertMethodInstrumentation(methodTimerName, invocation, false);
    }

    private void assertMethodInstrumentation(
            String methodTimerName,
            Supplier<String> invocation,
            boolean isInstrumented) {
        assertTimerNotRegistered(methodTimerName);

        assertThat(invocation.get()).isEqualTo(PING_RESPONSE);

        if (isInstrumented) {
            assertThat(metrics.timer(methodTimerName).getCount()).isEqualTo(1);
        } else {
            assertTimerNotRegistered(methodTimerName);
        }
    }

    private void assertTimerNotRegistered(String timer) {
        assertThat(metrics.getTimers().get(timer)).isNull();
    }

    public interface TestService {
        @Timed
        String ping();

        String pingNotTimed();
    }

    public interface TestServiceAutoDelegate extends TestService {

        TestService getDelegate();

        @Override
        default String ping() {
            return getDelegate().ping();
        }

        @Override
        default String pingNotTimed() {
            return getDelegate().pingNotTimed();
        }
    }
}
