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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.metrics.Timed;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class AtlasDbMetricsTest {

    private static final String CUSTOM_METRIC_NAME = "foo";
    private static final String PING_METHOD = "ping";
    private static final String PING_NOT_TIMED_METHOD = "pingNotTimed";
    private static final String PING_REQUEST_METRIC = MetricRegistry.name(TestService.class, PING_METHOD);
    private static final String PING_NOT_TIMED_METRIC = MetricRegistry.name(TestService.class, PING_NOT_TIMED_METHOD);
    private static final String PING_RESPONSE = "pong";
    private static final Duration ASYNC_DURATION_TTL = Duration.ofSeconds(2);

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

    private final ListeningScheduledExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
    private final TaggedMetricRegistry taggedMetrics = new DefaultTaggedMetricRegistry();
    private final AsyncTestService asyncTestService =
            () -> executorService.schedule(() -> "pong", ASYNC_DURATION_TTL.toMillis(), TimeUnit.MILLISECONDS);

    @After
    public void tearDown() {
        executorService.shutdown();
    }

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

    @Test
    public void instrumentTaggedAsyncFunction() throws InterruptedException, ExecutionException {
        AsyncTestService asyncTestService = AtlasDbMetrics.instrumentWithTaggedMetrics(taggedMetrics,
                AsyncTestService.class, this.asyncTestService);

        String asyncPingMetricName = MetricRegistry.name(AsyncTestService.class, "asyncPing");
        assertTimerNotRegistered(asyncPingMetricName);

        List<ListenableFuture<String>> futures = IntStream.range(0, 10)
                .mapToObj($ -> asyncTestService.asyncPing())
                .collect(Collectors.toList());

        Instant now = Instant.now();
        MetricName metricName = MetricName.builder().safeName(asyncPingMetricName).build();
        ListenableFuture<Boolean> done = Futures.whenAllSucceed(futures).call(() -> {
            // have to do it this because we can't edit the future we get back and it's only a callback as opposed to a
            // transformed future
            Awaitility.await()
                    .atMost(ASYNC_DURATION_TTL.toMillis(), TimeUnit.MILLISECONDS)
                    .until(() -> taggedMetrics.timer(metricName).getSnapshot().size() > 0);
            return true;
        }, MoreExecutors.directExecutor());

        assertThat(done.get()).isEqualTo(true);

        assertThat(done.get()).isEqualTo(true);
        assertThat(Instant.now())
                .as("in the event of scheduling issues, and despite having 10 concurrent futures, we complete "
                        + "everything with 2*ttl")
                .isBefore(now.plus(ASYNC_DURATION_TTL.plus(ASYNC_DURATION_TTL)));
        Snapshot snapshot = taggedMetrics.timer(metricName).getSnapshot();
        assertThat(Duration.ofNanos(snapshot.getMin())).isGreaterThan(ASYNC_DURATION_TTL);
        assertThat(snapshot.size()).isEqualTo(10);
    }

    @Test
    public void instrumentTaggedAsyncFunctionWithExtraTags() throws InterruptedException, ExecutionException {
        Map<String, String> extraTags = ImmutableMap.of("key", "value");
        AsyncTestService asyncTestService = AtlasDbMetrics.instrumentWithTaggedMetrics(
                taggedMetrics,
                AsyncTestService.class,
                this.asyncTestService,
                $ -> extraTags);

        String asyncPingMetricName = MetricRegistry.name(AsyncTestService.class, "asyncPing");

        List<ListenableFuture<String>> futures = IntStream.range(0, 10)
                .mapToObj($ -> asyncTestService.asyncPing())
                .collect(Collectors.toList());

        Instant now = Instant.now();
        MetricName metricName = MetricName.builder().safeName(asyncPingMetricName).safeTags(extraTags).build();
        ListenableFuture<Boolean> done = Futures.whenAllSucceed(futures).call(() -> {
            // have to do it this because we can't edit the future we get back and it's only a callback as opposed to a
            // transformed future
            Awaitility.await()
                    .atMost(ASYNC_DURATION_TTL.toMillis(), TimeUnit.MILLISECONDS)
                    .until(() -> taggedMetrics.timer(metricName).getSnapshot().size() > 0);
            return true;
        }, MoreExecutors.directExecutor());

        assertThat(done.get()).isEqualTo(true);
        assertThat(Instant.now())
                .as("in the event of scheduling issues, and despite having 10 concurrent futures, we complete "
                        + "everything with 2*ttl")
                .isBefore(now.plus(ASYNC_DURATION_TTL.plus(ASYNC_DURATION_TTL)));
        Snapshot snapshot = taggedMetrics.timer(metricName).getSnapshot();
        assertThat(Duration.ofNanos(snapshot.getMin())).isGreaterThan(ASYNC_DURATION_TTL);
        assertThat(snapshot.size()).isEqualTo(10);
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

    public interface AsyncTestService {
        ListenableFuture<String> asyncPing();
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
