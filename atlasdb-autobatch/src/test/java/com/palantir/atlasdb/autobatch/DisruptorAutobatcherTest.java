/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.Histogram;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public final class DisruptorAutobatcherTest {
    private static final String SAFE_LOGGABLE_PURPOSE = "test-purpose";
    private static final int BUFFER_SIZE = 128;

    @Test
    public void metricsAreNotReportedWhenRunningIsNotTriggered() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        DisruptorFuture<String> future =
                new DisruptorFuture<>(AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry));
        future.setException(new RuntimeException("Test"));
        assertNoWaitTimeAndRunningTimeMetricsAreProduced(registry);
    }

    @Test
    public void onlyWaitTimeIsReportedWhenRunningIsTriggeredButNoValueWasSet() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        FakeTicker fakeTicker = new FakeTicker();
        DisruptorFuture<String> future = new DisruptorFuture<>(
                fakeTicker, AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry));
        int waitTimeNanos = 10;
        fakeTicker.advance(waitTimeNanos, TimeUnit.NANOSECONDS);
        future.running();
        future.setException(new RuntimeException("Test"));
        assertOnlyWaitTimeMetricsAreProduced(registry, waitTimeNanos);
    }

    @Test
    public void allWaitAndRunningTimeMetricsAreReportedWhenRunningIsTriggeredAndValueWasSet() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        FakeTicker fakeTicker = new FakeTicker();
        DisruptorFuture<String> future = new DisruptorFuture<>(
                fakeTicker, AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry));
        long waitTimeNanos = 10;
        fakeTicker.advance(waitTimeNanos, TimeUnit.NANOSECONDS);
        future.running();
        long runningTimeNanos = 90;
        fakeTicker.advance(runningTimeNanos, TimeUnit.NANOSECONDS);
        future.set("Test");
        assertWaitTimeAndRunningTimeAndTotalTimeMetricsAreProduced(registry, waitTimeNanos, runningTimeNanos);
    }

    @Test
    public void closeClosesHandler() throws Exception {
        AutobatcherEventHandler<String, String> delegate = mock(AutobatcherEventHandler.class);
        DisruptorAutobatcher<String, String> autobatcher =
                DisruptorAutobatcher.create(delegate, BUFFER_SIZE, SAFE_LOGGABLE_PURPOSE, Optional.empty(), () -> {});
        autobatcher.close();
        verify(delegate).close();
    }

    @Test
    public void closeRunsClosingCallbacks() {
        Runnable closingCallback = mock(Runnable.class);
        DisruptorAutobatcher<String, String> autobatcher = DisruptorAutobatcher.create(
                NoOpAutobatcherEventHandler.INSTANCE,
                BUFFER_SIZE,
                SAFE_LOGGABLE_PURPOSE,
                Optional.empty(),
                closingCallback);
        autobatcher.close();
        verify(closingCallback).run();
    }

    @Test
    public void closeClosesHandlerWhenClosingCallbackThrows() throws Exception {
        AutobatcherEventHandler<String, String> delegate = mock(AutobatcherEventHandler.class);
        SafeRuntimeException exception = new SafeRuntimeException("test exception");
        DisruptorAutobatcher<String, String> autobatcher =
                DisruptorAutobatcher.create(delegate, BUFFER_SIZE, SAFE_LOGGABLE_PURPOSE, Optional.empty(), () -> {
                    throw exception;
                });
        assertThatLoggableExceptionThrownBy(autobatcher::close).isEqualTo(exception);
        verify(delegate).close();
    }

    @Test
    public void closeRunsClosingCallbackWhenHandlerClosingThrows() {
        Runnable closingCallback = mock(Runnable.class);
        SafeRuntimeException exception = new SafeRuntimeException("test exception");
        DisruptorAutobatcher<String, String> autobatcher = DisruptorAutobatcher.create(
                new AutobatcherEventHandlerWithThrowingClose(exception),
                BUFFER_SIZE,
                SAFE_LOGGABLE_PURPOSE,
                Optional.empty(),
                closingCallback);
        assertThatLoggableExceptionThrownBy(autobatcher::close)
                .hasLogMessage("Failed to close event handler")
                .hasNoArgs()
                .hasCause(exception);
        verify(closingCallback).run();
    }

    private void assertWaitTimeAndRunningTimeAndTotalTimeMetricsAreProduced(
            TaggedMetricRegistry registry, long waitTimeNanos, long runningTimeNanos) {
        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactly(waitTimeNanos);
        assertThat(getWaitTimePercentageHistogram(registry).getSnapshot().getValues())
                .containsExactly((100 * waitTimeNanos) / (waitTimeNanos + runningTimeNanos));
        assertThat(getRunningTimeHistogram(registry).getSnapshot().getValues()).containsExactly(runningTimeNanos);
        assertThat(getTotalTimeHistogram(registry).getSnapshot().getValues())
                .containsExactly(waitTimeNanos + runningTimeNanos);
    }

    private void assertNoWaitTimeAndRunningTimeMetricsAreProduced(TaggedMetricRegistry registry) {
        assertThat(getWaitTimeHistogram(registry)).isNull();
        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
        assertThat(getRunningTimeHistogram(registry)).isNull();
        assertThat(getTotalTimeHistogram(registry)).isNull();
    }

    private void assertOnlyWaitTimeMetricsAreProduced(TaggedMetricRegistry registry, int waitTimeNanos) {
        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactly(waitTimeNanos);
        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
        assertThat(getRunningTimeHistogram(registry)).isNull();
        assertThat(getTotalTimeHistogram(registry)).isNull();
    }

    private static Histogram getWaitTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimeNanosMetricName());
    }

    private static Histogram getRunningTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.runningTimeNanosMetricName());
    }

    private static Histogram getTotalTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.totalTimeNanosMetricName());
    }

    private static Histogram getWaitTimePercentageHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimePercentageMetricName());
    }

    private enum NoOpAutobatcherEventHandler implements AutobatcherEventHandler<String, String> {
        INSTANCE;

        @Override
        public void onEvent(BatchElement<String, String> _event, long _sequence, boolean _endOfBatch) {}

        @Override
        public void close() {}
    }

    private static final class AutobatcherEventHandlerWithThrowingClose
            implements AutobatcherEventHandler<String, String> {
        private final RuntimeException exception;

        AutobatcherEventHandlerWithThrowingClose(RuntimeException exception) {
            this.exception = exception;
        }

        @Override
        public void onEvent(BatchElement<String, String> _event, long _sequence, boolean _endOfBatch) {}

        @Override
        public void close() {
            throw exception;
        }
    }
}
