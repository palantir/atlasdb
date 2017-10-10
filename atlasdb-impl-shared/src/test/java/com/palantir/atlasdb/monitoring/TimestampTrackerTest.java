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

package com.palantir.atlasdb.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.v2.TimelockService;

public class TimestampTrackerTest {
    private static final long ONE = 1L;
    private static final long TEN = 10L;
    private static final long FORTY_TWO = 42L;

    private static final String IMMUTABLE_TIMESTAMP_NAME = "timestamp.immutable";
    private static final String FRESH_TIMESTAMP_NAME = "timestamp.fresh";
    private static final String UNREADABLE_TIMESTAMP_NAME = "timestamp.unreadable";
    private static final String FAKE_METRIC = "metric.fake";

    private static final long CACHE_RETRIGGER_NANOS = TimestampTracker.CACHE_INTERVAL.toNanos() + 1L;

    private final TimelockService timelockService = mock(TimelockService.class);
    private final Cleaner cleaner = mock(Cleaner.class);

    private final Clock mockClock = mock(Clock.class);

    @Test
    public void defaultTrackerGeneratesTimestampMetrics() {
        try (TimestampTracker ignored = TimestampTracker.createWithDefaultTrackers(timelockService, cleaner)) {
            assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                    .contains(buildFullyQualifiedMetricName(IMMUTABLE_TIMESTAMP_NAME))
                    .contains(buildFullyQualifiedMetricName(FRESH_TIMESTAMP_NAME))
                    .contains(buildFullyQualifiedMetricName(UNREADABLE_TIMESTAMP_NAME));
        }
    }

    @Test
    public void defaultTrackersDelegateToRelevantComponents() {
        try (TimestampTracker ignored = TimestampTracker.createWithDefaultTrackers(timelockService, cleaner)) {
            when(timelockService.getImmutableTimestamp()).thenReturn(ONE);
            when(timelockService.getFreshTimestamp()).thenReturn(TEN);
            when(cleaner.getUnreadableTimestamp()).thenReturn(FORTY_TWO);

            assertThat(getGauge(IMMUTABLE_TIMESTAMP_NAME).getValue()).isEqualTo(ONE);
            assertThat(getGauge(FRESH_TIMESTAMP_NAME).getValue()).isEqualTo(TEN);
            assertThat(getGauge(UNREADABLE_TIMESTAMP_NAME).getValue()).isEqualTo(FORTY_TWO);

            verify(timelockService).getImmutableTimestamp();
            verify(timelockService).getFreshTimestamp();
            verify(cleaner).getUnreadableTimestamp();
            verifyNoMoreInteractions(timelockService, cleaner);
        }
    }

    @Test
    public void metricsAreDeregisteredUponClose() {
        try (TimestampTracker tracker = new TimestampTracker(Clock.defaultClock())) {
            tracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);
            assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                    .contains(buildFullyQualifiedMetricName(FAKE_METRIC));
        }

        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .doesNotContain(buildFullyQualifiedMetricName(FAKE_METRIC));
    }

    @Test
    public void canCloseMultipleTimes() {
        TimestampTracker tracker = new TimestampTracker(Clock.defaultClock());
        tracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);

        tracker.close();
        tracker.close();
        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .doesNotContain(buildFullyQualifiedMetricName(FAKE_METRIC));
    }

    @Test
    public void doesNotAcquire() {
        try (TimestampTracker tracker = new TimestampTracker(mockClock)) {
            when(mockClock.getTick()).thenReturn(0L, 1L, 2L);
            AtomicLong timestampValue = new AtomicLong(0L);
            tracker.registerTimestampForTracking(FAKE_METRIC, timestampValue::incrementAndGet);

            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        }
    }

    @Test
    public void callsSupplierAgainAfterTimeElapses() {
        try (TimestampTracker tracker = new TimestampTracker(mockClock)) {
            when(mockClock.getTick()).thenReturn(0L, CACHE_RETRIGGER_NANOS, 2 * CACHE_RETRIGGER_NANOS);
            AtomicLong timestampValue = new AtomicLong(0L);
            tracker.registerTimestampForTracking(FAKE_METRIC, timestampValue::incrementAndGet);

            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(3L);
        }
    }

    private static String buildFullyQualifiedMetricName(String shortName) {
        return "com.palantir.atlasdb.monitoring.TimestampTracker." + shortName;
    }

    @SuppressWarnings("unchecked") // We know the gauges we are registering produce Longs
    private static Gauge<Long> getGauge(String shortName) {
        return (Gauge<Long>) AtlasDbMetrics.getMetricRegistry()
                .getGauges()
                .get(buildFullyQualifiedMetricName(shortName));
    }
}
