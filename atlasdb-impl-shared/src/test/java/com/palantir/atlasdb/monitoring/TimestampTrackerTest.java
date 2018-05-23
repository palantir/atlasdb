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
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cleaner.api.Cleaner;
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

    private static final long CACHE_INTERVAL_NANOS = TimestampTrackerImpl.CACHE_INTERVAL.toNanos();

    private final TimelockService timelockService = mock(TimelockService.class);
    private final Cleaner cleaner = mock(Cleaner.class);
    private final Clock mockClock = mock(Clock.class);

    @BeforeClass
    public static void cleanAtlasMetricsRegistry() {
        AtlasDbMetrics.getMetricRegistry().removeMatching(MetricFilter.ALL);
    }

    @Test
    public void defaultTrackerGeneratesTimestampMetrics() {
        try (TimestampTrackerImpl ignored = createDefaultTracker()) {
            assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                    .containsExactlyInAnyOrder(buildFullyQualifiedMetricName(IMMUTABLE_TIMESTAMP_NAME),
                            buildFullyQualifiedMetricName(FRESH_TIMESTAMP_NAME),
                            buildFullyQualifiedMetricName(UNREADABLE_TIMESTAMP_NAME));
        }
    }

    @Test
    public void immutableTimestampTrackerDelegatesToTimeLock() {
        try (TimestampTrackerImpl ignored = createDefaultTracker()) {
            when(timelockService.getImmutableTimestamp()).thenReturn(ONE);

            assertThat(getGauge(IMMUTABLE_TIMESTAMP_NAME).getValue()).isEqualTo(ONE);

            verify(timelockService).getImmutableTimestamp();
            verifyNoMoreInteractions(timelockService, cleaner);
        }
    }

    @Test
    public void freshTimestampTrackerDelegatesToTimeLock() {
        try (TimestampTrackerImpl ignored = createDefaultTracker()) {
            when(timelockService.getFreshTimestamp()).thenReturn(TEN);

            assertThat(getGauge(FRESH_TIMESTAMP_NAME).getValue()).isEqualTo(TEN);

            verify(timelockService).getFreshTimestamp();
            verifyNoMoreInteractions(timelockService, cleaner);
        }
    }

    @Test
    public void unreadableTimestampTrackerDelegatesToCleaner() {
        try (TimestampTrackerImpl ignored = createDefaultTracker()) {
            when(cleaner.getUnreadableTimestamp()).thenReturn(FORTY_TWO);

            assertThat(getGauge(UNREADABLE_TIMESTAMP_NAME).getValue()).isEqualTo(FORTY_TWO);

            verify(cleaner).getUnreadableTimestamp();
            verifyNoMoreInteractions(timelockService, cleaner);
        }
    }

    @Test
    public void metricsAreDeregisteredUponClose() {
        try (TimestampTrackerImpl tracker = createTrackerWithClock(Clock.defaultClock())) {
            tracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);
            assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                    .contains(buildFullyQualifiedMetricName(FAKE_METRIC));
        }

        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .doesNotContain(buildFullyQualifiedMetricName(FAKE_METRIC));
    }

    @Test
    public void canCloseMultipleTimes() {
        TimestampTrackerImpl tracker = createTrackerWithClock(Clock.defaultClock());
        tracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);

        tracker.close();
        tracker.close();
        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .doesNotContain(buildFullyQualifiedMetricName(FAKE_METRIC));
    }

    @Test
    public void doesNotCallSupplierOnRequestsWithinRetriggerInterval() {
        try (TimestampTrackerImpl tracker = createTrackerWithClock(mockClock)) {
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
        try (TimestampTrackerImpl tracker = createTrackerWithClock(mockClock)) {
            when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS - 1, CACHE_INTERVAL_NANOS + 1);
            AtomicLong timestampValue = new AtomicLong(0L);
            tracker.registerTimestampForTracking(FAKE_METRIC, timestampValue::incrementAndGet);

            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
        }
    }

    @Test
    public void doesNotCallSupplierUnlessGaugeIsQueried() {
        try (TimestampTrackerImpl tracker = createTrackerWithClock(mockClock)) {
            when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS * 50000);
            AtomicLong timestampValue = new AtomicLong(0L);
            tracker.registerTimestampForTracking(FAKE_METRIC, timestampValue::incrementAndGet);

            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
        }
    }

    @Test
    public void timestampTrackersDoNotThrowEvenIfUnderlyingSupplierThrows() {
        try (TimestampTrackerImpl tracker = createTrackerWithClock(mockClock)) {
            when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS);
            tracker.registerTimestampForTracking(FAKE_METRIC, () -> {
                throw new IllegalArgumentException("illegal argument");
            });

            getGauge(FAKE_METRIC).getValue();
        }
    }

    @Test
    public void timestampTrackersReturnTheLastKnownValueIfUnderlyingSupplierThrows() {
        try (TimestampTrackerImpl tracker = createTrackerWithClock(mockClock)) {
            when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS + 1);
            tracker.registerTimestampForTracking(FAKE_METRIC, new Supplier<Long>() {
                private boolean allowRequest = true;
                @Override
                public Long get() {
                    Preconditions.checkArgument(allowRequest, "not allowed");
                    allowRequest = false;
                    return FORTY_TWO;
                }
            });

            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(FORTY_TWO);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(FORTY_TWO);
        }
    }

    @Test
    public void canCreateMultipleDistinctTimestampTrackers() {
        when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS + 1);

        try (TimestampTrackerImpl firstTracker = createTrackerWithClock(mockClock)) {
            firstTracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        }

        try (TimestampTrackerImpl secondTracker = createTrackerWithClock(mockClock)) {
            secondTracker.registerTimestampForTracking(FAKE_METRIC, () -> 2L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
        }
    }

    @Test
    public void cachesNotSharedAcrossDistinctTimestampTrackers() {
        when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS - 1);

        try (TimestampTrackerImpl firstTracker = createTrackerWithClock(mockClock)) {
            firstTracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        }

        try (TimestampTrackerImpl secondTracker = createTrackerWithClock(mockClock)) {
            secondTracker.registerTimestampForTracking(FAKE_METRIC, () -> 2L);

            // If caches were shared, this would return 1L because we haven't had a cache interval yet.
            assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
        }
    }

    @Test
    public void doesNotThrowIfMetricsAreAccidentallyRegisteredMultipleTimes() {
        try (TimestampTrackerImpl firstTracker = createTrackerWithClock(mockClock);
                TimestampTrackerImpl secondTracker = createTrackerWithClock(mockClock)) {
            firstTracker.registerTimestampForTracking(FAKE_METRIC, () -> 1L);
            secondTracker.registerTimestampForTracking(FAKE_METRIC, () -> 2L); // OK

            // No guarantees on the value, other than that it's one of them
            assertThat(getGauge(FAKE_METRIC).getValue()).isIn(1L, 2L);
        }
    }

    private TimestampTrackerImpl createTrackerWithClock(Clock clock) {
        return new TimestampTrackerImpl(clock, timelockService, cleaner);
    }

    private TimestampTrackerImpl createDefaultTracker() {
        return (TimestampTrackerImpl) TimestampTrackerImpl.createWithDefaultTrackers(timelockService, cleaner, false);
    }

    private static String buildFullyQualifiedMetricName(String shortName) {
        return MetricRegistry.name(TimestampTracker.class, shortName);
    }

    @SuppressWarnings("unchecked") // We know the gauges we are registering produce Longs
    private static Gauge<Long> getGauge(String shortName) {
        return (Gauge<Long>) AtlasDbMetrics.getMetricRegistry()
                .getGauges()
                .get(buildFullyQualifiedMetricName(shortName));
    }
}
