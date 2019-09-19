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
package com.palantir.atlasdb.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.v2.TimelockService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.Test;

public class TimestampTrackerTest {
    private static final long ONE = 1L;
    private static final long TEN = 10L;
    private static final long FORTY_TWO = 42L;

    private static final String IMMUTABLE_TIMESTAMP_NAME = "timestamp.immutable";
    private static final String FRESH_TIMESTAMP_NAME = "timestamp.fresh";
    private static final String UNREADABLE_TIMESTAMP_NAME = "timestamp.unreadable";
    private static final String FAKE_METRIC = "metric.fake";

    private static final long CACHE_INTERVAL_NANOS = TrackerUtils.DEFAULT_CACHE_INTERVAL.toNanos();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final TimelockService timelockService = mock(TimelockService.class);
    private final Cleaner cleaner = mock(Cleaner.class);
    private final Clock mockClock = mock(Clock.class);

    @Test
    public void defaultTrackerGeneratesTimestampMetrics() {
        createDefaultTracker();
        assertThat(metricsManager.getRegistry().getNames())
                .containsExactlyInAnyOrder(buildFullyQualifiedMetricName(IMMUTABLE_TIMESTAMP_NAME),
                        buildFullyQualifiedMetricName(FRESH_TIMESTAMP_NAME),
                        buildFullyQualifiedMetricName(UNREADABLE_TIMESTAMP_NAME));
    }

    @Test
    public void immutableTimestampTrackerDelegatesToTimeLock() {
        createDefaultTracker();
        when(timelockService.getImmutableTimestamp()).thenReturn(ONE);

        assertThat(getGauge(IMMUTABLE_TIMESTAMP_NAME).getValue()).isEqualTo(ONE);

        verify(timelockService).getImmutableTimestamp();
        verifyNoMoreInteractions(timelockService, cleaner);
    }

    @Test
    public void freshTimestampTrackerDelegatesToTimeLock() {
        createDefaultTracker();
        when(timelockService.getFreshTimestamp()).thenReturn(TEN);

        assertThat(getGauge(FRESH_TIMESTAMP_NAME).getValue()).isEqualTo(TEN);

        verify(timelockService).getFreshTimestamp();
        verifyNoMoreInteractions(timelockService, cleaner);
    }

    @Test
    public void unreadableTimestampTrackerDelegatesToCleaner() {
        createDefaultTracker();
        when(cleaner.getUnreadableTimestamp()).thenReturn(FORTY_TWO);

        assertThat(getGauge(UNREADABLE_TIMESTAMP_NAME).getValue()).isEqualTo(FORTY_TWO);

        verify(cleaner).getUnreadableTimestamp();
        verifyNoMoreInteractions(timelockService, cleaner);
    }

    @Test
    public void doesNotCallSupplierOnRequestsWithinRetriggerInterval() {
        when(mockClock.getTick()).thenReturn(0L, 1L, 2L);
        AtomicLong timestampValue = new AtomicLong(0L);
        TimestampTracker.registerTimestampForTracking(
                mockClock, metricsManager, FAKE_METRIC, timestampValue::incrementAndGet);

        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
    }

    @Test
    public void callsSupplierAgainAfterTimeElapses() {
        when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS - 1, CACHE_INTERVAL_NANOS + 1);
        AtomicLong timestampValue = new AtomicLong(0L);
        TimestampTracker.registerTimestampForTracking(
                mockClock, metricsManager, FAKE_METRIC, timestampValue::incrementAndGet);

        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
    }

    @Test
    public void doesNotCallSupplierUnlessGaugeIsQueried() {
        when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS * 50000);
        AtomicLong timestampValue = new AtomicLong(0L);
        TimestampTracker.registerTimestampForTracking(
                mockClock, metricsManager, FAKE_METRIC, timestampValue::incrementAndGet);

        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(1L);
        assertThat(getGauge(FAKE_METRIC).getValue()).isEqualTo(2L);
    }

    @Test
    public void timestampTrackersDoNotThrowEvenIfUnderlyingSupplierThrows() {
        when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS);
        TimestampTracker.registerTimestampForTracking(mockClock, metricsManager, FAKE_METRIC, () -> {
            throw new IllegalArgumentException("illegal argument");
        });

        getGauge(FAKE_METRIC).getValue();
    }

    @Test
    public void timestampTrackersReturnTheLastKnownValueIfUnderlyingSupplierThrows() {
        when(mockClock.getTick()).thenReturn(0L, CACHE_INTERVAL_NANOS + 1);
        TimestampTracker.registerTimestampForTracking(mockClock, metricsManager, FAKE_METRIC, new Supplier<Long>() {
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

    @Test
    public void doesNotThrowIfMetricsAreAccidentallyRegisteredMultipleTimes() {
        TimestampTracker.registerTimestampForTracking(mockClock, metricsManager, FAKE_METRIC, () -> 1L);
        TimestampTracker.registerTimestampForTracking(mockClock, metricsManager, FAKE_METRIC, () -> 2L); // OK

        // No guarantees on the value, other than that it's one of them
        assertThat(getGauge(FAKE_METRIC).getValue()).isIn(1L, 2L);
    }

    private void createDefaultTracker() {
        TimestampTracker.instrumentTimestamps(metricsManager, timelockService, cleaner);
    }

    private static String buildFullyQualifiedMetricName(String shortName) {
        return MetricRegistry.name(TimestampTracker.class, shortName);
    }

    @SuppressWarnings("unchecked") // We know the gauges we are registering produce Longs
    private Gauge<Long> getGauge(String shortName) {
        return (Gauge<Long>) metricsManager.getRegistry()
                .getGauges()
                .get(buildFullyQualifiedMetricName(shortName));
    }
}
