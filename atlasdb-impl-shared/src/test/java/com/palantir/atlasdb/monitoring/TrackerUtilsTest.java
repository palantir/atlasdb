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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import java.util.function.Supplier;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked") // Mocks of known types
public class TrackerUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(TrackerUtilsTest.class);
    private static final String SHORT_NAME = "shortName";

    private final Supplier<Long> testSupplier = mock(Supplier.class);
    private final Clock clock = mock(Clock.class);
    private final Gauge<Long> cachingExceptionHandlingGauge =
            TrackerUtils.createCachingExceptionHandlingGauge(log, clock, SHORT_NAME, testSupplier);
    private final Gauge<Long> cachingMonotonicIncreasingGauge =
            TrackerUtils.createCachingMonotonicIncreasingGauge(log, clock, SHORT_NAME, testSupplier);

    @Test
    public void cachingExceptionHandlingGaugeReturnsPreviousValueIfExceptionsAreThrown() {
        when(testSupplier.get()).thenReturn(8L).thenThrow(new IllegalStateException("boo"));
        when(clock.getTick()).thenReturn(0L).thenReturn(TrackerUtils.DEFAULT_CACHE_INTERVAL.toNanos() + 1);

        assertThat(cachingExceptionHandlingGauge.getValue()).isEqualTo(8L);
        assertThat(cachingExceptionHandlingGauge.getValue()).isEqualTo(8L); // in particular, not an exception

        verify(testSupplier, times(2)).get();
    }

    @Test
    public void cachingExceptionHandlingGaugeChangesToNewValues() {
        when(testSupplier.get()).thenReturn(8L).thenReturn(11L);
        when(clock.getTick()).thenReturn(0L).thenReturn(TrackerUtils.DEFAULT_CACHE_INTERVAL.toNanos() + 1);

        assertThat(cachingExceptionHandlingGauge.getValue()).isEqualTo(8L);
        assertThat(cachingExceptionHandlingGauge.getValue()).isEqualTo(11L);

        verify(testSupplier, times(2)).get();
    }

    @Test
    public void cachingMonotonicIncreasingGaugeReturnsPreviousMaxIfExceptionsAreThrown() {
        when(testSupplier.get()).thenReturn(8L).thenThrow(new IllegalStateException("boo"));
        when(clock.getTick()).thenReturn(0L).thenReturn(TrackerUtils.DEFAULT_CACHE_INTERVAL.toNanos() + 1);

        assertThat(cachingMonotonicIncreasingGauge.getValue()).isEqualTo(8L);
        assertThat(cachingMonotonicIncreasingGauge.getValue()).isEqualTo(8L); // in particular, not an exception

        verify(testSupplier, times(2)).get();
    }

    @Test
    public void cachingMonotonicIncreasingGaugeChangesToNewValueIfItIsGreater() {
        when(testSupplier.get()).thenReturn(8L).thenReturn(11L);
        when(clock.getTick()).thenReturn(0L).thenReturn(TrackerUtils.DEFAULT_CACHE_INTERVAL.toNanos() + 1);

        assertThat(cachingMonotonicIncreasingGauge.getValue()).isEqualTo(8L);
        assertThat(cachingMonotonicIncreasingGauge.getValue()).isEqualTo(11L);

        verify(testSupplier, times(2)).get();
    }

    @Test
    public void cachingMonotonicIncreasingGaugeDoesNotChangeToNewValueIfItIsLesser() {
        when(testSupplier.get()).thenReturn(18L).thenReturn(11L);
        when(clock.getTick()).thenReturn(0L).thenReturn(TrackerUtils.DEFAULT_CACHE_INTERVAL.toNanos() + 1);

        assertThat(cachingMonotonicIncreasingGauge.getValue()).isEqualTo(18L);
        assertThat(cachingMonotonicIncreasingGauge.getValue()).isEqualTo(18L);

        verify(testSupplier, times(2)).get();
    }
}
