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
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.v2.TimelockService;

public class TimestampTrackerTest {
    private final TimelockService timelockService = mock(TimelockService.class);
    private final Cleaner cleaner = mock(Cleaner.class);

    private static final long ONE = 1L;
    private static final long TEN = 10L;
    private static final long FORTY_TWO = 42L;

    private static final String IMMUTABLE_TIMESTAMP_NAME = "timestamp.immutable";
    private static final String FRESH_TIMESTAMP_NAME = "timestamp.fresh";
    private static final String UNREADABLE_TIMESTAMP_NAME = "timestamp.unreadable";

    @Test
    public void defaultTrackerGeneratesTimestampMetrics() {
        TimestampTracker tracker = TimestampTracker.createWithDefaultTrackers(timelockService, cleaner);

        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .contains(buildFullyQualifiedMetricName(IMMUTABLE_TIMESTAMP_NAME))
                .contains(buildFullyQualifiedMetricName(FRESH_TIMESTAMP_NAME))
                .contains(buildFullyQualifiedMetricName(UNREADABLE_TIMESTAMP_NAME));

        tracker.close();
    }

    @Test
    public void defaultTrackersDelegateToRelevantComponents() {
        when(timelockService.getImmutableTimestamp()).thenReturn(ONE);
        when(timelockService.getFreshTimestamp()).thenReturn(TEN);
        when(cleaner.getUnreadableTimestamp()).thenReturn(FORTY_TWO);

        TimestampTracker tracker = TimestampTracker.createWithDefaultTrackers(timelockService, cleaner);

        Gauge<Long> immutableTimestampGauge = getGauge(IMMUTABLE_TIMESTAMP_NAME);
        assertThat(immutableTimestampGauge.getValue()).isEqualTo(ONE);

        tracker.close();
    }

    @Test
    public void metricsAreDeregisteredUponClose() {
        String shortName = "one";

        TimestampTracker tracker = new TimestampTracker();
        tracker.registerTimestampForTracking(shortName, () -> 1L);
        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .contains(buildFullyQualifiedMetricName(shortName));

        tracker.close();
        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .doesNotContain(buildFullyQualifiedMetricName(shortName));
    }

    @Test
    public void canCloseMultipleTimes() {
        String shortName = "one";

        TimestampTracker tracker = new TimestampTracker();
        tracker.registerTimestampForTracking(shortName, () -> 1L);

        tracker.close();
        tracker.close();
        assertThat(AtlasDbMetrics.getMetricRegistry().getNames())
                .doesNotContain(buildFullyQualifiedMetricName(shortName));

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
