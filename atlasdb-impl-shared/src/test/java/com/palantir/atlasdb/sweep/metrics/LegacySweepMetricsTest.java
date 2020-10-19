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
package com.palantir.atlasdb.sweep.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class LegacySweepMetricsTest {
    private static final long EXAMINED = 15L;
    private static final long DELETED = 10L;
    private static final long TIME_SWEEPING = 100L;
    private static final long START_TIME = 100_000L;

    private static final long OTHER_EXAMINED = 4L;
    private static final long OTHER_DELETED = 12L;
    private static final long OTHER_TIME_SWEEPING = 200L;
    private static final long OTHER_START_TIME = 1_000_000L;

    private static final String CELLS_EXAMINED = AtlasDbMetricNames.CELLS_EXAMINED;
    private static final String CELLS_SWEPT = AtlasDbMetricNames.CELLS_SWEPT;
    private static final String TIME_SPENT_SWEEPING = AtlasDbMetricNames.TIME_SPENT_SWEEPING;

    private static final List<String> CURRENT_VALUE_LONG_METRIC_NAMES = ImmutableList.of(
            CELLS_EXAMINED,
            CELLS_SWEPT,
            TIME_SPENT_SWEEPING);

    private static final SweepResults SWEEP_RESULTS = SweepResults.builder()
            .cellTsPairsExamined(EXAMINED)
            .staleValuesDeleted(DELETED)
            .timeInMillis(TIME_SWEEPING)
            .timeSweepStarted(START_TIME)
            .minSweptTimestamp(0L)
            .build();

    private static final SweepResults OTHER_SWEEP_RESULTS = SweepResults.builder()
            .cellTsPairsExamined(OTHER_EXAMINED)
            .staleValuesDeleted(OTHER_DELETED)
            .timeInMillis(OTHER_TIME_SWEEPING)
            .timeSweepStarted(OTHER_START_TIME)
            .minSweptTimestamp(0L)
            .build();

    private final MetricRegistry metricRegistry = new MetricRegistry();

    private LegacySweepMetrics sweepMetrics;

    @Before
    public void setUp() {
        sweepMetrics = new LegacySweepMetrics(metricRegistry);
        assertThat(metricRegistry.getMetrics()).isEmpty();
    }

    @Test
    public void allMetricsAreSet() {
        sweepMetrics.updateSweepTime(
                SWEEP_RESULTS.getTimeInMillis(),
                SWEEP_RESULTS.getTimeElapsedSinceStartedSweeping());
        sweepMetrics.updateCellsExaminedDeleted(EXAMINED, DELETED);

        assertRecordedExaminedDeletedTime(
                ImmutableList.of(EXAMINED, DELETED, TIME_SWEEPING)
        );

        sweepMetrics.updateSweepTime(
                OTHER_SWEEP_RESULTS.getTimeInMillis(),
                OTHER_SWEEP_RESULTS.getTimeElapsedSinceStartedSweeping());
        sweepMetrics.updateCellsExaminedDeleted(OTHER_EXAMINED, OTHER_DELETED);

        assertRecordedExaminedDeletedTime(
                ImmutableList.of(EXAMINED + OTHER_EXAMINED,
                DELETED + OTHER_DELETED,
                TIME_SWEEPING + OTHER_TIME_SWEEPING)
        );
    }

    @Test
    public void timeElapsedMeterIsSet() {
        sweepMetrics.updateSweepTime(
                SWEEP_RESULTS.getTimeInMillis(),
                SWEEP_RESULTS.getTimeElapsedSinceStartedSweeping());
        assertSweepTimeElapsedCurrentValueWithinMarginOfError(START_TIME);

        sweepMetrics.updateSweepTime(
                OTHER_SWEEP_RESULTS.getTimeInMillis(),
                OTHER_SWEEP_RESULTS.getTimeElapsedSinceStartedSweeping());
        assertSweepTimeElapsedCurrentValueWithinMarginOfError(OTHER_START_TIME);
    }

    @Test
    public void testSweepError() {
        sweepMetrics.sweepError();
        sweepMetrics.sweepError();

        assertThat(getCounter(AtlasDbMetricNames.SWEEP_ERROR).getCount()).isEqualTo(2L);
    }

    private void assertRecordedExaminedDeletedTime(List<Long> values) {
        for (int index = 0; index < 3; index++) {
            Counter counter = getCounter(CURRENT_VALUE_LONG_METRIC_NAMES.get(index));
            assertThat(counter.getCount()).isEqualTo(values.get(index));
        }
    }

    private void assertSweepTimeElapsedCurrentValueWithinMarginOfError(long timeSweepStarted) {
        Gauge<Long> gauge = getGauge(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
        assertWithinErrorMarginOf(gauge.getValue(), System.currentTimeMillis() - timeSweepStarted);
    }

    private Counter getCounter(String namePrefix) {
        return metricRegistry.counter(MetricRegistry.name(LegacySweepMetrics.METRIC_BASE_NAME, namePrefix));
    }

    private Gauge<Long> getGauge(String namePrefix) {
        return metricRegistry.getGauges().get(MetricRegistry.name(LegacySweepMetrics.METRIC_BASE_NAME, namePrefix));
    }

    private void assertWithinErrorMarginOf(long actual, long expected) {
        assertThat(actual).isGreaterThan((long) (expected * .95));
        assertThat(actual).isLessThanOrEqualTo((long) (expected * 1.05));
    }
}
