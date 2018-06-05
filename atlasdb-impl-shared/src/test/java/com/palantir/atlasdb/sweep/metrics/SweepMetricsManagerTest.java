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
package com.palantir.atlasdb.sweep.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class SweepMetricsManagerTest {
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

    private static final SweepResults SWEEP_RESULTS = ImmutableSweepResults.builder()
            .cellTsPairsExamined(EXAMINED)
            .staleValuesDeleted(DELETED)
            .timeInMillis(TIME_SWEEPING)
            .timeSweepStarted(START_TIME)
            .minSweptTimestamp(0L)
            .build();

    private static final SweepResults OTHER_SWEEP_RESULTS = ImmutableSweepResults.builder()
            .cellTsPairsExamined(OTHER_EXAMINED)
            .staleValuesDeleted(OTHER_DELETED)
            .timeInMillis(OTHER_TIME_SWEEPING)
            .timeSweepStarted(OTHER_START_TIME)
            .minSweptTimestamp(0L)
            .build();

    private static MetricRegistry metricRegistry;

    private SweepMetricsManager sweepMetricsManager;

    @Before
    public void setUp() {
        sweepMetricsManager = new SweepMetricsManager();
        metricRegistry = AtlasDbMetrics.getMetricRegistry();
    }

    @After
    public void tearDown() {
        AtlasDbMetrics.setMetricRegistries(new MetricRegistry(),
                new DefaultTaggedMetricRegistry());
    }

    @Test
    public void allMetersAreSet() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS);

        assertRecordedExaminedDeletedTime(
                ImmutableList.of(EXAMINED, DELETED, TIME_SWEEPING)
        );

        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS);

        assertRecordedExaminedDeletedTime(
                ImmutableList.of(EXAMINED + OTHER_EXAMINED,
                DELETED + OTHER_DELETED,
                TIME_SWEEPING + OTHER_TIME_SWEEPING)
        );
    }

    @Test
    public void metersAreUpdatedAfterDeleteBatchCorrectly() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS);
        sweepMetricsManager.updateAfterDeleteBatch(100L, 50L);
        sweepMetricsManager.updateAfterDeleteBatch(10L, 5L);

        assertRecordedExaminedDeletedTime(
                ImmutableList.of(EXAMINED + 110L, DELETED + 55L, TIME_SWEEPING));
    }


    @Test
    public void timeElapsedMeterIsSet() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS);
        assertSweepTimeElapsedCurrentValueWithinMarginOfError(START_TIME);
    }

    @Test
    public void testSweepError() {
        sweepMetricsManager.sweepError();
        sweepMetricsManager.sweepError();

        assertThat(getMeter(AtlasDbMetricNames.SWEEP_ERROR).getCount(), equalTo(2L));
    }

    private void assertRecordedExaminedDeletedTime(List<Long> values) {
        for (int index = 0; index < 3; index++) {
            Meter meter = getMeter(CURRENT_VALUE_LONG_METRIC_NAMES.get(index));
            assertThat(meter.getCount(), equalTo(values.get(index)));
        }
    }

    private void assertSweepTimeElapsedCurrentValueWithinMarginOfError(long timeSweepStarted) {
        Meter meter = getMeter(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
        assertWithinErrorMarginOf(meter.getCount(), System.currentTimeMillis() - timeSweepStarted);
    }

    private Meter getMeter(String namePrefix) {
        return metricRegistry.meter(MetricRegistry.name(SweepMetricsManager.METRIC_BASE_NAME, namePrefix));
    }

    private void assertWithinErrorMarginOf(long actual, long expected) {
        assertThat(actual, greaterThan((long) (expected * .95)));
        assertThat(actual, lessThanOrEqualTo((long) (expected * 1.05)));
    }
}
