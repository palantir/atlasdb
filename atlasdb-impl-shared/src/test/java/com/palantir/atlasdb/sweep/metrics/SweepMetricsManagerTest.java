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
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.util.CurrentValueMetric;

public class SweepMetricsManagerTest {
    private static final long EXAMINED = 15L;
    private static final long DELETED = 10L;
    private static final long TIME_SWEEPING = 100L;
    private static final long START_TIME = 100_000L;

    private static final List<Long> VALUES = ImmutableList.of(EXAMINED, DELETED, TIME_SWEEPING);

    private static final long OTHER_EXAMINED = 4L;
    private static final long OTHER_DELETED = 12L;
    private static final long OTHER_TIME_SWEEPING = 200L;
    private static final long OTHER_START_TIME = 1_000_000L;

    private static final List<Long> OTHER_VALUES = ImmutableList.of(OTHER_EXAMINED, OTHER_DELETED, OTHER_TIME_SWEEPING);

    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("sweep.test");
    private static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("sweep.test2");

    private static final String CELLS_EXAMINED = AtlasDbMetricNames.CELLS_EXAMINED;
    private static final String CELLS_SWEPT = AtlasDbMetricNames.CELLS_SWEPT;
    private static final String TIME_SPENT_SWEEPING = AtlasDbMetricNames.TIME_SPENT_SWEEPING;
    private static final String TABLE_BEING_SWEPT = AtlasDbMetricNames.TABLE_BEING_SWEPT;

    private static final List<String> CURRENT_VALUE_LONG_METRIC_NAMES = ImmutableList.of(
            CELLS_EXAMINED,
            CELLS_SWEPT,
            TIME_SPENT_SWEEPING);

    private static final String TABLE_FULLY_QUALIFIED = TABLE_REF.getQualifiedName();
    private static final String TABLE2_FULLY_QUALIFIED = TABLE_REF2.getQualifiedName();
    private static final String UNSAFE_FULLY_QUALIFIED = LoggingArgs.PLACEHOLDER_TABLE_REFERENCE.getQualifiedName();


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

    private static final byte[] SAFE_METADATA = createTableMetadataWithLogSafety(
            TableMetadataPersistence.LogSafety.SAFE).persistToBytes();

    private static final byte[] UNSAFE_METADATA = createTableMetadataWithLogSafety(
            TableMetadataPersistence.LogSafety.UNSAFE).persistToBytes();

    private MetricRegistry metricRegistry;

    private SweepMetricsManager sweepMetricsManager;

    @Before
    public void setUp() {
        metricRegistry = new MetricRegistry();
        sweepMetricsManager = new SweepMetricsManager(new SweepMetricsFactory(metricRegistry));
    }

    @Test
    public void allGaugesAreSetForSafeTables() {
        setLoggingSafety(ImmutableMap.of(TABLE_REF, SAFE_METADATA, TABLE_REF2, SAFE_METADATA));
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF);

        assertRecordedExaminedDeletedTimeTableName(VALUES, TABLE_FULLY_QUALIFIED);

        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2);

        assertRecordedExaminedDeletedTimeTableName(OTHER_VALUES, TABLE2_FULLY_QUALIFIED);
    }

    @Test
    public void allGaugesAreSetForUnsafeTables() {
        setLoggingSafety(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA, TABLE_REF2, UNSAFE_METADATA));
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF);

        assertRecordedExaminedDeletedTimeTableName(VALUES, UNSAFE_FULLY_QUALIFIED);

        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2);

        assertRecordedExaminedDeletedTimeTableName(OTHER_VALUES, UNSAFE_FULLY_QUALIFIED);
    }

    @Test
    public void allGaugesAreSetForUnknownSafetyAsUnsafe() {
        setLoggingSafety(ImmutableMap.of());
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF);

        assertRecordedExaminedDeletedTimeTableName(VALUES, UNSAFE_FULLY_QUALIFIED);

        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2);

        assertRecordedExaminedDeletedTimeTableName(OTHER_VALUES, UNSAFE_FULLY_QUALIFIED);
    }

    @Test
    public void gaugesAreUpdatedAfterDeleteBatchCorrectly() {
        setLoggingSafety(ImmutableMap.of(TABLE_REF, SAFE_METADATA, TABLE_REF2, SAFE_METADATA));
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF);
        sweepMetricsManager.updateAfterDeleteBatch(100L, 50L);
        sweepMetricsManager.updateAfterDeleteBatch(10L, 5L);

        assertRecordedExaminedDeletedTimeTableName(
                ImmutableList.of(EXAMINED + 110L, DELETED + 55L, TIME_SWEEPING),
                TABLE_FULLY_QUALIFIED);
    }

    @Test
    public void updateMetricsResetsUpdateAfterDeleteBatch() {
        setLoggingSafety(ImmutableMap.of(TABLE_REF, SAFE_METADATA, TABLE_REF2, SAFE_METADATA));
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF);
        sweepMetricsManager.updateAfterDeleteBatch(100L, 50L);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2);

        assertRecordedExaminedDeletedTimeTableName(OTHER_VALUES, TABLE2_FULLY_QUALIFIED);
    }

    @Test
    public void timeElapsedGaugeIsSetToNewestValueForOneIteration() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF);
        assertSweepTimeElapsedCurrentValueWithinMarginOfError(START_TIME);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2);
        assertSweepTimeElapsedCurrentValueWithinMarginOfError(OTHER_START_TIME);
    }

    @Test
    public void testSweepError() {
        sweepMetricsManager.sweepError();
        sweepMetricsManager.sweepError();

        assertThat(getMeter(AtlasDbMetricNames.SWEEP_ERROR).getCount(), equalTo(2L));
    }

    private void setLoggingSafety(Map<TableReference, byte[]> args) {
        LoggingArgs.hydrate(args);
    }

    private void assertRecordedExaminedDeletedTimeTableName(List<Long> values, String name) {
        for (int index = 0; index < 3; index++) {
            Gauge<Long> gauge = getCurrentValueMetric(CURRENT_VALUE_LONG_METRIC_NAMES.get(index));
            assertThat(gauge.getValue(), equalTo(values.get(index)));
        }
        Gauge<String> gauge = getCurrentValueMetric(TABLE_BEING_SWEPT);
        assertThat(gauge.getValue(), equalTo(name));
    }

    private void assertSweepTimeElapsedCurrentValueWithinMarginOfError(long timeSweepStarted) {
        Gauge<Long> gauge = getCurrentValueMetric(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
        assertWithinErrorMarginOf(gauge.getValue(), System.currentTimeMillis() - timeSweepStarted);
    }

    private Meter getMeter(String namePrefix) {
        return metricRegistry.meter(MetricRegistry.name(SweepMetric.class, namePrefix));
    }

    private <T> Gauge<T> getCurrentValueMetric(String namePrefix) {
        return metricRegistry.gauge(MetricRegistry.name(SweepMetric.class, namePrefix), CurrentValueMetric::new);
    }

    private void assertWithinErrorMarginOf(long actual, long expected) {
        assertThat(actual, greaterThan((long) (expected * .95)));
        assertThat(actual, lessThanOrEqualTo((long) (expected * 1.05)));
    }

    private static TableMetadata createTableMetadataWithLogSafety(TableMetadataPersistence.LogSafety safety) {
        return new TableMetadata(
                new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                TableMetadataPersistence.CachePriority.WARM,
                false,
                0,
                false,
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                false,
                safety);
    }
}
