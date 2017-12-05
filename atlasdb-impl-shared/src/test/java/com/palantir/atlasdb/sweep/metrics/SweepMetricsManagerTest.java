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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
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
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricsManagerTest {
    private static final long DELETED = 10L;
    private static final long EXAMINED = 15L;
    private static final long TIME_SWEEPING = 100L;
    private static final long START_TIME = 100_000L;

    private static final long OTHER_DELETED = 12L;
    private static final long OTHER_EXAMINED = 4L;
    private static final long OTHER_TIME_SWEEPING = 200L;
    private static final long OTHER_START_TIME = 100_000_000L;

    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("sweep.test");
    private static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("sweep.test2");
    private static final TableReference DUMMY = TableReference.createWithEmptyNamespace("dummy");

    private static final String CELLS_EXAMINED = AtlasDbMetricNames.CELLS_EXAMINED;
    private static final String CELLS_SWEPT = AtlasDbMetricNames.CELLS_SWEPT;
    private static final String TIME_SPENT_SWEEPING = AtlasDbMetricNames.TIME_SPENT_SWEEPING;

    private static final SweepResults SWEEP_RESULTS = ImmutableSweepResults.builder()
            .cellTsPairsExamined(EXAMINED)
            .staleValuesDeleted(DELETED)
            .timeInMillis(TIME_SWEEPING)
            .timeSweepStarted(START_TIME)
            .sweptTimestamp(0L)
            .build();

    private static final SweepResults OTHER_SWEEP_RESULTS = ImmutableSweepResults.builder()
            .cellTsPairsExamined(OTHER_EXAMINED)
            .staleValuesDeleted(OTHER_DELETED)
            .timeInMillis(OTHER_TIME_SWEEPING)
            .timeSweepStarted(OTHER_START_TIME)
            .sweptTimestamp(0L)
            .build();

    private static final byte[] SAFE_METADATA = createTableMetadataWithLogSafety(
            TableMetadataPersistence.LogSafety.SAFE).persistToBytes();

    private static final byte[] UNSAFE_METADATA = createTableMetadataWithLogSafety(
            TableMetadataPersistence.LogSafety.UNSAFE).persistToBytes();

    private static TaggedMetricRegistry taggedMetricRegistry;

    private SweepMetricsManager sweepMetricsManager;

    @Before
    public void setUp() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetricsManager = new SweepMetricsManager();
        taggedMetricRegistry = AtlasDbMetrics.getTaggedMetricRegistry();
    }

    @After
    public void tearDown() {
        AtlasDbMetrics.setMetricRegistries(AtlasDbMetrics.getMetricRegistry(),
                new DefaultTaggedMetricRegistry());
    }

    @Test
    public void allTaggedHistogramsAreRecordedForOneIterationAndSafeTable() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);

        assertRecordedHistogramTaggedSafeOneIteration(CELLS_EXAMINED, TABLE_REF, EXAMINED);
        assertRecordedHistogramTaggedSafeOneIteration(CELLS_SWEPT, TABLE_REF, DELETED);
        assertRecordedHistogramTaggedSafeOneIteration(TIME_SPENT_SWEEPING, TABLE_REF, TIME_SWEEPING);
    }

    @Test
    public void allTaggedHistogramsAreRecordedForOneIterationAndUnSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA));
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);

        assertRecordedHistogramTaggedUnSafeOneIteration(CELLS_EXAMINED, EXAMINED);
        assertRecordedHistogramTaggedUnSafeOneIteration(CELLS_SWEPT, DELETED);
        assertRecordedHistogramTaggedUnSafeOneIteration(TIME_SPENT_SWEEPING, TIME_SWEEPING);
    }

    @Test
    public void allTaggedHistogramsAreRecordedAsUnsafeIfMetadataUnavailable() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF2, UpdateEvent.ONE_ITERATION);

        assertRecordedHistogramTaggedUnSafeOneIteration(CELLS_EXAMINED, EXAMINED);
        assertRecordedHistogramTaggedUnSafeOneIteration(CELLS_SWEPT, DELETED);
        assertRecordedHistogramTaggedUnSafeOneIteration(TIME_SPENT_SWEEPING, TIME_SWEEPING);
    }

    @Test
    public void allMetersAreRecordedForOneIteration() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);

        assertRecordedMeterNonTaggedOneIteration(CELLS_EXAMINED, EXAMINED);
        assertRecordedMeterNonTaggedOneIteration(CELLS_SWEPT, DELETED);
        assertRecordedMeterNonTaggedOneIteration(TIME_SPENT_SWEEPING, TIME_SWEEPING);
    }

    @Test
    public void timeElapsedCurrentValueIsRecordedForOneIteration() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);

        assertSweepTimeElapsedCurrentValueWithinMarginOfError(START_TIME);
    }

    @Test
    public void allNonTaggedHistogramsAreRecordedForFulTable() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.FULL_TABLE);

        assertRecordedHistogramNonTaggedFullTable(CELLS_EXAMINED, TABLE_REF, EXAMINED);
        assertRecordedHistogramNonTaggedFullTable(CELLS_SWEPT, TABLE_REF, DELETED);
        assertRecordedHistogramNonTaggedFullTable(TIME_SPENT_SWEEPING, TABLE_REF, TIME_SWEEPING);
        assertSweepTimeElapsedHistogramWithinMarginOfError(START_TIME);
    }

    @Test
    public void allTaggedHistogramsAreAggregatedForSafeTable() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);

        assertRecordedHistogramTaggedSafeOneIteration(CELLS_EXAMINED, TABLE_REF, EXAMINED, OTHER_EXAMINED);
        assertRecordedHistogramTaggedSafeOneIteration(CELLS_SWEPT, TABLE_REF, DELETED, OTHER_DELETED);
        assertRecordedHistogramTaggedSafeOneIteration(
                TIME_SPENT_SWEEPING, TABLE_REF, TIME_SWEEPING, OTHER_TIME_SWEEPING);
    }

    @Test
    public void allTaggedHistogramsAreAggregatedForAllUnSafeTables() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA, TABLE_REF2, UNSAFE_METADATA));
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2, UpdateEvent.ONE_ITERATION);

        assertRecordedHistogramTaggedUnSafeOneIteration(CELLS_EXAMINED, EXAMINED, OTHER_EXAMINED);
        assertRecordedHistogramTaggedUnSafeOneIteration(CELLS_SWEPT, DELETED, OTHER_DELETED);
        assertRecordedHistogramTaggedUnSafeOneIteration(TIME_SPENT_SWEEPING, TIME_SWEEPING, OTHER_TIME_SWEEPING);
    }

    @Test
    public void allMetersAreAggregated() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2, UpdateEvent.ONE_ITERATION);

        assertRecordedMeterNonTaggedOneIteration(CELLS_EXAMINED, EXAMINED, OTHER_EXAMINED);
        assertRecordedMeterNonTaggedOneIteration(CELLS_SWEPT, DELETED, OTHER_DELETED);
        assertRecordedMeterNonTaggedOneIteration(TIME_SPENT_SWEEPING, TIME_SWEEPING, OTHER_TIME_SWEEPING);
    }

    @Test
    public void timeElapsedGaugeIsUpdatedToNewestValueForOneIteration() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2, UpdateEvent.ONE_ITERATION);

        assertSweepTimeElapsedCurrentValueWithinMarginOfError(OTHER_START_TIME);
    }

    @Test
    public void allNonTaggedHistogramsAreAggregatedForFulTable() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.FULL_TABLE);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF2, UpdateEvent.FULL_TABLE);

        assertRecordedHistogramNonTaggedFullTable(CELLS_EXAMINED, TABLE_REF, EXAMINED, OTHER_EXAMINED);
        assertRecordedHistogramNonTaggedFullTable(CELLS_SWEPT, TABLE_REF, DELETED, OTHER_DELETED);
        assertRecordedHistogramNonTaggedFullTable(TIME_SPENT_SWEEPING, TABLE_REF, TIME_SWEEPING, OTHER_TIME_SWEEPING);
        assertSweepTimeElapsedHistogramWithinMarginOfError(START_TIME, OTHER_START_TIME);
    }

    @Test
    public void noCrossContaminationOfMetrics() {
        sweepMetricsManager.updateMetrics(SWEEP_RESULTS, TABLE_REF, UpdateEvent.ONE_ITERATION);
        sweepMetricsManager.updateMetrics(OTHER_SWEEP_RESULTS, TABLE_REF, UpdateEvent.FULL_TABLE);

        assertRecordedHistogramTaggedSafeOneIteration(CELLS_EXAMINED, TABLE_REF, EXAMINED);
        assertRecordedHistogramTaggedSafeOneIteration(CELLS_SWEPT, TABLE_REF, DELETED);
        assertRecordedHistogramTaggedSafeOneIteration(TIME_SPENT_SWEEPING, TABLE_REF, TIME_SWEEPING);
        assertRecordedMeterNonTaggedOneIteration(CELLS_EXAMINED, EXAMINED);
        assertRecordedMeterNonTaggedOneIteration(CELLS_SWEPT, DELETED);
        assertRecordedMeterNonTaggedOneIteration(TIME_SPENT_SWEEPING, TIME_SWEEPING);
        assertSweepTimeElapsedCurrentValueWithinMarginOfError(START_TIME);

        assertRecordedHistogramNonTaggedFullTable(CELLS_EXAMINED, TABLE_REF, OTHER_EXAMINED);
        assertRecordedHistogramNonTaggedFullTable(CELLS_SWEPT, TABLE_REF, OTHER_DELETED);
        assertRecordedHistogramNonTaggedFullTable(TIME_SPENT_SWEEPING, TABLE_REF, OTHER_TIME_SWEEPING);
        assertSweepTimeElapsedHistogramWithinMarginOfError(OTHER_START_TIME);
    }

    @Test
    public void testSweepError() {
        sweepMetricsManager.sweepError();
        sweepMetricsManager.sweepError();

        Meter meter = getMeter(AtlasDbMetricNames.SWEEP_ERROR, UpdateEvent.ERROR);
        assertThat(meter.getCount(), equalTo(2L));
    }

    private void assertRecordedHistogramTaggedSafeOneIteration(String name, TableReference tableRef, Long... values) {
        Histogram histogram = getHistogram(name, tableRef, UpdateEvent.ONE_ITERATION, true);
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertRecordedHistogramTaggedUnSafeOneIteration(String name, Long... values) {
        Histogram histogram =
                getHistogram(name, LoggingArgs.PLACEHOLDER_TABLE_REFERENCE, UpdateEvent.ONE_ITERATION, true);
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertRecordedHistogramNonTaggedFullTable(String name, TableReference tableRef, Long... values) {
        Histogram histogram = getHistogram(name, tableRef, UpdateEvent.FULL_TABLE, false);
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertRecordedMeterNonTaggedOneIteration(String aggregateMetric, Long... values) {
        Meter meter = getMeter(aggregateMetric, UpdateEvent.ONE_ITERATION);
        assertThat(meter.getCount(), equalTo(Arrays.asList(values).stream().reduce(0L, Long::sum)));
    }

    private void assertSweepTimeElapsedCurrentValueWithinMarginOfError(long timeSweepStarted) {
        Gauge<Long> gauge = getCurrentValueMetric(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
        assertWithinErrorMarginOf(gauge.getValue(), System.currentTimeMillis() - timeSweepStarted);
    }

    private void assertSweepTimeElapsedHistogramWithinMarginOfError(Long... timeSweepStarted) {
        Histogram histogram =
                getHistogram(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING, DUMMY, UpdateEvent.FULL_TABLE, false);
        assertWithinMarginOfError(histogram, Arrays.asList(timeSweepStarted));
    }

    private Histogram getHistogram(String namePrefix, TableReference tableRef, UpdateEvent updateEvent,
            boolean taggedWithTableName) {
        return taggedMetricRegistry.histogram(SweepMetricImpl.getTaggedMetricName(
                namePrefix + SweepMetricAdapter.HISTOGRAM_ADAPTER.getNameSuffix(),
                updateEvent, tableRef, taggedWithTableName));
    }

    private Meter getMeter(String namePrefix, UpdateEvent updateEvent) {
        return taggedMetricRegistry.meter(SweepMetricImpl.getTaggedMetricName(
                namePrefix + SweepMetricAdapter.METER_ADAPTER.getNameSuffix(),
                updateEvent, DUMMY, false));
    }

    private Gauge getCurrentValueMetric(String namePrefix) {
        return taggedMetricRegistry.gauge(SweepMetricImpl.getTaggedMetricName(
                namePrefix + SweepMetricAdapter.CURRENT_VALUE_ADAPTER.getNameSuffix(),
                UpdateEvent.ONE_ITERATION, DUMMY, false), new CurrentValueMetric());
    }

    private void assertWithinMarginOfError(Histogram histogram, List<Long> timesStarted) {
        List<Long> timesRecorded = Longs.asList(histogram.getSnapshot().getValues());
        timesRecorded.sort(Long::compareTo);
        timesStarted.sort((fst, snd) -> Long.compare(snd, fst));
        assertThat(timesRecorded.size(), equalTo(timesStarted.size()));
        for (int i = 0; i < timesRecorded.size(); i++) {
            assertWithinErrorMarginOf(timesRecorded.get(i), System.currentTimeMillis() - timesStarted.get(i));
        }
    }

    private void assertWithinErrorMarginOf(long value, long expected) {
        assertThat(value, greaterThan(expected - 1000L));
        assertThat(value, lessThanOrEqualTo(expected));
    }

    private static TableMetadata createTableMetadataWithLogSafety(TableMetadataPersistence.LogSafety safety) {
        return new TableMetadata(
                new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                TableMetadataPersistence.CachePriority.WARM,
                TableMetadataPersistence.PartitionStrategy.ORDERED,
                false,
                0,
                false,
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                TableMetadataPersistence.ExpirationStrategy.NEVER,
                false,
                safety);
    }
}
