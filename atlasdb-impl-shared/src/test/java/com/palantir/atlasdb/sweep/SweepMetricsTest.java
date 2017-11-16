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
package com.palantir.atlasdb.sweep;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
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
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricsTest {
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

    private SweepMetrics sweepMetrics;

    @Before
    public void setUp() {
        sweepMetrics = new SweepMetrics();
        taggedMetricRegistry = AtlasDbMetrics.getTaggedMetricRegistry();
    }

    @After
    public void tearDown() {
        AtlasDbMetrics.setMetricRegistries(AtlasDbMetrics.getMetricRegistry(),
                new DefaultTaggedMetricRegistry());
    }

    @Test
    public void allHistogramsAreRecordedForOneIteration() {
        sweepMetrics.updateMetricsOneIteration(SWEEP_RESULTS);

        assertValuesRecordedInHistogramNonTagged("cellTimestampPairsExamined", EXAMINED);
        assertValuesRecordedInHistogramNonTagged("staleValuesDeleted", DELETED);
        assertValuesRecordedInHistogramNonTagged("sweepTimeSweeping", TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorNonTagged(START_TIME);
    }

    @Test
    public void allMetersExceptTimeElapsedAreRecordedForOneIteration() {
        sweepMetrics.updateMetricsOneIteration(SWEEP_RESULTS);

        assertValuesRecordedInMeterNonTagged("cellTimestampPairsExamined", EXAMINED);
        assertValuesRecordedInMeterNonTagged("staleValuesDeleted", DELETED);
        assertValuesRecordedInMeterNonTagged("sweepTimeSweeping", TIME_SWEEPING);
        assertValuesRecordedInMeterNonTagged("sweepTimeElapsedSinceStart");
    }

    @Test
    public void timeElapsedGaugeIsRecordedForOneIteration() {
        sweepMetrics.updateMetricsOneIteration(SWEEP_RESULTS);

        assertSweepTimeElapsedGaugeWithinMarginOfError(START_TIME);
    }

    @Test
    public void allHistogramsAreRecordedForSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, true, EXAMINED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true, DELETED);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, true, TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorTagged(TABLE_REF, true, START_TIME);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, false);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, false);
        assertValuesRecordedTagged("sweepTimeElapsedSinceStart", TABLE_REF, false);
    }

    @Test
    public void timeElapsedMeterIsRecordedForSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);

        assertSweepTimeElapsedWithinMarginOfErrorInMeterTagged(TABLE_REF, START_TIME);
    }

    @Test
    public void allHistogramsAreRecordedForUnSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA));
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, true);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, true);
        assertValuesRecordedTagged("sweepTimeElapsedSinceStart", TABLE_REF, true);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, false, EXAMINED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false, DELETED);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, false, TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorTagged(TABLE_REF, false, START_TIME);
    }

    @Test
    public void allHistogramsAreRecordedAsUnsafeIfMetadataUnavailable() {
        LoggingArgs.hydrate(ImmutableMap.of());
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, true);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, true);
        assertValuesRecordedTagged("sweepTimeElapsedSinceStart", TABLE_REF, true);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, false, EXAMINED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false, DELETED);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, false, TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorTagged(TABLE_REF, false, START_TIME);
    }

    @Test
    public void allHistogramsForOneIterationAreAggregated() {
        sweepMetrics.updateMetricsOneIteration(SWEEP_RESULTS);
        sweepMetrics.updateMetricsOneIteration(OTHER_SWEEP_RESULTS);

        assertValuesRecordedInHistogramNonTagged("cellTimestampPairsExamined", EXAMINED, OTHER_EXAMINED);
        assertValuesRecordedInHistogramNonTagged("staleValuesDeleted", DELETED, OTHER_DELETED);
        assertValuesRecordedInHistogramNonTagged("sweepTimeSweeping", TIME_SWEEPING, OTHER_TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorNonTagged(START_TIME, OTHER_START_TIME);
    }

    @Test
    public void allMetersExceptTimeElapsedForOneIterationAreAggregated() {
        sweepMetrics.updateMetricsOneIteration(SWEEP_RESULTS);
        sweepMetrics.updateMetricsOneIteration(OTHER_SWEEP_RESULTS);

        assertValuesRecordedInMeterNonTagged("cellTimestampPairsExamined", EXAMINED, OTHER_EXAMINED);
        assertValuesRecordedInMeterNonTagged("staleValuesDeleted", DELETED, OTHER_DELETED);
        assertValuesRecordedInMeterNonTagged("sweepTimeSweeping", TIME_SWEEPING, OTHER_TIME_SWEEPING);
        assertValuesRecordedInMeterNonTagged("sweepTimeElapsedSinceStart");
    }

    @Test
    public void timeElapsedGaugeIsUpdatedToNewestValueForOneIteration() {
        sweepMetrics.updateMetricsOneIteration(SWEEP_RESULTS);
        sweepMetrics.updateMetricsOneIteration(OTHER_SWEEP_RESULTS);

        assertSweepTimeElapsedGaugeWithinMarginOfError(OTHER_START_TIME);
    }

    @Test
    public void allHistogramsAreAggregatedForSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);
        sweepMetrics.updateMetricsFullTable(OTHER_SWEEP_RESULTS, TABLE_REF);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, true, EXAMINED, OTHER_EXAMINED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true, DELETED, OTHER_DELETED);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, true, TIME_SWEEPING, OTHER_TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorTagged(TABLE_REF, true, START_TIME, OTHER_START_TIME);
    }

    @Test
    public void timeElapsedMeterIsAggregatedForSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);
        sweepMetrics.updateMetricsFullTable(OTHER_SWEEP_RESULTS, TABLE_REF);

        assertSweepTimeElapsedWithinMarginOfErrorInMeterTagged(TABLE_REF, START_TIME, OTHER_START_TIME);
    }

    // todo(gmaretic): this is not a "feature" but fix is not trivial. The test is for documentation purposes
    @Test
    public void allHistogramsAreAggregatedForAllUnSafeTables() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA, TABLE_REF2, UNSAFE_METADATA));
        sweepMetrics.updateMetricsFullTable(SWEEP_RESULTS, TABLE_REF);
        sweepMetrics.updateMetricsFullTable(OTHER_SWEEP_RESULTS, TABLE_REF2);

        assertValuesRecordedTagged("cellTimestampPairsExamined", TABLE_REF, false, EXAMINED, OTHER_EXAMINED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false, DELETED, OTHER_DELETED);
        assertValuesRecordedTagged("sweepTimeSweeping", TABLE_REF, false, TIME_SWEEPING, OTHER_TIME_SWEEPING);
        assertSweepTimeElapsedWithinMarginOfErrorTagged(TABLE_REF, false, START_TIME, OTHER_START_TIME);
    }

    private void assertValuesRecordedInHistogramNonTagged(String aggregateMetric, Long... values) {
        Histogram histogram = getHistogram(aggregateMetric, Optional.empty(), false);
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertValuesRecordedInMeterNonTagged(String aggregateMetric, Long... values) {
        Meter meter = getMeter(aggregateMetric, Optional.empty(), false);
        assertThat(meter.getCount(), equalTo(Arrays.asList(values).stream().reduce(0L, Long::sum)));
    }

    private void assertValuesRecordedTagged(String aggregateMetric, TableReference tableRef, boolean safe,
            Long... values) {
        Histogram histogram = getHistogram(aggregateMetric, Optional.of(tableRef), safe);
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertSweepTimeElapsedWithinMarginOfErrorNonTagged(Long... timeSweepStarted) {
        Histogram histogram = getHistogram("sweepTimeElapsedSinceStart", Optional.empty(), false);
        assertWithinMarginOfError(histogram, Arrays.asList(timeSweepStarted));
    }

    private void assertSweepTimeElapsedWithinMarginOfErrorTagged(TableReference tableRef,
            boolean safe, Long... timeSweepStarted) {
        Histogram histogram = getHistogram("sweepTimeElapsedSinceStart", Optional.of(tableRef), safe);
        assertWithinMarginOfError(histogram, Arrays.asList(timeSweepStarted));
    }

    private void assertSweepTimeElapsedWithinMarginOfErrorInMeterTagged(TableReference tableRef,
            Long... timeSweepStarted) {
        Meter meter = getMeter("sweepTimeElapsedSinceStart", Optional.of(tableRef), true);
        long totalDelta = Arrays.asList(timeSweepStarted).stream()
                .reduce(0L, (fst, snd) -> fst + System.currentTimeMillis() - snd);
        assertWithinErrorMarginOf(meter.getCount(), totalDelta);
    }

    private void assertSweepTimeElapsedGaugeWithinMarginOfError(long timeSweepStarted) {
        Gauge<Long> gauge = taggedMetricRegistry.gauge(
                getMetricName("sweepTimeElapsedSinceStart" + "G", Optional.empty(), false), new SweepMetrics.Current());
        assertWithinErrorMarginOf(gauge.getValue(), System.currentTimeMillis() - timeSweepStarted);
    }

    private Histogram getHistogram(String name, Optional<TableReference> tableRef, boolean safe) {
        return taggedMetricRegistry.histogram(getMetricName(name + "H", tableRef, safe));
    }

    private Meter getMeter(String name, Optional<TableReference> tableRef, boolean safe) {
        return taggedMetricRegistry.meter(getMetricName(name + "M", tableRef, safe));
    }

    private MetricName getMetricName(String name, Optional<TableReference> tableRef, boolean safe) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(tableRef
                        .map(tableReference -> safe
                                ? ImmutableMap.of("tableRef", tableReference.toString())
                                : ImmutableMap.of("unsafeTableRef", "unsafe"))
                        .orElse(ImmutableMap.of()))
                .build();
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
