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
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
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

    private static final long OTHER_DELETED = 12L;
    private static final long OTHER_EXAMINED = 4L;

    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("sweep.test");
    private static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("sweep.test2");

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
    public void cellsDeletedAreRecorded() {
        sweepMetrics.examinedCellsOneIteration(EXAMINED);
        sweepMetrics.deletedCellsOneIteration(DELETED);

        assertValuesRecordedNonTagged("staleValuesDeleted", DELETED);
    }

    @Test
    public void cellsDeletedAreRecordedForSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetrics.examinedCellsFullTable(EXAMINED, TABLE_REF);
        sweepMetrics.deletedCellsFullTable(DELETED, TABLE_REF);

        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true, DELETED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false);
    }

    @Test
    public void cellsDeletedAreRecordedForUnSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA));
        sweepMetrics.examinedCellsFullTable(EXAMINED, TABLE_REF);
        sweepMetrics.deletedCellsFullTable(DELETED, TABLE_REF);

        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false, DELETED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true);
    }

    @Test
    public void cellsDeletedAreRecordedAsUnsafeIfMetadataUnavailable() {
        LoggingArgs.hydrate(ImmutableMap.of());
        sweepMetrics.examinedCellsFullTable(EXAMINED, TABLE_REF);
        sweepMetrics.deletedCellsFullTable(DELETED, TABLE_REF);

        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false, DELETED);
        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true);
    }

    @Test
    public void cellsDeletedAreAggregated() {
        sweepMetrics.examinedCellsOneIteration(EXAMINED);
        sweepMetrics.deletedCellsOneIteration(DELETED);

        sweepMetrics.examinedCellsOneIteration(OTHER_EXAMINED);
        sweepMetrics.deletedCellsOneIteration(OTHER_DELETED);

        assertValuesRecordedNonTagged("staleValuesDeleted", DELETED, OTHER_DELETED);
    }

    @Test
    public void cellsDeletedAreAggregatedForSafeTable() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, SAFE_METADATA));
        sweepMetrics.examinedCellsFullTable(EXAMINED, TABLE_REF);
        sweepMetrics.deletedCellsFullTable(DELETED, TABLE_REF);

        sweepMetrics.examinedCellsFullTable(OTHER_EXAMINED, TABLE_REF);
        sweepMetrics.deletedCellsFullTable(OTHER_DELETED, TABLE_REF);

        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, true, DELETED, OTHER_DELETED);
    }

    // todo(gmaretic): this is not a "feature" but fix is not trivial
    @Test
    public void cellsDeletedAreAggregatedForAllUnSafeTables() {
        LoggingArgs.hydrate(ImmutableMap.of(TABLE_REF, UNSAFE_METADATA, TABLE_REF2, UNSAFE_METADATA));
        sweepMetrics.examinedCellsFullTable(EXAMINED, TABLE_REF);
        sweepMetrics.deletedCellsFullTable(DELETED, TABLE_REF);

        sweepMetrics.examinedCellsFullTable(OTHER_EXAMINED, TABLE_REF2);
        sweepMetrics.deletedCellsFullTable(OTHER_DELETED, TABLE_REF2);

        assertValuesRecordedTagged("staleValuesDeleted", TABLE_REF, false, DELETED, OTHER_DELETED);
    }

    @Test
    public void cellsExaminedAreRecorded() {
        sweepMetrics.examinedCellsOneIteration(EXAMINED);
        sweepMetrics.deletedCellsOneIteration(DELETED);

        assertValuesRecordedNonTagged("cellTimestampPairsExamined", EXAMINED);
    }

    @Test
    public void cellsExaminedAreAggregated() {
        sweepMetrics.examinedCellsOneIteration(EXAMINED);
        sweepMetrics.deletedCellsOneIteration(DELETED);

        sweepMetrics.examinedCellsOneIteration(OTHER_EXAMINED);
        sweepMetrics.deletedCellsOneIteration(OTHER_DELETED);

        assertValuesRecordedNonTagged("cellTimestampPairsExamined", EXAMINED, OTHER_EXAMINED);
    }

    private void assertValuesRecordedNonTagged(String aggregateMetric, Long... values) {
        Histogram histogram = taggedMetricRegistry.histogram(SweepMetrics.getNonTaggedMetric(aggregateMetric + "H"));
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private void assertValuesRecordedTagged(String aggregateMetric, TableReference tableRef, boolean safe,
            Long... values) {
        Histogram histogram = taggedMetricRegistry
                .histogram(getMetricName(aggregateMetric + "H", tableRef, safe));
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }

    private MetricName getMetricName(String name, TableReference tableRef, boolean safe) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetrics.class, name))
                .safeTags(safe
                        ? ImmutableMap.of("tableRef", tableRef.toString())
                        : ImmutableMap.of("unsafeTableRef", "unsafe"))
                .build();
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
