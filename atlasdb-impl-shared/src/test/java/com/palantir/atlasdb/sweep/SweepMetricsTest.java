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
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricsTest {
    private static final long DELETED = 10L;
    private static final long EXAMINED = 15L;

    private static final long OTHER_DELETED = 12L;
    private static final long OTHER_EXAMINED = 4L;

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
    public void cellsDeletedAreAggregated() {
        sweepMetrics.examinedCellsOneIteration(EXAMINED);
        sweepMetrics.deletedCellsOneIteration(DELETED);

        sweepMetrics.examinedCellsOneIteration(OTHER_EXAMINED);
        sweepMetrics.deletedCellsOneIteration(OTHER_DELETED);

        assertValuesRecordedNonTagged("staleValuesDeleted", DELETED, OTHER_DELETED);
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

    // todo(gmaretic): add tagged metrics tests

    private void assertValuesRecordedNonTagged(String aggregateMetric, Long... values) {
        Histogram histogram = taggedMetricRegistry.histogram(SweepMetrics.getNonTaggedMetric(aggregateMetric));
        assertThat(Longs.asList(histogram.getSnapshot().getValues()), containsInAnyOrder(values));
    }
}
