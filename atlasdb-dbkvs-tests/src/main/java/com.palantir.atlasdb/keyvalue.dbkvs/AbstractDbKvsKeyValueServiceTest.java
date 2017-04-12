/*
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;
import com.palantir.atlasdb.sweep.EquivalenceCountingIterator;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.common.base.ClosableIterator;

public abstract class AbstractDbKvsKeyValueServiceTest extends AbstractKeyValueServiceTest {
    protected static final Namespace TEST_NAMESPACE = Namespace.create("ns");
    protected static final String TEST_LONG_TABLE_NAME =
            "ThisShouldAlwaysBeAVeryLongTableNameThatExceedsPostgresLengthLimit";

    @After
    public void resetMaxBatch() {
        setMaxRangeOfTimestampsBatchSize(DbKvs.DEFAULT_GET_RANGE_OF_TS_BATCH);
    }

    @Test
    public void dontThrowWhenCreatingTheSameLongTable() throws Exception {
        TableReference longTableName = TableReference.create(TEST_NAMESPACE, TEST_LONG_TABLE_NAME);

        keyValueService.createTable(longTableName, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(longTableName, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.dropTable(longTableName);
    }

    @Test
    public void getRangeOfTimestampsMaxRangeOfTimestampsBatchSizeBatchingTest() {
        setupTestTable();
        setMaxRangeOfTimestampsBatchSize(7);
        try (ClosableIterator<RowResult<Set<Long>>> rowResults = keyValueService.getRangeOfTimestamps(
                TEST_TABLE,
                normalRange(10),
                62)) {
            EquivalenceCountingIterator<RowResult<Set<Long>>> iterator =
                    new EquivalenceCountingIterator<>(rowResults, 10, SweepTaskRunnerImpl.sameRowEquivalence());

            assertRowColumnsTimestamps(iterator.next(), 1, ImmutableSet.of(1, 2, 3, 4),
                    10L, 20L, 21L, 30L, 31L, 32L, 40L, 41L, 42L, 43L);
            assertRowColumnsTimestamps(iterator.next(), 1, ImmutableSet.of(5, 6),
                    50L, 51L, 52L, 53L, 54L, 60L, 61L);
            assertRowColumnsTimestamps(iterator.next(), 2, ImmutableSet.of(2, 3, 4),
                    20L, 21L, 30L, 31L, 32L, 40L, 41L, 42L, 43L);
            assertRowColumnsTimestamps(iterator.next(), 2, ImmutableSet.of(5, 6),
                    50L, 51L, 52L, 53L, 54L, 60L, 61L);
            assertRowColumnsTimestamps(iterator.next(), 3, ImmutableSet.of(3, 4),
                    30L, 31L, 32L, 40L, 41L, 42L, 43L);
            assertRowColumnsTimestamps(iterator.next(), 3, ImmutableSet.of(5, 6),
                    50L, 51L, 52L, 53L, 54L, 60L, 61L);
            assertRowColumnsTimestamps(iterator.next(), 4, ImmutableSet.of(4, 5),
                    40L, 41L, 42L, 43L, 50L, 51L, 52L, 53L, 54L);
            assertRowColumnsTimestamps(iterator.next(), 4, ImmutableSet.of(6),
                    60L, 61L);
            exhaustIterator(iterator);
            assertThat(iterator.size()).isEqualTo(6);
        }
    }

    @Test
    public void getRangeOfTimestampsWithReverseRangeOrdersTimestampsAndRepeatsWhenNecessary() {
        setupTestTable();
        setMaxRangeOfTimestampsBatchSize(8);
        try (ClosableIterator<RowResult<Set<Long>>> rowResults = keyValueService.getRangeOfTimestamps(
                TEST_TABLE,
                reverseRange(5),
                100)) {
            EquivalenceCountingIterator<RowResult<Set<Long>>> iterator =
                    new EquivalenceCountingIterator<>(rowResults, 5, SweepTaskRunnerImpl.sameRowEquivalence());

            assertRowColumnsTimestamps(iterator.next(), 9, ImmutableSet.of(9),
                    90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L, 98L);
            assertRowColumnsTimestamps(iterator.next(), 8, ImmutableSet.of(8),
                    80L, 81L, 82L, 83L, 84L, 85L, 86L, 87L);
            assertRowColumnsTimestamps(iterator.next(), 8, ImmutableSet.of(9),
                    90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L, 98L);
            assertRowColumnsTimestamps(iterator.next(), 7, ImmutableSet.of(7, 8),
                    70L, 71L, 72L, 73L, 74L, 75L, 76L, 80L, 81L, 82L, 83L, 84L, 85L, 86L, 87L);
            exhaustIterator(iterator);
            assertThat(iterator.size()).isEqualTo(5);
            assertRowColumnsTimestamps(iterator.lastItem(), 5, ImmutableSet.of(9),
                    90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L, 98L);
        }
    }

    private void exhaustIterator(EquivalenceCountingIterator<RowResult<Set<Long>>> iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    /**
     * Asserts that the specified RowResult contains the expected entries.
     * @param entry The RowResult to check
     * @param row expected row name (row names are expected to be integers converted to bytes)
     * @param cols expected set of columns in the RowResult (set of integers)
     * @param values expected timestamps as an union of timestamps for all the columns, order-invariant
     */
    private void assertRowColumnsTimestamps(RowResult<Set<Long>> entry, int row, Set<Integer> cols, Long... values) {
        assertThat(entry.getRowName()).isEqualTo(PtBytes.toBytes(row));
        SortedMap<byte[], Set<Long>> columns = entry.getColumns();
        assertThat(columns.keySet()).containsExactlyElementsOf(
                cols.stream().map(PtBytes::toBytes).collect(Collectors.toList()));
        Set<Long> timestamps = new HashSet<>();
        columns.values().stream().forEach(set -> timestamps.addAll(set));
        assertThat(timestamps).containsExactlyInAnyOrder(values);
    }

    private RangeRequest normalRange(int rowBatchSize) {
        return getRangeRequest(false, new byte[0], rowBatchSize);
    }

    private RangeRequest reverseRange(int rowBatchSize) {
        return getRangeRequest(true, PtBytes.toBytes(Long.MAX_VALUE), rowBatchSize);
    }

    private RangeRequest getRangeRequest(boolean reverse, byte[] firstRow, int rowBatchSize) {
        return RangeRequest.builder(reverse)
                .startRowInclusive(firstRow)
                .batchHint(rowBatchSize)
                .build();
    }

    private void setMaxRangeOfTimestampsBatchSize(long value) {
        DbKvsTestUtils.setMaxRangeOfTimestampsBatchSize(value, (ConnectionManagerAwareDbKvs) keyValueService);
    }

    private void setupTestTable() {
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        DbKvsTestUtils.setupTestTable(keyValueService, TEST_TABLE, null);
    }
}
