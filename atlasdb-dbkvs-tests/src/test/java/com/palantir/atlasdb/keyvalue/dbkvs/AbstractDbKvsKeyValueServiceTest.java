/**
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
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;
import com.palantir.atlasdb.sweep.EquivalenceCountingIterator;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.common.base.ClosableIterator;

public abstract class AbstractDbKvsKeyValueServiceTest extends AbstractKeyValueServiceTest {
    @After
    public void resetMaxBatch() {
        setMaxRangeOfTimestampsBatchSize(DbKvs.DEFAULT_GET_RANGE_OF_TS_BATCH);
    }

    @Test
    public void getRangeOfTimestampsWithReverseRangeOrdersTimestampsAndRepeatsWhenNecessary() {
        setupTestTable();
        setMaxRangeOfTimestampsBatchSize(4);
        try (ClosableIterator<RowResult<Set<Long>>> rowResults = keyValueService.getRangeOfTimestamps(
                TEST_TABLE,
                reverseRange(5),
                100)) {
            EquivalenceCountingIterator<RowResult<Set<Long>>> iterator =
                    new EquivalenceCountingIterator<>(rowResults, 5, SweepTaskRunnerImpl.sameRowEquivalence());
            assertRowColumnsTimestamps(iterator.next(), 9, ImmutableSet.of(9), 90L, 91L, 92L, 93L);
            assertRowColumnsTimestamps(iterator.next(), 9, ImmutableSet.of(9), 93L, 94L, 95L, 96L);
            assertRowColumnsTimestamps(iterator.next(), 9, ImmutableSet.of(9), 96L, 97L, 98L);
            assertRowColumnsTimestamps(iterator.next(), 8, ImmutableSet.of(8), 80L);
            assertRowColumnsTimestamps(iterator.next(), 8, ImmutableSet.of(8), 80L, 81L, 82L, 83L);
            assertRowColumnsTimestamps(iterator.next(), 8, ImmutableSet.of(8), 83L, 84L, 85L, 86L);
            assertRowColumnsTimestamps(iterator.next(), 8, ImmutableSet.of(8, 9), 86L, 87L, 90L, 91L);
            while (iterator.hasNext()) {
                iterator.next();
            }
            assertThat(iterator.size()).isEqualTo(5);
            assertRowColumnsTimestamps(iterator.lastItem(), 5, ImmutableSet.of(9), 95L, 96L, 97L, 98L);
        }
    }

    private void assertRowColumnsTimestamps(RowResult<Set<Long>> entry, int row, Set<Integer> cols, Long... values) {
        assertThat(entry.getRowName()).isEqualTo(PtBytes.toBytes(row));
        SortedMap<byte[], Set<Long>> columns = entry.getColumns();
        assertThat(columns.keySet()).containsExactlyElementsOf(
                cols.stream().map(PtBytes::toBytes).collect(Collectors.toList()));
        Set<Long> timestamps = new HashSet<>();
        columns.values().stream().forEach(set -> timestamps.addAll(set));
        assertThat(timestamps).containsExactlyInAnyOrder(values);
    }

    private RangeRequest reverseRange(int rowBatchSize) {
        return RangeRequest.builder(true)
                .startRowInclusive(PtBytes.toBytes(Long.MAX_VALUE))
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
