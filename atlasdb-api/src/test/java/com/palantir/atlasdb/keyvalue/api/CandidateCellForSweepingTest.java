/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CandidateCellForSweepingTest {
    private static final byte BYTE = (byte) 0xa;
    private static final long TIMESTAMP = 1977;
    private static final ImmutableSet<Integer> THREE_CELL_NAME_SIZES =
            ImmutableSet.of(1, Cell.MAX_NAME_LENGTH / 2, Cell.MAX_NAME_LENGTH);
    private static final ImmutableList<Cell> EXAMPLE_CELLS =
            Sets.cartesianProduct(THREE_CELL_NAME_SIZES, THREE_CELL_NAME_SIZES).stream()
                    .map(pair -> Cell.create(spawnBytes(pair.get(0)), spawnBytes(pair.get(1))))
                    .collect(ImmutableList.toImmutableList());

    private static final ImmutableList<Integer> SORTED_TIMESTAMPS_SIZES = ImmutableList.of(0, 1, 2, 100, 1000);

    @Mock
    private List<Long> MOCK_TIMESTAMPS;

    @Test
    public void candidateCellSizeWithLargerTimestampCollectionIsBigger() {
        EXAMPLE_CELLS.forEach(cell -> {
            CandidateCellForSweeping withOneTimestamp = createCandidateCell(cell, ImmutableSet.of(TIMESTAMP), true);
            CandidateCellForSweeping withTwoTimestamps =
                    createCandidateCell(cell, ImmutableSet.of(TIMESTAMP, TIMESTAMP + 1), false);
            assertThat(withOneTimestamp.sizeInBytes()).isLessThan(withTwoTimestamps.sizeInBytes());
        });
    }

    @Test
    public void candidateCellSizeIsCorrectForDifferentSortedTimestampSizes() {
        SORTED_TIMESTAMPS_SIZES.forEach(sortedTimestampsSize -> {
            for (CandidateCellForSweeping candidate : createCandidateCells(sortedTimestampsSize)) {
                assertThat(candidate.sizeInBytes())
                        .isEqualTo(Long.sum(candidate.cell().sizeInBytes(), (long) sortedTimestampsSize * Long.BYTES));
            }
        });
    }

    @Test
    public void noOverflowFromCollectionSize() {
        // Mocking because otherwise we OOM.
        when(MOCK_TIMESTAMPS.size()).thenReturn(Integer.MAX_VALUE);
        Cell exampleCell = EXAMPLE_CELLS.get(0);
        for (boolean isLatestValueEmpty : new boolean[] {true, false}) {
            assertThat(createCandidateCell(exampleCell, MOCK_TIMESTAMPS, isLatestValueEmpty)
                            .sizeInBytes())
                    .isEqualTo(Long.sum(Integer.MAX_VALUE * 8L, exampleCell.sizeInBytes()));
        }
    }

    private static ImmutableSet<CandidateCellForSweeping> createCandidateCells(int sortedTimestampsSize) {
        ImmutableSet.Builder<CandidateCellForSweeping> builder = ImmutableSet.<CandidateCellForSweeping>builder();
        for (boolean isLatestValueEmpty : new boolean[] {true, false}) {
            builder.addAll(EXAMPLE_CELLS.stream()
                    .map(cell -> createCandidateCell(
                            cell, spawnCollectionOfTimestamps(sortedTimestampsSize), isLatestValueEmpty))
                    .iterator());
        }
        return builder.build();
    }

    private static CandidateCellForSweeping createCandidateCell(
            Cell cell, Collection<Long> sortedTimestamps, boolean isLatestValueEmpty) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(cell)
                .sortedTimestamps(sortedTimestamps)
                .isLatestValueEmpty(isLatestValueEmpty)
                .build();
    }

    private static List<Long> spawnCollectionOfTimestamps(int size) {
        return Collections.nCopies(size, TIMESTAMP);
    }

    private static byte[] spawnBytes(int size) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, BYTE);
        return bytes;
    }
}
