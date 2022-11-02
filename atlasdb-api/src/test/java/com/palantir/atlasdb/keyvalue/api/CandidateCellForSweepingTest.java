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

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CandidateCellForSweepingTest {
    private static final int CELL_NAME_SIZE = 100;
    private static final int TIMESTAMPS_COLLECTION_SIZE = 200;
    private static final long TIMESTAMP = 1977;

    private static final Cell CELL = Cell.create(spawnBytes(), spawnBytes());
    private static final List<Long> TIMESTAMPS = Collections.nCopies(TIMESTAMPS_COLLECTION_SIZE, TIMESTAMP);

    @Mock
    private List<Long> MOCK_TIMESTAMPS;

    @Test
    public void candidateCellHasCorrectSizeForEmptyTimestampCollection() {
        CandidateCellForSweeping candidate = createCandidateCell(ImmutableSet.of(), false);
        assertThat(candidate.sizeInBytes()).isEqualTo(CELL.sizeInBytes());
    }

    @Test
    public void candidateCellHasCorrectSizeForOneTimestamp() {
        CandidateCellForSweeping candidate = createCandidateCell(ImmutableSet.of(TIMESTAMP), false);
        assertThat(candidate.sizeInBytes()).isEqualTo(Long.sum(CELL.sizeInBytes(), Long.BYTES));
    }

    @Test
    public void candidateCellSizeHasCorrectSizeForMultipleTimestamps() {
        CandidateCellForSweeping candidate = createCandidateCell(TIMESTAMPS, false);
        assertThat(candidate.sizeInBytes())
                .isEqualTo(Long.sum(CELL.sizeInBytes(), Long.BYTES * ((long) TIMESTAMPS_COLLECTION_SIZE)));
    }

    @Test
    public void noOverflowFromCollectionSize() {
        // Mocking because otherwise we OOM.
        when(MOCK_TIMESTAMPS.size()).thenReturn(Integer.MAX_VALUE);
        assertThat(createCandidateCell(MOCK_TIMESTAMPS, false).sizeInBytes())
                .isEqualTo(Long.sum(Integer.MAX_VALUE * 8L, CELL.sizeInBytes()));
    }

    @Test
    public void candidateCellSizeIsEqualRegardlessOfLatestValueEmpty() {
        CandidateCellForSweeping candidateWithLatestValueNonEmpty = createCandidateCell(TIMESTAMPS, false);
        CandidateCellForSweeping candidateWithLatestValueEmpty = createCandidateCell(TIMESTAMPS, true);
        assertThat(candidateWithLatestValueNonEmpty.sizeInBytes())
                .isEqualTo(candidateWithLatestValueEmpty.sizeInBytes());
    }

    private static CandidateCellForSweeping createCandidateCell(
            Collection<Long> sortedTimestamps, boolean isLatestValueEmpty) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(CELL)
                .sortedTimestamps(sortedTimestamps)
                .isLatestValueEmpty(isLatestValueEmpty)
                .build();
    }

    private static byte[] spawnBytes() {
        return new byte[CELL_NAME_SIZE];
    }
}
