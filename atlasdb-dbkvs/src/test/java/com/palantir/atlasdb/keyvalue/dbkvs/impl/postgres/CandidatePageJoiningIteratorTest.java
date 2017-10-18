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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;

public class CandidatePageJoiningIteratorTest {

    @Test
    public void emptySequenceStaysEmpty() {
        // nothing -> nothing
        assertTrue(transform(2, ImmutableList.of()).isEmpty());
    }

    @Test
    public void oneEmptyListBecomesEmptySequence() {
        // [] -> nothing
        assertTrue(transform(2, ImmutableList.of(ImmutableList.of())).isEmpty());
    }

    @Test
    public void manyEmptyListsBecomeEmptySequence() {
        // [], [], [] -> nothing
        assertTrue(
                transform(2, ImmutableList.of(ImmutableList.of(), ImmutableList.of(), ImmutableList.of())).isEmpty());
    }

    @Test
    public void pagesThatAreBigEnoughArePreservedAsIs() {
        // min page size 2: [2], [2] -> [2], [2]
        assertEquals(
                ImmutableList.of(ImmutableList.of(candidate(100, 2)), ImmutableList.of(candidate(110, 2))),
                transform(2,
                        ImmutableList.of(ImmutableList.of(candidate(100, 2)), ImmutableList.of(candidate(110, 2)))));
    }

    @Test
    public void smallPagesAreJoined() {
        // min page size 2: [1], [], [2], [1] -> [1, 2], [1]
        assertEquals(
                ImmutableList.of(
                        ImmutableList.of(candidate(100, 1), candidate(110, 2)),
                        ImmutableList.of(candidate(120, 1))),
                transform(2, ImmutableList.of(
                        ImmutableList.of(candidate(100, 1)),
                        ImmutableList.of(),
                        ImmutableList.of(candidate(110, 2)),
                        ImmutableList.of(candidate(120, 1)))));
    }

    @Test
    public void nonCandidateCellsCountAsOne() {
        // min page size 2: [0], [0], [0], -> [0, 0], [0]
        assertEquals(
                ImmutableList.of(
                        ImmutableList.of(candidate(100, 0), candidate(110, 0)),
                        ImmutableList.of(candidate(120, 0))),
                transform(2, ImmutableList.of(
                        ImmutableList.of(candidate(100, 0)),
                        ImmutableList.of(candidate(110, 0)),
                        ImmutableList.of(candidate(120, 0)))));
    }

    private static List<List<CandidateCellForSweeping>> transform(int minPageSize,
                                                                  List<List<CandidateCellForSweeping>> inputPages) {
        return ImmutableList.copyOf(new CandidatePageJoiningIterator(inputPages.iterator(), minPageSize));
    }

    private static CandidateCellForSweeping candidate(int rowName, int numTs) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(Cell.create(Ints.toByteArray(rowName), Ints.toByteArray(1)))
                .isLatestValueEmpty(false)
                .numCellsTsPairsExamined(123)
                .sortedTimestamps(new long[numTs])
                .build();
    }

}
