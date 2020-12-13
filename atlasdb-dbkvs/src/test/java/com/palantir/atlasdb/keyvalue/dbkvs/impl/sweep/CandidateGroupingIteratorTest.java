/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CandidateGroupingIteratorTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void emptyInput() {
        assertThat(group(ImmutableList.of())).isEmpty();
    }

    @Test
    public void singleCellInOnePage() {
        assertThat(group(ImmutableList.of(ImmutableList.of(cellTs("a", "x", 10L), cellTs("a", "x", 20L))))).isEqualTo(ImmutableList.of(ImmutableList.of(candidate("a", "x", 2L, false, 10L, 20L))));
    }

    @Test
    public void singleCellSpanningTwoPages() {
        assertThat(group(ImmutableList.of(
                        ImmutableList.of(cellTs("a", "x", 10L), cellTs("a", "x", 20L)),
                        ImmutableList.of(cellTs("a", "x", 30L))))).isEqualTo(ImmutableList.of(
                        ImmutableList.of(), ImmutableList.of(candidate("a", "x", 3L, false, 10L, 20L, 30L))));
    }

    @Test
    public void severalCellsInSinglePage() {
        assertThat(group(ImmutableList.of(
                        ImmutableList.of(cellTs("a", "x", 10L), cellTs("a", "y", 10L), cellTs("b", "y", 10L))))).isEqualTo(ImmutableList.of(ImmutableList.of(
                        candidate("a", "x", 1L, false, 10L),
                        candidate("a", "y", 2L, false, 10L),
                        candidate("b", "y", 3L, false, 10L))));
    }

    @Test
    public void latestValueEmpty() {
        assertThat(group(ImmutableList.of(ImmutableList.of(cellTs("a", "x", 10L, false), cellTs("a", "x", 20L, true))))).isEqualTo(ImmutableList.of(ImmutableList.of(candidate("a", "x", 2L, true, 10L, 20L))));
    }

    @Test
    public void nonLatestValueEmpty() {
        assertThat(group(ImmutableList.of(ImmutableList.of(cellTs("a", "x", 10L, true), cellTs("a", "x", 20L, false))))).isEqualTo(ImmutableList.of(ImmutableList.of(candidate("a", "x", 2L, false, 10L, 20L))));
    }

    @Test
    public void throwIfTimestampsAreOutOfOrder() {
        thrown.expectMessage("Timestamps for each cell must be fed in strictly increasing order");
        group(ImmutableList.of(ImmutableList.of(cellTs("a", "x", 20L), cellTs("a", "x", 10L))));
    }

    private static CandidateCellForSweeping candidate(
            String rowName, String colName, long numCellTsPairsExamined, boolean latestValEmpty, Long... ts) {
        Arrays.sort(ts);
        return ImmutableCandidateCellForSweeping.builder()
                .cell(Cell.create(bytes(rowName), bytes(colName)))
                .isLatestValueEmpty(latestValEmpty)
                .sortedTimestamps(Arrays.asList(ts))
                .build();
    }

    private List<List<CandidateCellForSweeping>> group(List<List<CellTsPairInfo>> cellTsPairBatches) {
        return ImmutableList.copyOf(CandidateGroupingIterator.create(cellTsPairBatches.iterator()));
    }

    private static CellTsPairInfo cellTs(String rowName, String colName, long ts) {
        return cellTs(rowName, colName, ts, false);
    }

    private static CellTsPairInfo cellTs(String rowName, String colName, long ts, boolean emptyVal) {
        return new CellTsPairInfo(bytes(rowName), bytes(colName), ts, emptyVal);
    }

    private static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }
}
