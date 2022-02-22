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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import java.util.List;
import org.junit.Test;

public class WhereClausesTest {
    private static final byte[] COL1 = PtBytes.toBytes("col1");
    private static final byte[] COL2 = PtBytes.toBytes("col2");
    private static final byte[] COL3 = PtBytes.toBytes("col3");

    private static final byte[] START = PtBytes.toBytes("start");
    private static final byte[] END = PtBytes.toBytes("the end");

    @Test
    public void startOnly() {
        RangeRequest request = RangeRequest.builder().startRowInclusive(START).build();
        WhereClauses whereClauses = WhereClauses.create("i", request);

        List<String> expectedClauses = ImmutableList.of("i.row_name >= ?");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(START));
    }

    @Test
    public void endOnly() {
        RangeRequest request = RangeRequest.builder().endRowExclusive(END).build();
        WhereClauses whereClauses = WhereClauses.create("i", request);

        List<String> expectedClauses = ImmutableList.of("i.row_name < ?");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(END));
    }

    @Test
    public void whereClausesNoColumns() {
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(START)
                .endRowExclusive(END)
                .build();
        WhereClauses whereClauses = WhereClauses.create("i", request);

        List<String> expectedClauses = ImmutableList.of("i.row_name >= ?", "i.row_name < ?");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(START, END));
    }

    @Test
    public void withReverseRange() {
        RangeRequest request = RangeRequest.reverseBuilder()
                .startRowInclusive(END)
                .endRowExclusive(START)
                .build();
        WhereClauses whereClauses = WhereClauses.create("i", request);

        List<String> expectedClauses = ImmutableList.of("i.row_name <= ?", "i.row_name > ?");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(END, START));
    }

    @Test
    public void whereClausesOneColumn() {
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(START)
                .endRowExclusive(END)
                .retainColumns(ColumnSelection.create(ImmutableList.of(COL1)))
                .build();
        WhereClauses whereClauses = WhereClauses.create("i", request);

        List<String> expectedClauses = ImmutableList.of("i.row_name >= ?", "i.row_name < ?", "i.col_name IN (?)");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(START, END, COL1));
    }

    @Test
    public void whereClausesMultiColumn() {
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(START)
                .endRowExclusive(END)
                .retainColumns(ColumnSelection.create(ImmutableList.of(COL1, COL2, COL3)))
                .build();
        WhereClauses whereClauses = WhereClauses.create("i", request);

        List<String> expectedClauses = ImmutableList.of("i.row_name >= ?", "i.row_name < ?", "i.col_name IN (?,?,?)");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(START, END, COL1, COL2, COL3));
    }

    @Test
    public void whereClausesWithExtraClause() {
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(START)
                .endRowExclusive(END)
                .build();
        String extraClause = "i.foo = bar";
        WhereClauses whereClauses = WhereClauses.create("i", request, extraClause);

        List<String> expectedClauses = ImmutableList.of("i.row_name >= ?", "i.row_name < ?", extraClause);
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(START, END));
    }

    @Test
    public void usesDifferentTableIdentifier() {
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(START)
                .endRowExclusive(END)
                .retainColumns(ColumnSelection.create(ImmutableList.of(COL1)))
                .build();
        WhereClauses whereClauses = WhereClauses.create("other", request);

        List<String> expectedClauses =
                ImmutableList.of("other.row_name >= ?", "other.row_name < ?", "other.col_name IN (?)");
        assertThat(expectedClauses).containsExactlyElementsOf(whereClauses.getClauses());

        checkWhereArguments(whereClauses, ImmutableList.of(START, END, COL1));
    }

    private void checkWhereArguments(WhereClauses whereClauses, List<byte[]> expectedArgs) {
        List<Object> actualArgs = whereClauses.getArguments();
        for (int i = 0; i < actualArgs.size(); i++) {
            assertThat((byte[]) actualArgs.get(i)).isEqualTo(expectedArgs.get(i));
        }
        assertThat(actualArgs).hasSameSizeAs(expectedArgs);
    }
}
