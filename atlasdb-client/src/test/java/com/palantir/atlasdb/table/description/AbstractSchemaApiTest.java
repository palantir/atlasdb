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

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractTransaction;
import com.palantir.common.base.BatchingVisitableFromIterable;

public abstract class AbstractSchemaApiTest {
    private static final TableReference tableRef =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "SchemaApiTest");

    private static final String TEST_ROW_KEY = "testRowKey";
    private static final String TEST_ROW_KEY2 = "testRowKey2";
    private static final long TEST_VALUE = 2L;
    private static final long ANOTHER_TEST_VALUE = 3L;
    private static final ColumnSelection FIRST_COLUMN_SELECTION =
            ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));

    protected static final String FIRST_COL_SHORT_NAME = "c";
    protected static final String SECOND_COL_SHORT_NAME = "d";

    protected abstract void putSingleRowFirstColumn(Transaction transaction, String roWKey, long value);
    protected abstract Long getSingleRowFirstColumn(Transaction transaction, String rowKey);
    protected abstract List<Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKey);
    protected abstract List<String> getRangeSecondColumn(Transaction transaction, String startRowKey, String endRowKey);
    protected abstract void deleteWholeRow(Transaction transaction, String rowKey);
    protected abstract void deleteFirstColumn(Transaction transaction, String rowKey);

    @Test
    public void testPutSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        putSingleRowFirstColumn(transaction, TEST_ROW_KEY, TEST_VALUE);

        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(transaction, times(1)).put(eq(tableRef), argument.capture());

        Map<Cell, byte[]> foundMap = argument.getValue();
        Cell expectedCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY), PtBytes.toBytes(FIRST_COL_SHORT_NAME));
        assertThat(Iterables.getOnlyElement(foundMap.keySet())).isEqualTo(expectedCell);
        assertThat(Iterables.getOnlyElement(foundMap.values()))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(EncodingUtils.encodeUnsignedVarLong(TEST_VALUE));
    }

    @Test
    public void testGetSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell resultCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY), PtBytes.toBytes(FIRST_COL_SHORT_NAME));
        SortedMap<byte[], RowResult<byte[]>> expectedResults = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        expectedResults.put(
                TEST_ROW_KEY.getBytes(),
                RowResult.of(resultCell, EncodingUtils.encodeUnsignedVarLong(TEST_VALUE)));
        Mockito.when(transaction.getRows(eq(tableRef), any(), eq(FIRST_COLUMN_SELECTION))).thenReturn(expectedResults);

        long value = getSingleRowFirstColumn(transaction, TEST_ROW_KEY);

        assertThat(value).isEqualTo(TEST_VALUE);
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        Mockito.verify(transaction, times(1))
                .getRows(eq(tableRef), argument.capture(), eq(FIRST_COLUMN_SELECTION));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertThat(Iterables.getOnlyElement(argumentRows))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY));
    }

    @Test
    public void testGetMultipleRowsFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY), PtBytes.toBytes(FIRST_COL_SHORT_NAME));
        Cell anotherExpectedCell =
                Cell.create(PtBytes.toBytes(TEST_ROW_KEY2), PtBytes.toBytes(FIRST_COL_SHORT_NAME));
        SortedMap<byte[], RowResult<byte[]>> resultsMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        resultsMap.put(
                TEST_ROW_KEY.getBytes(),
                RowResult.of(expectedCell, EncodingUtils.encodeUnsignedVarLong(TEST_VALUE)));
        resultsMap.put(
                TEST_ROW_KEY2.getBytes(),
                RowResult.of(anotherExpectedCell, EncodingUtils.encodeUnsignedVarLong(ANOTHER_TEST_VALUE)));
        Mockito.when(transaction.getRows(eq(tableRef), any(), eq(FIRST_COLUMN_SELECTION))).thenReturn(resultsMap);

        List<Long> result = getMultipleRowsFirstColumn(transaction, Arrays.asList(TEST_ROW_KEY, TEST_ROW_KEY2));

        assertThat(result).isEqualTo(Arrays.asList(TEST_VALUE, ANOTHER_TEST_VALUE));
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        Mockito.verify(transaction, times(1))
                .getRows(eq(tableRef), argument.capture(), eq(FIRST_COLUMN_SELECTION));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertThat(argumentRows.size()).isEqualTo(2);
        assertThat(argumentRows.get(0))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY));
        assertThat(argumentRows.get(1))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY2));
    }

    @Test
    public void testRowRange() {
        final String endRowKey = "testRowKey3";
        final String testStringValue = "value1";
        final String anotherTestStringValue = "value2";

        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY), PtBytes.toBytes(SECOND_COL_SHORT_NAME));
        Cell anotherExpectedCell =
                Cell.create(PtBytes.toBytes(TEST_ROW_KEY2), PtBytes.toBytes(SECOND_COL_SHORT_NAME));
        Mockito.when(transaction.getRange(eq(tableRef), any())).thenReturn(
                BatchingVisitableFromIterable.create(Arrays.asList(
                        RowResult.of(expectedCell, testStringValue.getBytes()),
                        RowResult.of(anotherExpectedCell, anotherTestStringValue.getBytes())
                ))
        );

        List<String> result = getRangeSecondColumn(transaction, TEST_ROW_KEY, endRowKey);

        assertThat(result).isEqualTo(Arrays.asList(testStringValue, anotherTestStringValue));
        ArgumentCaptor<RangeRequest> argument = ArgumentCaptor.forClass(RangeRequest.class);
        Mockito.verify(transaction, times(1))
                .getRange(eq(tableRef), argument.capture());

        RangeRequest rangeRequestFound = argument.getValue();
        assertThat(rangeRequestFound.getStartInclusive())
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY));
        assertThat(rangeRequestFound.getEndExclusive())
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(endRowKey));
        assertThat(Iterables.getOnlyElement(rangeRequestFound.getColumnNames()))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(SECOND_COL_SHORT_NAME));
    }

    @Test
    public void testDeleteWholeRow() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        deleteWholeRow(transaction, TEST_ROW_KEY);

        Cell expectedDeletedFirstCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY),
                PtBytes.toBytes(FIRST_COL_SHORT_NAME));
        Cell expectedDeletedSecondCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY),
                PtBytes.toBytes(SECOND_COL_SHORT_NAME));
        Mockito.verify(transaction, times(1)).delete(tableRef,
                ImmutableSet.of(expectedDeletedFirstCell, expectedDeletedSecondCell));
    }

    @Test
    public void testDeleteFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        deleteFirstColumn(transaction, TEST_ROW_KEY);

        Cell expectedDeletedCell = Cell.create(PtBytes.toBytes(TEST_ROW_KEY), PtBytes.toBytes(FIRST_COL_SHORT_NAME));
        Mockito.verify(transaction, times(1)).delete(tableRef,
                ImmutableSet.of(expectedDeletedCell));
    }
}
