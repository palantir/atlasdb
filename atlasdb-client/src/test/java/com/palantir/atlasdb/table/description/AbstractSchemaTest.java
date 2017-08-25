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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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


public abstract class AbstractSchemaTest {
    private static final TableReference tableRef =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "GenericTest");

    private static final String testRowKey = "testRowKey";
    private static final String anotherTestRowKey = "testRowKey2";
    private static final long testValue = 2L;
    private static final long anotherTestValue = 3L;
    private static ColumnSelection firstColSelection =
            ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));

    protected static final String firstColShortName = "c";
    protected static final String secondColShortName = "d";

    protected abstract void putSingleRowFirstColumn(Transaction transaction, String roWKey, long value);
    protected abstract Long getSingleRowFirstColumn(Transaction transaction, String rowKey);
    protected abstract List<Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKey);
    protected abstract List<String> getRangeSecondColumn(Transaction transaction, String startRowKey, String endRowKey);
    protected abstract void deleteWholeRow(Transaction transaction, String rowKey);
    protected abstract void deleteFirstColumn(Transaction transaction, String rowKey);

    @Test
    public void testPutSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        putSingleRowFirstColumn(transaction, testRowKey, testValue);

        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(transaction, times(1)).put(eq(tableRef), argument.capture());

        Cell expectedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(firstColShortName));
        assertEquals(argument.getValue().entrySet().size(), 1);
        assertEquals(argument.getValue().keySet().toArray()[0], expectedCell);
        assertTrue(Arrays.equals((byte[]) argument.getValue().values().toArray()[0],
                EncodingUtils.encodeUnsignedVarLong(testValue)));
    }

    @Test
    public void testGetSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell resultCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(firstColShortName));
        SortedMap<byte[], RowResult<byte[]>> expectedResults = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        expectedResults.put(
                testRowKey.getBytes(),
                RowResult.of(resultCell, EncodingUtils.encodeUnsignedVarLong(testValue)));
        Mockito.when(transaction.getRows(any(), any(), any())).thenReturn(expectedResults);

        long value = getSingleRowFirstColumn(transaction, testRowKey);

        assertEquals(value, testValue);
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        Mockito.verify(transaction, times(1))
                .getRows(eq(tableRef), argument.capture(), eq(firstColSelection));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertEquals(argumentRows.size(), 1);
        assertTrue(Arrays.equals(argumentRows.get(0), PtBytes.toBytes(testRowKey)));
    }

    @Test
    public void testGetMultipleRowsFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(firstColShortName));
        Cell anotherExpectedCell =
                Cell.create(PtBytes.toBytes(anotherTestRowKey), PtBytes.toBytes(firstColShortName));
        SortedMap<byte[], RowResult<byte[]>> resultsMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        resultsMap.put(
                testRowKey.getBytes(),
                RowResult.of(expectedCell, EncodingUtils.encodeUnsignedVarLong(testValue)));
        resultsMap.put(
                anotherTestRowKey.getBytes(),
                RowResult.of(anotherExpectedCell, EncodingUtils.encodeUnsignedVarLong(anotherTestValue)));
        Mockito.when(transaction.getRows(any(), any(), any())).thenReturn(resultsMap);

        List<Long> result = getMultipleRowsFirstColumn(transaction, Arrays.asList(testRowKey, anotherTestRowKey));

        assertEquals(result, Arrays.asList(testValue, anotherTestValue));
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        Mockito.verify(transaction, times(1))
                .getRows(eq(tableRef), argument.capture(), eq(firstColSelection));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertEquals(argumentRows.size(), 2);
        assertTrue(Arrays.equals(argumentRows.get(0), PtBytes.toBytes(testRowKey)));
        assertTrue(Arrays.equals(argumentRows.get(1), PtBytes.toBytes(anotherTestRowKey)));
    }

    @Test
    public void testRowRange() {
        String endRowKey = "testRowKey3";
        String testStringValue = "value1";
        String anotherTestStringValue = "value2";

        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(secondColShortName));
        Cell anotherExpectedCell =
                Cell.create(PtBytes.toBytes(anotherTestRowKey), PtBytes.toBytes(secondColShortName));
        Mockito.when(transaction.getRange(any(), any())).thenReturn(
                BatchingVisitableFromIterable.create(Arrays.asList(
                        RowResult.of(expectedCell, testStringValue.getBytes()),
                        RowResult.of(anotherExpectedCell, anotherTestStringValue.getBytes())
                ))
        );

        List<String> result = getRangeSecondColumn(transaction, testRowKey, endRowKey);

        assertEquals(result, Arrays.asList(testStringValue, anotherTestStringValue));
        ArgumentCaptor<RangeRequest> argument = ArgumentCaptor.forClass(RangeRequest.class);
        Mockito.verify(transaction, times(1))
                .getRange(eq(tableRef), argument.capture());

        RangeRequest rangeRequest = argument.getValue();
        assertTrue(Arrays.equals(rangeRequest.getStartInclusive(), PtBytes.toBytes(testRowKey)));
        assertTrue(Arrays.equals(rangeRequest.getEndExclusive(), PtBytes.toBytes(endRowKey)));
        assertEquals(rangeRequest.getColumnNames().size(), 1);
        assertTrue(rangeRequest.containsColumn(PtBytes.toBytes(secondColShortName)));
    }

    @Test
    public void testDeleteWholeRow() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        deleteWholeRow(transaction, testRowKey);

        Cell expectedDeletedFirstCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(firstColShortName));
        Cell expectedDeletedSecondCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(secondColShortName));
        Mockito.verify(transaction, times(1)).delete(tableRef,
                ImmutableSet.of(expectedDeletedFirstCell, expectedDeletedSecondCell));
    }

    @Test
    public void testDeleteFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        deleteFirstColumn(transaction, testRowKey);

        Cell expectedDeletedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(firstColShortName));
        Mockito.verify(transaction, times(1)).delete(tableRef,
                ImmutableSet.of(expectedDeletedCell));
    }
}
