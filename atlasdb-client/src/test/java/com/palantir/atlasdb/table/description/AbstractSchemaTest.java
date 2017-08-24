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
import java.util.Set;
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
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.GenericTestTable;
import com.palantir.atlasdb.transaction.impl.AbstractTransaction;


public abstract class AbstractSchemaTest {
    private static final TableReference tableRef =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "GenericTest");
    private static final GenericTestSchemaTableFactory tableFactory = GenericTestSchemaTableFactory.of();

    private static final String testFirstColShortName = "c";
    private static final String testSecondColShortName = "d";
    private static final String testRowKey = "testRowKey";
    private static final String anotherTestRowKey = "testRowKey2";
    private static final long testValue = 2L;
    private static final long anotherTestValue = 3L;

    protected abstract void putSingleRowFirstColumn(GenericTestTable table, String roWKey, long value);
    protected abstract Long getSingleRowFirstColumn(GenericTestTable table, String rowKey);
    protected abstract List<Long> getMultipleRowsFirstColumn(GenericTestTable table, List<String> rowKey);
    protected abstract void deleteWholeRow(GenericTestTable table, String rowKey);
    protected abstract void deleteFirstColumn(GenericTestTable table, String rowKey);

    @Test
    public void testPutSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        putSingleRowFirstColumn(table, testRowKey, testValue);

        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(transaction, times(1)).useTable(tableRef, table);
        Mockito.verify(transaction, times(1)).put(eq(tableRef), argument.capture());

        Cell expectedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        assertEquals(argument.getValue().entrySet().size(), 1);
        assertEquals(argument.getValue().keySet().toArray()[0], expectedCell);
        assertTrue(Arrays.equals((byte[]) argument.getValue().values().toArray()[0],
                EncodingUtils.encodeUnsignedVarLong(testValue)));
    }

    @Test
    public void testGetSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell resultCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        SortedMap<byte[], RowResult<byte[]>> expectedResults = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        expectedResults.put(
                testRowKey.getBytes(),
                RowResult.of(resultCell, EncodingUtils.encodeUnsignedVarLong(testValue)));
        Mockito.when(transaction.getRows(any(), any(), any())).thenReturn(expectedResults);

        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        long value = getSingleRowFirstColumn(table, testRowKey);

        assertEquals(value, testValue);
        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        Mockito.verify(transaction, times(1))
                .getRows(eq(tableRef), argument.capture(), eq(firstColSelection));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertEquals(argumentRows.size(), 1);
        assertTrue(Arrays.equals(argumentRows.get(0), PtBytes.toBytes(testRowKey)));
    }

    @Test
    public void testMultipleRowsFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        Cell anotherExpectedCell =
                Cell.create(PtBytes.toBytes(anotherTestRowKey), PtBytes.toBytes(testFirstColShortName));
        SortedMap<byte[], RowResult<byte[]>> resultsMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        resultsMap.put(
                testRowKey.getBytes(),
                RowResult.of(expectedCell, EncodingUtils.encodeUnsignedVarLong(testValue)));
        resultsMap.put(
                anotherTestRowKey.getBytes(),
                RowResult.of(anotherExpectedCell, EncodingUtils.encodeUnsignedVarLong(anotherTestValue)));
        Mockito.when(transaction.getRows(any(), any(), any())).thenReturn(resultsMap);

        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        List<Long> result = getMultipleRowsFirstColumn(table, Arrays.asList(testRowKey, anotherTestRowKey));

        assertEquals(result, Arrays.asList(testValue, anotherTestValue));
        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        Mockito.verify(transaction, times(1))
                .getRows(eq(tableRef), argument.capture(), eq(firstColSelection));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertEquals(argumentRows.size(), 2);
        assertTrue(Arrays.equals(argumentRows.get(0), PtBytes.toBytes(testRowKey)));
        assertTrue(Arrays.equals(argumentRows.get(1), PtBytes.toBytes(anotherTestRowKey)));
    }

    @Test
    public void testDeleteWholeRow() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        deleteWholeRow(table, testRowKey);

        ArgumentCaptor<Set> argument = ArgumentCaptor.forClass(Set.class);
        Mockito.verify(transaction, times(1)).delete(eq(tableRef), argument.capture());

        Cell deletedFirstCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        Cell deletedSecondCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testSecondColShortName));
        assertEquals(argument.getValue(), ImmutableSet.of(deletedFirstCell, deletedSecondCell));
    }

    @Test
    public void testDeleteFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        deleteFirstColumn(table, testRowKey);

        ArgumentCaptor<Set> argument = ArgumentCaptor.forClass(Set.class);
        Mockito.verify(transaction, times(1)).delete(eq(tableRef), argument.capture());

        Cell deletedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        assertEquals(argument.getValue(), ImmutableSet.of(deletedCell));
    }
}
