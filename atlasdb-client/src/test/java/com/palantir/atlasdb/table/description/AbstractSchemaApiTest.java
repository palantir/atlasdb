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
package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
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
import com.palantir.atlasdb.table.description.generated.SchemaApiTestTable;
import com.palantir.atlasdb.table.description.test.StringValue;
import com.palantir.atlasdb.table.description.test.StringValuePersister;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractTransaction;
import com.palantir.common.base.BatchingVisitableFromIterable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public abstract class AbstractSchemaApiTest {
    private static final TableReference tableRef =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, SchemaApiTestTable.getRawTableName());

    protected static final String TEST_ROW_KEY = "testRowKey";
    protected static final String TEST_ROW_KEY2 = "testRowKey2";
    protected static final String TEST_ROW_KEY3 = "testRowKey3";
    protected static final String RANGE_END_ROW_KEY = "testRowKeyEndRange";
    protected static final long TEST_VALUE_LONG = 2L;
    protected static final long TEST_VALUE_LONG2 = 3L;
    protected static final StringValue TEST_VALUE_STRING = StringValue.of("value1");
    protected static final StringValue TEST_VALUE_STRING2 = StringValue.of("value2");
    protected static final StringValue TEST_VALUE_STRING3 = StringValue.of("value3");

    protected static final StringValuePersister STRING_VALUE_PERSISTER = new StringValuePersister();

    protected static final String FIRST_COL_SHORT_NAME = "c";
    protected static final String SECOND_COL_SHORT_NAME = "d";

    protected static final ColumnSelection FIRST_COLUMN_SELECTION =
            ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes(FIRST_COL_SHORT_NAME)));
    protected static final ColumnSelection SECOND_COLUMN_SELECTION =
            ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes(SECOND_COL_SHORT_NAME)));

    protected abstract void putSingleRowFirstColumn(Transaction transaction, String roWKey, long value);

    protected abstract Long getSingleRowFirstColumn(Transaction transaction, String rowKey);

    protected abstract Map<String, Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKey);

    protected abstract Map<String, StringValue> getRangeSecondColumn(
            Transaction transaction, String startRowKey, String endRowKey);

    protected abstract Map<String, StringValue> getRangeSecondColumnOnlyFirstTwoResults(
            Transaction transaction, String testRowKey, String rangeEndRowKey);

    protected abstract void deleteWholeRow(Transaction transaction, String rowKey);

    protected abstract void deleteFirstColumn(Transaction transaction, String rowKey);

    @Test
    public void testPutSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        putSingleRowFirstColumn(transaction, TEST_ROW_KEY, TEST_VALUE_LONG);

        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        verify(transaction, times(1)).put(eq(tableRef), argument.capture());

        Map<Cell, byte[]> foundMap = argument.getValue();
        Cell expectedCell = getCell(TEST_ROW_KEY, FIRST_COL_SHORT_NAME);
        assertThat(Iterables.getOnlyElement(foundMap.keySet())).isEqualTo(expectedCell);
        assertThat(Iterables.getOnlyElement(foundMap.values()))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(encodeLong(TEST_VALUE_LONG));
    }

    @Test
    public void testGetSingleRowFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        NavigableMap<byte[], RowResult<byte[]>> resultsMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        addToResultsMap(resultsMap, TEST_ROW_KEY, FIRST_COL_SHORT_NAME, encodeLong(TEST_VALUE_LONG));
        when(transaction.getRows(eq(tableRef), any(), eq(FIRST_COLUMN_SELECTION)))
                .thenReturn(resultsMap);

        long value = getSingleRowFirstColumn(transaction, TEST_ROW_KEY);

        assertThat(value).isEqualTo(TEST_VALUE_LONG);
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        verify(transaction, times(1)).getRows(eq(tableRef), argument.capture(), eq(FIRST_COLUMN_SELECTION));

        Iterable<byte[]> argumentRows = argument.getValue();
        assertThat(Iterables.getOnlyElement(argumentRows))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY));
    }

    @Test
    public void testGetMultipleRowsFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        NavigableMap<byte[], RowResult<byte[]>> resultsMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        addToResultsMap(resultsMap, TEST_ROW_KEY, FIRST_COL_SHORT_NAME, encodeLong(TEST_VALUE_LONG));
        addToResultsMap(resultsMap, TEST_ROW_KEY2, FIRST_COL_SHORT_NAME, encodeLong(TEST_VALUE_LONG2));
        when(transaction.getRows(eq(tableRef), any(), eq(FIRST_COLUMN_SELECTION)))
                .thenReturn(resultsMap);

        Map<String, Long> result = getMultipleRowsFirstColumn(transaction, Arrays.asList(TEST_ROW_KEY, TEST_ROW_KEY2));

        assertThat(result).isEqualTo(ImmutableMap.of(TEST_ROW_KEY, TEST_VALUE_LONG, TEST_ROW_KEY2, TEST_VALUE_LONG2));
        ArgumentCaptor<Iterable> argument = ArgumentCaptor.forClass(Iterable.class);
        verify(transaction, times(1)).getRows(eq(tableRef), argument.capture(), eq(FIRST_COLUMN_SELECTION));

        List<byte[]> argumentRows = Lists.newArrayList(argument.getValue());
        assertThat(argumentRows).hasSize(2);
        assertThat(argumentRows.get(0))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY));
        assertThat(argumentRows.get(1))
                .usingComparator(UnsignedBytes.lexicographicalComparator())
                .isEqualTo(PtBytes.toBytes(TEST_ROW_KEY2));
    }

    @Test
    public void testRowRangeSecondColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = getCell(TEST_ROW_KEY, SECOND_COL_SHORT_NAME);
        Cell anotherExpectedCell = getCell(TEST_ROW_KEY2, SECOND_COL_SHORT_NAME);
        RangeRequest expectedRange = RangeRequest.builder()
                .startRowInclusive(TEST_ROW_KEY.getBytes())
                .endRowExclusive(RANGE_END_ROW_KEY.getBytes())
                .retainColumns(SECOND_COLUMN_SELECTION)
                .build();
        when(transaction.getRange(tableRef, expectedRange))
                .thenReturn(BatchingVisitableFromIterable.create(Arrays.asList(
                        RowResult.of(expectedCell, STRING_VALUE_PERSISTER.persistToBytes(TEST_VALUE_STRING)),
                        RowResult.of(anotherExpectedCell, STRING_VALUE_PERSISTER.persistToBytes(TEST_VALUE_STRING2)))));

        Map<String, StringValue> result = getRangeSecondColumn(transaction, TEST_ROW_KEY, RANGE_END_ROW_KEY);

        assertThat(result)
                .isEqualTo(ImmutableMap.of(TEST_ROW_KEY, TEST_VALUE_STRING, TEST_ROW_KEY2, TEST_VALUE_STRING2));
        verify(transaction, times(1)).getRange(tableRef, expectedRange);
    }

    @Test
    public void testRowRangeSecondColumnFirstTwoResults() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        Cell expectedCell = getCell(TEST_ROW_KEY, SECOND_COL_SHORT_NAME);
        Cell anotherExpectedCell = getCell(TEST_ROW_KEY2, SECOND_COL_SHORT_NAME);
        Cell cellToBeDroppedFromResults = getCell(TEST_ROW_KEY3, SECOND_COL_SHORT_NAME);
        RangeRequest expectedRange = RangeRequest.builder()
                .startRowInclusive(TEST_ROW_KEY.getBytes())
                .endRowExclusive(RANGE_END_ROW_KEY.getBytes())
                .retainColumns(SECOND_COLUMN_SELECTION)
                .batchHint(2)
                .build();
        when(transaction.getRange(tableRef, expectedRange))
                .thenReturn(BatchingVisitableFromIterable.create(Arrays.asList(
                        RowResult.of(expectedCell, STRING_VALUE_PERSISTER.persistToBytes(TEST_VALUE_STRING)),
                        RowResult.of(anotherExpectedCell, STRING_VALUE_PERSISTER.persistToBytes(TEST_VALUE_STRING2)),
                        RowResult.of(
                                cellToBeDroppedFromResults,
                                STRING_VALUE_PERSISTER.persistToBytes(TEST_VALUE_STRING3)))));

        Map<String, StringValue> result =
                getRangeSecondColumnOnlyFirstTwoResults(transaction, TEST_ROW_KEY, RANGE_END_ROW_KEY);

        assertThat(result)
                .isEqualTo(ImmutableMap.of(TEST_ROW_KEY, TEST_VALUE_STRING, TEST_ROW_KEY2, TEST_VALUE_STRING2));
        verify(transaction, times(1)).getRange(tableRef, expectedRange);
    }

    @Test
    public void testDeleteWholeRow() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        deleteWholeRow(transaction, TEST_ROW_KEY);

        Cell expectedDeletedFirstCell = getCell(TEST_ROW_KEY, FIRST_COL_SHORT_NAME);
        Cell expectedDeletedSecondCell = getCell(TEST_ROW_KEY, SECOND_COL_SHORT_NAME);
        verify(transaction, times(1))
                .delete(tableRef, ImmutableSet.of(expectedDeletedFirstCell, expectedDeletedSecondCell));
    }

    @Test
    public void testDeleteFirstColumn() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);

        deleteFirstColumn(transaction, TEST_ROW_KEY);

        Cell expectedDeletedCell = getCell(TEST_ROW_KEY, FIRST_COL_SHORT_NAME);
        verify(transaction, times(1)).delete(tableRef, ImmutableSet.of(expectedDeletedCell));
    }

    protected void addToResultsMap(
            SortedMap<byte[], RowResult<byte[]>> map, String rowKey, String colShortName, byte[] value) {
        Cell resultCell = getCell(rowKey, colShortName);
        map.put(rowKey.getBytes(), RowResult.of(resultCell, value));
    }

    protected Cell getCell(String rowKey, String colShortName) {
        return Cell.create(PtBytes.toBytes(rowKey), PtBytes.toBytes(colShortName));
    }

    protected byte[] encodeLong(Long value) {
        return EncodingUtils.encodeUnsignedVarLong(value);
    }
}
