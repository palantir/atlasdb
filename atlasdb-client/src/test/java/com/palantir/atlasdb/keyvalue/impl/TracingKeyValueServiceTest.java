/**
 * Copyright 2017 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.tracing.TestSpanObserver;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.remoting1.tracing.SpanType;
import com.palantir.remoting1.tracing.Tracer;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@RunWith(MockitoJUnitRunner.class)
public class TracingKeyValueServiceTest {

    private KeyValueService delegate;
    private KeyValueService kvs;

    private final TestSpanObserver observer = new TestSpanObserver();
    private final Namespace nameSpace = Namespace.create("test");
    private final byte[] rowName = "row".getBytes(Charsets.UTF_8);
    private final byte[] colName = "col".getBytes(Charsets.UTF_8);
    private final TableReference tableRef = TableReference.create(nameSpace, "testTable");
    private final Cell cell = Cell.create(rowName, colName);
    private final RangeRequest rangeRequest = RangeRequest.all();
    private final ImmutableList<RangeRequest> rangeRequests = ImmutableList.of(rangeRequest);
    private final long timestamp = 1L;
    private byte[] contents = "value".getBytes(Charsets.UTF_8);
    private final Value value = Value.create(contents, timestamp);
    private byte[] metadata = "metadata".getBytes(Charsets.UTF_8);

    @Before
    public void before() throws Exception {
        Tracer.initTrace(Optional.of(true), getClass().getSimpleName() + "." + Math.random());
        Tracer.subscribe(getClass().getName(), observer);
        delegate = mock(KeyValueService.class);
        kvs = TracingKeyValueService.create(delegate);
        assertThat(observer.spans(), hasSize(0));
    }

    @After
    public void after() throws Exception {
        Tracer.unsubscribe(getClass().getName());
        kvs.close();
        verify(delegate, atLeast(1)).close();
    }

    @Test(expected = NullPointerException.class)
    public void createNullThrows() throws Exception {
        TracingKeyValueService.create(null);
    }

    @Test
    public void addGarbageCollectionSentinelValues() throws Exception {
        ImmutableSet<Cell> cells = ImmutableSet.of(cell);
        kvs.addGarbageCollectionSentinelValues(tableRef, cells);

        checkSpan("atlasdb-kvs.addGarbageCollectionSentinelValues(test.testTable, 1 cells)");
        verify(delegate).addGarbageCollectionSentinelValues(tableRef, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void checkAndSet() throws Exception {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(tableRef, cell, rowName, rowName);
        kvs.checkAndSet(request);

        checkSpan("atlasdb-kvs.checkAndSet(test.testTable, Cell{rowName=726f77, columnName=636f6c, no TTL})");
        verify(delegate).checkAndSet(request);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void close() throws Exception {
        kvs.close();

        checkSpan("atlasdb-kvs.close()");
        verify(delegate).close();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void compactInternally() throws Exception {
        kvs.compactInternally(tableRef);

        checkSpan("atlasdb-kvs.compactInternally(test.testTable)");
        verify(delegate).compactInternally(tableRef);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void createTable() throws Exception {
        byte[] metadata = new byte[0];
        kvs.createTable(tableRef, metadata);

        checkSpan("atlasdb-kvs.createTable(test.testTable)");
        verify(delegate).createTable(tableRef, metadata);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void createTables() throws Exception {
        byte[] metadata = new byte[0];
        kvs.createTables(ImmutableMap.of(tableRef, metadata));

        checkSpan("atlasdb-kvs.createTables([test.testTable])");
        verify(delegate).createTables(ImmutableMap.of(tableRef, metadata));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void delete() throws Exception {
        Multimap<Cell, Long> cells = ImmutableMultimap.of(cell, 1L);
        kvs.delete(tableRef, cells);

        checkSpan("atlasdb-kvs.delete(test.testTable, 1 keys)");
        verify(delegate).delete(tableRef, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void dropTable() throws Exception {
        kvs.dropTable(tableRef);

        checkSpan("atlasdb-kvs.dropTable(test.testTable)");
        verify(delegate).dropTable(tableRef);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void dropTables() throws Exception {
        kvs.dropTables(ImmutableSet.of(tableRef));

        checkSpan("atlasdb-kvs.dropTables([test.testTable])");
        verify(delegate).dropTables(ImmutableSet.of(tableRef));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void get() throws Exception {
        Map<Cell, Value> expectedResult = ImmutableMap.of(cell, value);
        Map<Cell, Long> cells = ImmutableMap.of(cell, 1L);
        when(delegate.get(tableRef, cells)).thenReturn(expectedResult);

        Map<Cell, Value> result = kvs.get(tableRef, cells);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.get(test.testTable, 1 cells)");
        verify(delegate).get(tableRef, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getAllTableNames() throws Exception {
        kvs.getAllTableNames();

        checkSpan("atlasdb-kvs.getAllTableNames()");
        verify(delegate).getAllTableNames();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getAllTimestamps() throws Exception {
        Set<Cell> cells = ImmutableSet.of(cell);
        kvs.getAllTimestamps(tableRef, cells, 1L);

        checkSpan("atlasdb-kvs.getAllTimestamps(test.testTable, 1 keys, ts 1)");
        verify(delegate).getAllTimestamps(tableRef, cells, 1L);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getDelegates() throws Exception {
        Collection<? extends KeyValueService> delegates = kvs.getDelegates();

        checkSpan("atlasdb-kvs.getDelegates()");
        assertThat(delegates, hasSize(1));
        assertThat(delegates.iterator().next(), equalTo(delegate));
        verify(delegate).getDelegates();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getFirstBatchForRanges() throws Exception {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> expectedResult = ImmutableMap.of();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> result = kvs.getFirstBatchForRanges(
                tableRef, rangeRequests, timestamp);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getFirstBatchForRanges(test.testTable, 1 ranges, ts 1)");
        verify(delegate).getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getLatestTimestamps() throws Exception {
        Map<Cell, Long> cells = ImmutableMap.of(cell, timestamp);
        when(delegate.getLatestTimestamps(tableRef, cells)).thenReturn(cells);

        Map<Cell, Long> result = kvs.getLatestTimestamps(tableRef, cells);

        assertThat(result.entrySet(), hasSize(1));
        checkSpan("atlasdb-kvs.getLatestTimestamps(test.testTable, 1 cells)");
        verify(delegate).getLatestTimestamps(tableRef, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMetadataForTable() throws Exception {
        when(delegate.getMetadataForTable(tableRef)).thenReturn(metadata);

        byte[] result = kvs.getMetadataForTable(tableRef);

        assertThat(result, equalTo(metadata));
        checkSpan("atlasdb-kvs.getMetadataForTable(test.testTable)");
        verify(delegate).getMetadataForTable(tableRef);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMetadataForTables() throws Exception {
        Map<TableReference, byte[]> expectedResult = ImmutableMap.of(tableRef, metadata);
        when(delegate.getMetadataForTables()).thenReturn(expectedResult);

        Map<TableReference, byte[]> result = kvs.getMetadataForTables();

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getMetadataForTables()");
        verify(delegate).getMetadataForTables();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRange() throws Exception {
        ClosableIterator<RowResult<Value>> expectedResult = ClosableIterators.wrap(ImmutableList.of(
                RowResult.of(cell, value)).iterator());
        when(delegate.getRange(tableRef, rangeRequest, timestamp)).thenReturn(expectedResult);
        try (ClosableIterator<RowResult<Value>> result = kvs.getRange(tableRef, rangeRequest, timestamp)) {
            assertThat(result, equalTo(expectedResult));
            checkSpan("atlasdb-kvs.getRange(test.testTable, RangeRequest{reverse=false}, ts 1)");
            verify(delegate).getRange(tableRef, rangeRequest, timestamp);
            verifyNoMoreInteractions(delegate);
        }
    }

    @Test
    public void getRangeOfTimestamps() throws Exception {
        Set<Long> longs = ImmutableSet.of(timestamp);
        ClosableIterator<RowResult<Set<Long>>> expectedResult = ClosableIterators.wrap(ImmutableList.of(
                RowResult.of(cell, longs)).iterator());
        when(delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp)).thenReturn(expectedResult);
        try (ClosableIterator<RowResult<Set<Long>>> result = kvs.getRangeOfTimestamps(
                tableRef, rangeRequest, timestamp)) {
            assertThat(result, equalTo(expectedResult));
            checkSpan("atlasdb-kvs.getRangeOfTimestamps(test.testTable, RangeRequest{reverse=false}, ts 1)");
            verify(delegate).getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
            verifyNoMoreInteractions(delegate);
        }
    }

    @Test
    public void getRows() throws Exception {
        ImmutableList<byte[]> rows = ImmutableList.of(rowName);
        Map<Cell, Value> expectedResult = ImmutableMap.of();
        when(delegate.getRows(tableRef, rows, ColumnSelection.all(), timestamp)).thenReturn(expectedResult);

        Map<Cell, Value> result = kvs.getRows(tableRef, rows, ColumnSelection.all(), timestamp);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getRows(test.testTable, 1 rows, , ts 1)");
        verify(delegate).getRows(tableRef, rows, ColumnSelection.all(), timestamp);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsColumnRangeBatch() throws Exception {
        RowColumnRangeIterator rowColumnIterator = mock(RowColumnRangeIterator.class);
        List<byte[]> rows = ImmutableList.of(rowName);
        Map<byte[], RowColumnRangeIterator> expectedResult = ImmutableMap.of(rowName, rowColumnIterator);
        BatchColumnRangeSelection range = BatchColumnRangeSelection.create(colName, colName, 2);
        when(delegate.getRowsColumnRange(tableRef, rows, range, timestamp)).thenReturn(expectedResult);

        Map<byte[], RowColumnRangeIterator> result = kvs.getRowsColumnRange(tableRef, rows, range, timestamp);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getRowsColumnRange(test.testTable, 1 rows, Y29s,Y29s,2, ts 1)");
        verify(delegate).getRowsColumnRange(tableRef, rows, range, timestamp);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsColumnRange() throws Exception {
        RowColumnRangeIterator expectedResult = mock(RowColumnRangeIterator.class);
        List<byte[]> rows = ImmutableList.of(rowName);
        ColumnRangeSelection range = new ColumnRangeSelection(colName, colName);
        int cellBatchHint = 2;
        when(delegate.getRowsColumnRange(tableRef, rows, range, cellBatchHint, timestamp)).thenReturn(expectedResult);

        RowColumnRangeIterator result = kvs.getRowsColumnRange(tableRef, rows, range, cellBatchHint, timestamp);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getRowsColumnRange(test.testTable, 1 rows, Y29s,Y29s, 2 hint, ts 1)");
        verify(delegate).getRowsColumnRange(tableRef, rows, range, cellBatchHint, timestamp);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void multiPut() throws Exception {
        Map<TableReference, Map<Cell, byte[]>> values = ImmutableMap.of(tableRef, ImmutableMap.of(cell, contents));
        kvs.multiPut(values, timestamp);

        checkSpan("atlasdb-kvs.multiPut(1 values, ts 1)");
        verify(delegate).multiPut(values, timestamp);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void put() throws Exception {
        Map<Cell, byte[]> values = ImmutableMap.of(cell, contents);
        kvs.put(tableRef, values, timestamp);

        checkSpan("atlasdb-kvs.put(test.testTable, 1 values, ts 1)");
        verify(delegate).put(tableRef, values, timestamp);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putMetadataForTable() throws Exception {
        kvs.putMetadataForTable(tableRef, metadata);

        checkSpan("atlasdb-kvs.putMetadataForTable(test.testTable, 8 bytes)");
        verify(delegate).putMetadataForTable(tableRef, metadata);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putMetadataForTables() throws Exception {
        kvs.putMetadataForTables(ImmutableMap.of(tableRef, metadata));

        checkSpan("atlasdb-kvs.putMetadataForTables([test.testTable])");
        verify(delegate).putMetadataForTables(ImmutableMap.of(tableRef, metadata));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putUnlessExists() throws Exception {
        Map<Cell, byte[]> values = ImmutableMap.of(cell, contents);
        kvs.putUnlessExists(tableRef, values);

        checkSpan("atlasdb-kvs.putUnlessExists(test.testTable, 1 values)");
        verify(delegate).putUnlessExists(tableRef, values);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putWithTimestamps() throws Exception {
        Multimap<Cell, Value> values = ImmutableMultimap.of(cell, value);
        kvs.putWithTimestamps(tableRef, values);

        checkSpan("atlasdb-kvs.putWithTimestamps(test.testTable, 1 values)");
        verify(delegate).putWithTimestamps(tableRef, values);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void truncateTable() throws Exception {
        kvs.truncateTable(tableRef);

        checkSpan("atlasdb-kvs.truncateTable(test.testTable)");
        verify(delegate).truncateTable(tableRef);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void truncateTables() throws Exception {
        kvs.truncateTables(ImmutableSet.of(tableRef));

        checkSpan("atlasdb-kvs.truncateTables([test.testTable])");
        verify(delegate).truncateTables(ImmutableSet.of(tableRef));
        verifyNoMoreInteractions(delegate);
    }

    private void checkSpan(String opName) {
        assertThat(observer.spans(), hasSize(1));
        assertThat(observer.spans().get(0).getOperation(), equalTo(opName));
        assertThat(observer.spans().get(0).type(), equalTo(SpanType.LOCAL));
        assertThat(observer.spans().get(0).getDurationNanoSeconds(), greaterThanOrEqualTo(0L));
    }

}
