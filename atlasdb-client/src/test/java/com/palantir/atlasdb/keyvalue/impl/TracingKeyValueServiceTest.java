/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Optional;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Charsets;
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
import com.palantir.remoting2.tracing.SpanType;
import com.palantir.remoting2.tracing.Tracer;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@RunWith(MockitoJUnitRunner.class)
public class TracingKeyValueServiceTest {

    private static final Namespace NAMESPACE = Namespace.create("test");
    private static final byte[] ROW_NAME = "row".getBytes(Charsets.UTF_8);
    private static final byte[] COL_NAME = "col".getBytes(Charsets.UTF_8);
    private static final TableReference TABLE_REF = TableReference.create(NAMESPACE, "testTable");
    private static final Cell CELL = Cell.create(ROW_NAME, COL_NAME);
    private static final RangeRequest RANGE_REQUEST = RangeRequest.all();
    private static final ImmutableList<RangeRequest> RANGE_REQUESTS = ImmutableList.of(RANGE_REQUEST);
    private static final long TIMESTAMP = 1L;
    private static final byte[] VALUE_BYTES = "value".getBytes(Charsets.UTF_8);
    private static final Value VALUE = Value.create(VALUE_BYTES, TIMESTAMP);
    private static final byte[] METADATA_BYTES = "metadata".getBytes(Charsets.UTF_8);

    private KeyValueService delegate;
    private KeyValueService kvs;

    private final TestSpanObserver observer = new TestSpanObserver();

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
        ImmutableSet<Cell> cells = ImmutableSet.of(CELL);
        kvs.addGarbageCollectionSentinelValues(TABLE_REF, cells);

        checkSpan("atlasdb-kvs.addGarbageCollectionSentinelValues(test.testTable, 1 cells)");
        verify(delegate).addGarbageCollectionSentinelValues(TABLE_REF, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void checkAndSet() throws Exception {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(TABLE_REF, CELL, ROW_NAME, ROW_NAME);
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
        kvs.compactInternally(TABLE_REF);

        checkSpan("atlasdb-kvs.compactInternally(test.testTable)");
        verify(delegate).compactInternally(TABLE_REF);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void createTable() throws Exception {
        byte[] metadata = new byte[0];
        kvs.createTable(TABLE_REF, metadata);

        checkSpan("atlasdb-kvs.createTable(test.testTable)");
        verify(delegate).createTable(TABLE_REF, metadata);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void createTables() throws Exception {
        byte[] metadata = new byte[0];
        kvs.createTables(ImmutableMap.of(TABLE_REF, metadata));

        checkSpan("atlasdb-kvs.createTables([test.testTable])");
        verify(delegate).createTables(ImmutableMap.of(TABLE_REF, metadata));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void delete() throws Exception {
        Multimap<Cell, Long> cells = ImmutableMultimap.of(CELL, 1L);
        kvs.delete(TABLE_REF, cells);

        checkSpan("atlasdb-kvs.delete(test.testTable, 1 keys)");
        verify(delegate).delete(TABLE_REF, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void dropTable() throws Exception {
        kvs.dropTable(TABLE_REF);

        checkSpan("atlasdb-kvs.dropTable(test.testTable)");
        verify(delegate).dropTable(TABLE_REF);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void dropTables() throws Exception {
        kvs.dropTables(ImmutableSet.of(TABLE_REF));

        checkSpan("atlasdb-kvs.dropTables([test.testTable])");
        verify(delegate).dropTables(ImmutableSet.of(TABLE_REF));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void get() throws Exception {
        Map<Cell, Value> expectedResult = ImmutableMap.of(CELL, VALUE);
        Map<Cell, Long> cells = ImmutableMap.of(CELL, 1L);
        when(delegate.get(TABLE_REF, cells)).thenReturn(expectedResult);

        Map<Cell, Value> result = kvs.get(TABLE_REF, cells);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.get(test.testTable, 1 cells)");
        verify(delegate).get(TABLE_REF, cells);
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
        Set<Cell> cells = ImmutableSet.of(CELL);
        kvs.getAllTimestamps(TABLE_REF, cells, 1L);

        checkSpan("atlasdb-kvs.getAllTimestamps(test.testTable, 1 keys, ts 1)");
        verify(delegate).getAllTimestamps(TABLE_REF, cells, 1L);
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
                TABLE_REF, RANGE_REQUESTS, TIMESTAMP);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getFirstBatchForRanges(test.testTable, 1 ranges, ts 1)");
        verify(delegate).getFirstBatchForRanges(TABLE_REF, RANGE_REQUESTS, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getLatestTimestamps() throws Exception {
        Map<Cell, Long> cells = ImmutableMap.of(CELL, TIMESTAMP);
        when(delegate.getLatestTimestamps(TABLE_REF, cells)).thenReturn(cells);

        Map<Cell, Long> result = kvs.getLatestTimestamps(TABLE_REF, cells);

        assertThat(result.entrySet(), hasSize(1));
        checkSpan("atlasdb-kvs.getLatestTimestamps(test.testTable, 1 cells)");
        verify(delegate).getLatestTimestamps(TABLE_REF, cells);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMetadataForTable() throws Exception {
        when(delegate.getMetadataForTable(TABLE_REF)).thenReturn(METADATA_BYTES);

        byte[] result = kvs.getMetadataForTable(TABLE_REF);

        assertThat(result, equalTo(METADATA_BYTES));
        checkSpan("atlasdb-kvs.getMetadataForTable(test.testTable)");
        verify(delegate).getMetadataForTable(TABLE_REF);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMetadataForTables() throws Exception {
        Map<TableReference, byte[]> expectedResult = ImmutableMap.of(TABLE_REF, METADATA_BYTES);
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
                RowResult.of(CELL, VALUE)).iterator());
        when(delegate.getRange(TABLE_REF, RANGE_REQUEST, TIMESTAMP)).thenReturn(expectedResult);
        try (ClosableIterator<RowResult<Value>> result = kvs.getRange(TABLE_REF, RANGE_REQUEST, TIMESTAMP)) {
            assertThat(result, equalTo(expectedResult));
            checkSpan("atlasdb-kvs.getRange(test.testTable, ts 1)");
            verify(delegate).getRange(TABLE_REF, RANGE_REQUEST, TIMESTAMP);
            verifyNoMoreInteractions(delegate);
        }
    }

    @Test
    public void getRangeOfTimestamps() throws Exception {
        Set<Long> longs = ImmutableSet.of(TIMESTAMP);
        ClosableIterator<RowResult<Set<Long>>> expectedResult = ClosableIterators.wrap(ImmutableList.of(
                RowResult.of(CELL, longs)).iterator());
        when(delegate.getRangeOfTimestamps(TABLE_REF, RANGE_REQUEST, TIMESTAMP)).thenReturn(expectedResult);
        try (ClosableIterator<RowResult<Set<Long>>> result = kvs.getRangeOfTimestamps(
                TABLE_REF, RANGE_REQUEST, TIMESTAMP)) {
            assertThat(result, equalTo(expectedResult));
            checkSpan("atlasdb-kvs.getRangeOfTimestamps(test.testTable, ts 1)");
            verify(delegate).getRangeOfTimestamps(TABLE_REF, RANGE_REQUEST, TIMESTAMP);
            verifyNoMoreInteractions(delegate);
        }
    }

    @Test
    public void getRows() throws Exception {
        ImmutableList<byte[]> rows = ImmutableList.of(ROW_NAME);
        Map<Cell, Value> expectedResult = ImmutableMap.of();
        when(delegate.getRows(TABLE_REF, rows, ColumnSelection.all(), TIMESTAMP)).thenReturn(expectedResult);

        Map<Cell, Value> result = kvs.getRows(TABLE_REF, rows, ColumnSelection.all(), TIMESTAMP);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getRows(test.testTable, 1 rows, ts 1)");
        verify(delegate).getRows(TABLE_REF, rows, ColumnSelection.all(), TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsColumnRangeBatch() throws Exception {
        RowColumnRangeIterator rowColumnIterator = mock(RowColumnRangeIterator.class);
        List<byte[]> rows = ImmutableList.of(ROW_NAME);
        Map<byte[], RowColumnRangeIterator> expectedResult = ImmutableMap.of(ROW_NAME, rowColumnIterator);
        BatchColumnRangeSelection range = BatchColumnRangeSelection.create(COL_NAME, COL_NAME, 2);
        when(delegate.getRowsColumnRange(TABLE_REF, rows, range, TIMESTAMP)).thenReturn(expectedResult);

        Map<byte[], RowColumnRangeIterator> result = kvs.getRowsColumnRange(TABLE_REF, rows, range, TIMESTAMP);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getRowsColumnRange(test.testTable, 1 rows, ts 1)");
        verify(delegate).getRowsColumnRange(TABLE_REF, rows, range, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsColumnRange() throws Exception {
        RowColumnRangeIterator expectedResult = mock(RowColumnRangeIterator.class);
        List<byte[]> rows = ImmutableList.of(ROW_NAME);
        ColumnRangeSelection range = new ColumnRangeSelection(COL_NAME, COL_NAME);
        int cellBatchHint = 2;
        when(delegate.getRowsColumnRange(TABLE_REF, rows, range, cellBatchHint, TIMESTAMP)).thenReturn(expectedResult);

        RowColumnRangeIterator result = kvs.getRowsColumnRange(TABLE_REF, rows, range, cellBatchHint, TIMESTAMP);

        assertThat(result, equalTo(expectedResult));
        checkSpan("atlasdb-kvs.getRowsColumnRange(test.testTable, 1 rows, 2 hint, ts 1)");
        verify(delegate).getRowsColumnRange(TABLE_REF, rows, range, cellBatchHint, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void multiPut() throws Exception {
        Map<TableReference, Map<Cell, byte[]>> values = ImmutableMap.of(TABLE_REF, ImmutableMap.of(CELL, VALUE_BYTES));
        kvs.multiPut(values, TIMESTAMP);

        checkSpan("atlasdb-kvs.multiPut(1 values, ts 1)");
        verify(delegate).multiPut(values, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void put() throws Exception {
        Map<Cell, byte[]> values = ImmutableMap.of(CELL, VALUE_BYTES);
        kvs.put(TABLE_REF, values, TIMESTAMP);

        checkSpan("atlasdb-kvs.put(test.testTable, 1 values, ts 1)");
        verify(delegate).put(TABLE_REF, values, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putMetadataForTable() throws Exception {
        kvs.putMetadataForTable(TABLE_REF, METADATA_BYTES);

        checkSpan("atlasdb-kvs.putMetadataForTable(test.testTable, 8 bytes)");
        verify(delegate).putMetadataForTable(TABLE_REF, METADATA_BYTES);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putMetadataForTables() throws Exception {
        kvs.putMetadataForTables(ImmutableMap.of(TABLE_REF, METADATA_BYTES));

        checkSpan("atlasdb-kvs.putMetadataForTables([test.testTable])");
        verify(delegate).putMetadataForTables(ImmutableMap.of(TABLE_REF, METADATA_BYTES));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putUnlessExists() throws Exception {
        Map<Cell, byte[]> values = ImmutableMap.of(CELL, VALUE_BYTES);
        kvs.putUnlessExists(TABLE_REF, values);

        checkSpan("atlasdb-kvs.putUnlessExists(test.testTable, 1 values)");
        verify(delegate).putUnlessExists(TABLE_REF, values);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void putWithTimestamps() throws Exception {
        Multimap<Cell, Value> values = ImmutableMultimap.of(CELL, VALUE);
        kvs.putWithTimestamps(TABLE_REF, values);

        checkSpan("atlasdb-kvs.putWithTimestamps(test.testTable, 1 values)");
        verify(delegate).putWithTimestamps(TABLE_REF, values);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void truncateTable() throws Exception {
        kvs.truncateTable(TABLE_REF);

        checkSpan("atlasdb-kvs.truncateTable(test.testTable)");
        verify(delegate).truncateTable(TABLE_REF);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void truncateTables() throws Exception {
        kvs.truncateTables(ImmutableSet.of(TABLE_REF));

        checkSpan("atlasdb-kvs.truncateTables([test.testTable])");
        verify(delegate).truncateTables(ImmutableSet.of(TABLE_REF));
        verifyNoMoreInteractions(delegate);
    }

    private void checkSpan(String opName) {
        assertThat(observer.spans(), hasSize(1));
        assertThat(observer.spans().get(0).getOperation(), equalTo(opName));
        assertThat(observer.spans().get(0).type(), equalTo(SpanType.LOCAL));
        assertThat(observer.spans().get(0).getDurationNanoSeconds(), greaterThanOrEqualTo(0L));
    }

}
