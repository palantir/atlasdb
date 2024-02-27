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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.streams.KeyedStream;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Broadly tests that {@link TrackingTransactionKeyValueServiceImpl} methods forward delegate results using: physical equality
 * when relevant, and element/component physical equality when the semantics are different (methods returning
 * iterators or collections of iterators).
 * Mocks are used whenever possible. Exceptions include: types which cannot be mocked (e.g. byte arrays),
 * interfaces/implementations annotated with {@link org.mockito.DoNotMock} or similar (e.g. {@link Multimap}), and
 * when the method semantics and resulting tests impose it (e.g. methods where an iterator is wrapped can only be tested
 * by equality/in-order testing of its components, {@link TrackingTransactionKeyValueServiceImpl#getCandidateCellsForSweeping}).
 */
@ExtendWith(MockitoExtension.class)
public final class TrackingTransactionKeyValueServiceForwardingTest {
    private static final long TIMESTAMP = 12L;
    private static final byte[] BYTES_1 = new byte[1];
    private static final byte[] BYTES_2 = new byte[2];
    private static final Cell CELL_1 = Cell.create(BYTES_1, BYTES_1);
    private static final Cell CELL_2 = Cell.create(BYTES_2, BYTES_2);
    private static final Value VALUE_1 = Value.create(BYTES_1, TIMESTAMP);
    private static final Value VALUE_2 = Value.create(BYTES_2, TIMESTAMP);

    @Mock
    private TableReference tableReference;

    @Mock
    private RangeRequest rangeRequest;

    @Mock
    private List<byte[]> rows;

    @Mock
    private Map<Cell, Long> timestampByCellMap;

    @Mock
    private Map<Cell, Value> valueByCellMap;

    @Mock
    private TransactionKeyValueService delegate;

    private TrackingTransactionKeyValueService trackingKvs;

    @BeforeEach
    public void setUp() {
        trackingKvs = new TrackingTransactionKeyValueServiceImpl(delegate);
    }

    @Test
    public void getAsyncWrapsAndForwardsDelegateFutureResult() throws ExecutionException, InterruptedException {
        when(delegate.getAsync(tableReference, timestampByCellMap)).thenReturn(Futures.immediateFuture(valueByCellMap));

        assertThat(trackingKvs.getAsync(tableReference, timestampByCellMap).get())
                .isSameAs(valueByCellMap);
    }

    @Test
    public void getAsyncResultCompletesAndForwardsIffDelegateResultCompletes()
            throws ExecutionException, InterruptedException {
        SettableFuture<Map<Cell, Value>> delegateFuture = SettableFuture.create();
        when(delegate.getAsync(tableReference, timestampByCellMap)).thenReturn(delegateFuture);

        ListenableFuture<Map<Cell, Value>> future = trackingKvs.getAsync(tableReference, timestampByCellMap);
        assertThat(future).isNotDone();

        // completes the delegate future
        delegateFuture.set(valueByCellMap);

        assertThat(future).isDone();
        assertThat(future.get()).isSameAs(valueByCellMap);
    }

    @Test
    public void getAsyncResultThrowsIfDelegateResultThrows() {
        Exception exception = new RuntimeException();
        ListenableFuture<Map<Cell, Value>> delegateFuture = Futures.immediateFailedFuture(exception);
        when(delegate.getAsync(tableReference, timestampByCellMap)).thenReturn(delegateFuture);

        ListenableFuture<Map<Cell, Value>> future = trackingKvs.getAsync(tableReference, timestampByCellMap);
        assertThatThrownBy(future::get).hasCause(exception);
    }

    @Test
    public void getRowsForwardsDelegateResult() {
        ColumnSelection columnSelection = mock(ColumnSelection.class);
        when(delegate.getRows(tableReference, rows, columnSelection, TIMESTAMP)).thenReturn(valueByCellMap);

        assertThat(trackingKvs.getRows(tableReference, rows, columnSelection, TIMESTAMP))
                .isSameAs(valueByCellMap);
    }

    @Test
    public void getRowsBatchColumnRangeForwardsDelegateResult() {
        BatchColumnRangeSelection batchColumnRangeSelection = mock(BatchColumnRangeSelection.class);

        Map<byte[], List<Entry<Cell, Value>>> backingRowColumnRangeMap = ImmutableMap.of(
                BYTES_1, ImmutableList.of(new SimpleEntry<>(CELL_1, VALUE_1)),
                BYTES_2, ImmutableList.of(new SimpleEntry<>(CELL_2, VALUE_2)));

        when(delegate.getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP))
                .thenReturn(convertMapValuesToRowColumnRangeIterators(backingRowColumnRangeMap));

        Map<byte[], RowColumnRangeIterator> rowsColumnRangeMap =
                trackingKvs.getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP);

        assertThat(rowsColumnRangeMap).containsOnlyKeys(backingRowColumnRangeMap.keySet());
        for (Entry<byte[], List<Entry<Cell, Value>>> entry : backingRowColumnRangeMap.entrySet()) {
            assertThat(rowsColumnRangeMap.get(entry.getKey()))
                    .toIterable()
                    .usingElementComparator(identityComparator())
                    .containsExactlyElementsOf(entry.getValue());
        }
    }

    @Test
    public void getForwardsDelegateResult() {
        when(delegate.get(tableReference, timestampByCellMap)).thenReturn(valueByCellMap);
        assertThat(trackingKvs.get(tableReference, timestampByCellMap)).isSameAs(valueByCellMap);
    }

    @Test
    public void getRowsColumnRangeWrapsAndForwardsDelegateResult() {
        int cellBatchHint = 12;
        ColumnRangeSelection columnRangeSelection = mock(ColumnRangeSelection.class);
        List<Entry<Cell, Value>> valueByCellEntries =
                ImmutableList.of(new SimpleEntry<>(CELL_1, VALUE_1), new SimpleEntry<>(CELL_2, VALUE_2));

        when(delegate.getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP))
                .thenReturn(new LocalRowColumnRangeIterator(valueByCellEntries.iterator()));

        assertThat(trackingKvs.getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP))
                .toIterable()
                .usingElementComparator(identityComparator())
                .containsExactlyElementsOf(valueByCellEntries);
    }

    @Test
    public void getLatestTimestampsForwardsDelegateResult() {
        Map<Cell, Long> timestampByCellMapAsReturnValue = mock(Map.class);
        when(delegate.getLatestTimestamps(tableReference, timestampByCellMap))
                .thenReturn(timestampByCellMapAsReturnValue);
        assertThat(trackingKvs.getLatestTimestamps(tableReference, timestampByCellMap))
                .isSameAs(timestampByCellMapAsReturnValue);
    }

    @Test
    @SuppressWarnings("MustBeClosedChecker")
    public void getRangeWrapsAndForwardsDelegateResult() {
        List<RowResult<Value>> rowResults =
                ImmutableList.of(RowResult.of(CELL_1, VALUE_1), RowResult.of(CELL_1, VALUE_2));

        when(delegate.getRange(tableReference, rangeRequest, TIMESTAMP))
                .thenReturn(ClosableIterators.wrapWithEmptyClose(rowResults.iterator()));

        assertThat(trackingKvs.getRange(tableReference, rangeRequest, TIMESTAMP))
                .toIterable()
                .usingElementComparator(identityComparator())
                .containsExactlyElementsOf(rowResults);
    }

    @Test
    public void getFirstBatchForRangesForwardsDelegateResult() {
        Iterable<RangeRequest> rangeRequests = mock(Iterable.class);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> batchForRangesMap = mock(Map.class);
        when(delegate.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP))
                .thenReturn(batchForRangesMap);
        assertThat(trackingKvs.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP))
                .isSameAs(batchForRangesMap);
    }

    private static Map<byte[], RowColumnRangeIterator> convertMapValuesToRowColumnRangeIterators(
            Map<byte[], ? extends List<Entry<Cell, Value>>> map) {
        return KeyedStream.stream(map)
                .map(List::iterator)
                .<RowColumnRangeIterator>map(LocalRowColumnRangeIterator::new)
                .collectToMap();
    }

    /**
     * This is clearly in violation of the {@link Comparator} contract and should only be used for equality testing.
     * It is used to test that iterables/iterators have the same objects in order with respect to == (in idiomatic
     * AssertJ). In the absence of an iterable equivalent to {@link org.assertj.core.api.AbstractAssert#isSameAs}, we
     * use this as a custom comparator fed to {@link org.assertj.core.api.IterableAssert#usingElementComparator}.
     */
    private static <T> Comparator<T> identityComparator() {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }
            return 1;
        };
    }
}
