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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public final class TrackingKeyValueServiceTest {
    private static final long TIMESTAMP = 14L;
    private static final int MAX_RESULTS = 25;
    private static final ImmutableMap<Cell, Long> TIMESTAMP_BY_CELL_MAP =
            TrackingKeyValueServiceTestUtils.createLongByCellMapWithSize(121, TIMESTAMP);
    private static final Iterable<byte[]> ROWS =
            Stream.generate(() -> new byte[100]).limit(3).collect(Collectors.toUnmodifiableList());
    private static final TableReference TABLE_REF = TableReference.createWithEmptyNamespace("table1.namespace1");
    private static final RangeRequest RANGE_REQUEST = RangeRequest.all();
    private static final ImmutableList<RangeRequest> RANGE_REQUESTS = ImmutableList.of(
            RangeRequest.all(),
            TrackingKeyValueServiceTestUtils.createRangeRequest((byte) 1),
            TrackingKeyValueServiceTestUtils.createRangeRequest((byte) 2));
    private static final int CELL_BATCH_HINT = 2;
    private static final byte[] START_BYTES = TrackingKeyValueServiceTestUtils.createBytesWithSize(5, (byte) 1);
    private static final byte[] END_BYTES = TrackingKeyValueServiceTestUtils.createBytesWithSize(10, (byte) 1);
    private static final ColumnSelection COLUMN_SELECTION = ColumnSelection.create(ImmutableSet.of(
            TrackingKeyValueServiceTestUtils.createBytesWithSize(100, (byte) 0),
            TrackingKeyValueServiceTestUtils.createBytesWithSize(101, (byte) 1),
            TrackingKeyValueServiceTestUtils.createBytesWithSize(102, (byte) 2)));
    private static final ColumnRangeSelection COLUMN_RANGE_SELECTION = new ColumnRangeSelection(START_BYTES, END_BYTES);
    private static final BatchColumnRangeSelection BATCH_COLUMN_RANGE_SELECTION =
            BatchColumnRangeSelection.create(COLUMN_RANGE_SELECTION, CELL_BATCH_HINT);
    private static final CandidateCellForSweepingRequest CANDIDATE_CELL_FOR_SWEEPING_REQUEST =
            ImmutableCandidateCellForSweepingRequest.builder()
                    .startRowInclusive()
                    .maxTimestampExclusive(TIMESTAMP)
                    .shouldCheckIfLatestValueIsEmpty(false)
                    .shouldDeleteGarbageCollectionSentinels(false)
                    .build();
    private static final Set<Cell> CELLS = ImmutableSet.of(
            TrackingKeyValueServiceTestUtils.createCellWithSize(10, (byte) 0),
            TrackingKeyValueServiceTestUtils.createCellWithSize(20, (byte) 1),
            TrackingKeyValueServiceTestUtils.createCellWithSize(30, (byte) 2));

    @Parameterized.Parameters(name = "size = {0}")
    public static Object[] size() {
        // divisible by 12 to allow creation of TableReference of wanted sizes
        return new Object[] {12 * 129, 12 * 365, 12 * 411};
    }

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private final int size;
    private final byte[] bytesOfSize;
    private final ImmutableSet<TableReference> tableRefsOfSize;
    private final ImmutableMap<Cell, Long> timestampByCellMapOfSize;
    private final ImmutableList<byte[]> byteArrayListOfSize;
    private final ImmutableMap<Cell, Value> backingValueByCellMapOfSize;
    private final ImmutableMap<byte[], ImmutableMap<Cell, Value>> backingRowColumnRangeMapOfSize;
    private final ImmutableList<RowResult<Value>> backingValueRowResultListOfSize;
    private final ImmutableList<RowResult<Set<Long>>> backingLongSetRowResultListOfSize;
    private final ImmutableList<ImmutableList<CandidateCellForSweeping>> backingCandidateForSweepingTableOfSize;
    private final ImmutableMap<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>
            pageByRangeRequestMapOfSize;
    private final ImmutableMultimap<Cell, Long> longByCellMultimapOfSize;
    private final ImmutableMap<TableReference, byte[]> metadataForTablesOfSize;

    @Mock
    private KeyValueService kvs;

    private TrackingKeyValueService trackingKvs;

    public TrackingKeyValueServiceTest(int size) {
        this.size = size;
        this.bytesOfSize = new byte[size];
        this.tableRefsOfSize = TrackingKeyValueServiceTestUtils.createTableReferenceSetWithSize(size);
        this.timestampByCellMapOfSize = TrackingKeyValueServiceTestUtils.createLongByCellMapWithSize(size, 0L);
        this.byteArrayListOfSize = TrackingKeyValueServiceTestUtils.createListOfByteArraysWithSize(size);
        this.backingValueByCellMapOfSize = TrackingKeyValueServiceTestUtils.createValueByCellMapWithSize(size);
        this.backingRowColumnRangeMapOfSize = TrackingKeyValueServiceTestUtils.createRowColumnRangeMap(size);
        this.backingValueRowResultListOfSize = TrackingKeyValueServiceTestUtils.createValueRowResultListWithSize(size);
        this.backingLongSetRowResultListOfSize =
                TrackingKeyValueServiceTestUtils.createLongSetRowResultListWithSize(size);
        this.backingCandidateForSweepingTableOfSize =
                TrackingKeyValueServiceTestUtils.createCandidateCellForSweepingTableWithSize(size);
        this.pageByRangeRequestMapOfSize = TrackingKeyValueServiceTestUtils.createPageByRangeRequestMapWithSize(size);
        this.longByCellMultimapOfSize = TrackingKeyValueServiceTestUtils.createLongByCellMultimapWithSize(size);
        this.metadataForTablesOfSize =
                TrackingKeyValueServiceTestUtils.createByteArrayByTableReferenceMapWithSize(size);
    }

    @Before
    public void setUp() {
        this.trackingKvs = new TrackingKeyValueServiceImpl(kvs);
    }

    @Test
    public void getAsyncTest() throws ExecutionException, InterruptedException {
        when(kvs.getAsync(TABLE_REF, TIMESTAMP_BY_CELL_MAP))
                .thenReturn(Futures.immediateFuture(backingValueByCellMapOfSize));
        ListenableFuture<Map<Cell, Value>> future = trackingKvs.getAsync(TABLE_REF, TIMESTAMP_BY_CELL_MAP);

        assertThat(future.get()).isSameAs(backingValueByCellMapOfSize);

        verify(kvs, times(1)).getAsync(TABLE_REF, TIMESTAMP_BY_CELL_MAP);
        validateReadInfoForReadForTable("getAsync");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getRowsTest() {
        when(kvs.getRows(TABLE_REF, ROWS, COLUMN_SELECTION, TIMESTAMP)).thenReturn(backingValueByCellMapOfSize);

        assertThat(trackingKvs.getRows(TABLE_REF, ROWS, COLUMN_SELECTION, TIMESTAMP))
                .isSameAs(backingValueByCellMapOfSize);

        verify(kvs, times(1)).getRows(TABLE_REF, ROWS, COLUMN_SELECTION, TIMESTAMP);
        validateReadInfoForReadForTable("getRows");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getRowsBatchColumnRangeTest() {
        when(kvs.getRowsColumnRange(TABLE_REF, ROWS, BATCH_COLUMN_RANGE_SELECTION, TIMESTAMP))
                .thenReturn(createRowColumnRangeIteratorByByteArrayMapOfSize());

        Map<byte[], RowColumnRangeIterator> trackingKvsReturnValue =
                trackingKvs.getRowsColumnRange(TABLE_REF, ROWS, BATCH_COLUMN_RANGE_SELECTION, TIMESTAMP);

        assertThat(trackingKvsReturnValue).containsOnlyKeys(backingRowColumnRangeMapOfSize.keySet());
        for (Entry<byte[], ImmutableMap<Cell, Value>> entry : backingRowColumnRangeMapOfSize.entrySet()) {
            assertThat(trackingKvsReturnValue.get(entry.getKey()))
                    .toIterable()
                    .usingElementComparator(identityComparator())
                    .containsExactlyElementsOf(entry.getValue().entrySet());
        }

        verify(kvs, times(1)).getRowsColumnRange(TABLE_REF, ROWS, BATCH_COLUMN_RANGE_SELECTION, TIMESTAMP);
        validateReadInfoForPartialRead();
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getRowsColumnRangeTest() {
        when(kvs.getRowsColumnRange(TABLE_REF, ROWS, COLUMN_RANGE_SELECTION, CELL_BATCH_HINT, TIMESTAMP))
                .thenReturn(createRowColumnRangeIteratorOfSize());

        assertThat(trackingKvs.getRowsColumnRange(TABLE_REF, ROWS, COLUMN_RANGE_SELECTION, CELL_BATCH_HINT, TIMESTAMP))
                .toIterable()
                .usingElementComparator(identityComparator())
                .containsExactlyElementsOf(ImmutableList.copyOf(createRowColumnRangeIteratorOfSize()));

        verify(kvs, times(1)).getRowsColumnRange(TABLE_REF, ROWS, COLUMN_RANGE_SELECTION, CELL_BATCH_HINT, TIMESTAMP);
        validateReadInfoForPartialRead();
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getTest() {
        when(kvs.get(TABLE_REF, TIMESTAMP_BY_CELL_MAP)).thenReturn(backingValueByCellMapOfSize);

        assertThat(trackingKvs.get(TABLE_REF, TIMESTAMP_BY_CELL_MAP)).isSameAs(backingValueByCellMapOfSize);

        verify(kvs, times(1)).get(TABLE_REF, TIMESTAMP_BY_CELL_MAP);
        validateReadInfoForReadForTable("get");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getLatestTimestampsTest() {
        when(kvs.getLatestTimestamps(TABLE_REF, TIMESTAMP_BY_CELL_MAP)).thenReturn(timestampByCellMapOfSize);

        assertThat(trackingKvs.getLatestTimestamps(TABLE_REF, TIMESTAMP_BY_CELL_MAP))
                .isSameAs(timestampByCellMapOfSize);

        verify(kvs, times(1)).getLatestTimestamps(TABLE_REF, TIMESTAMP_BY_CELL_MAP);
        validateReadInfoForReadForTable("getLatestTimestamps");
        verifyNoMoreInteractions(kvs);
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test
    public void getRangeTest() {
        when(kvs.getRange(TABLE_REF, RANGE_REQUEST, TIMESTAMP))
                .thenReturn(createClosableValueRowResultIteratorOfSize());

        assertThat(trackingKvs.getRange(TABLE_REF, RANGE_REQUEST, TIMESTAMP))
                .toIterable()
                .usingElementComparator(identityComparator())
                .containsExactlyElementsOf(ImmutableList.copyOf(createClosableValueRowResultIteratorOfSize()));

        verify(kvs, times(1)).getRange(TABLE_REF, RANGE_REQUEST, TIMESTAMP);
        validateReadInfoForPartialRead();
        verifyNoMoreInteractions(kvs);
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test
    public void getRangeOfTimestampsTest() {
        when(kvs.getRangeOfTimestamps(TABLE_REF, RANGE_REQUEST, TIMESTAMP))
                .thenReturn(createClosableLongSetRowResultIteratorOfSize());

        assertThat(trackingKvs.getRangeOfTimestamps(TABLE_REF, RANGE_REQUEST, TIMESTAMP))
                .toIterable()
                .usingElementComparator(identityComparator())
                .containsExactlyElementsOf(ImmutableList.copyOf(createClosableLongSetRowResultIteratorOfSize()));

        verify(kvs, times(1)).getRangeOfTimestamps(TABLE_REF, RANGE_REQUEST, TIMESTAMP);
        validateReadInfoForPartialRead();
        verifyNoMoreInteractions(kvs);
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test
    public void getCandidateCellsForSweepingTest() {
        when(kvs.getCandidateCellsForSweeping(TABLE_REF, CANDIDATE_CELL_FOR_SWEEPING_REQUEST))
                .thenReturn(createClosableCandidateCellForSweepingListIteratorOfSize());

        assertThat(trackingKvs.getCandidateCellsForSweeping(TABLE_REF, CANDIDATE_CELL_FOR_SWEEPING_REQUEST))
                .toIterable()
                .usingElementComparator(identityComparator())
                .containsExactlyElementsOf(
                        ImmutableList.copyOf(createClosableCandidateCellForSweepingListIteratorOfSize()));

        verify(kvs, times(1)).getCandidateCellsForSweeping(TABLE_REF, CANDIDATE_CELL_FOR_SWEEPING_REQUEST);
        validateReadInfoForPartialRead();
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getFirstBatchForRangesTest() {
        when(kvs.getFirstBatchForRanges(TABLE_REF, RANGE_REQUESTS, TIMESTAMP)).thenReturn(pageByRangeRequestMapOfSize);

        assertThat(trackingKvs.getFirstBatchForRanges(TABLE_REF, RANGE_REQUESTS, TIMESTAMP))
                .isSameAs(pageByRangeRequestMapOfSize);

        verify(kvs, times(1)).getFirstBatchForRanges(TABLE_REF, RANGE_REQUESTS, TIMESTAMP);
        validateReadInfoForReadForTable("getFirstBatchForRanges");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getAllTableNamesTest() {
        when(kvs.getAllTableNames()).thenReturn(tableRefsOfSize);

        assertThat(trackingKvs.getAllTableNames()).isSameAs(tableRefsOfSize);

        verify(kvs, times(1)).getAllTableNames();
        validateReadInfoForTableAgnosticRead("getAllTableNames");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getMetadataForTableTest() {
        when(kvs.getMetadataForTable(TABLE_REF)).thenReturn(bytesOfSize);

        assertThat(trackingKvs.getMetadataForTable(TABLE_REF)).isSameAs(bytesOfSize);

        verify(kvs, times(1)).getMetadataForTable(TABLE_REF);
        validateReadInfoForTableAgnosticRead("getMetadataForTable");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getMetadataForTablesTest() {
        when(kvs.getMetadataForTables()).thenReturn(metadataForTablesOfSize);

        assertThat(trackingKvs.getMetadataForTables()).isSameAs(metadataForTablesOfSize);

        verify(kvs, times(1)).getMetadataForTables();
        validateReadInfoForTableAgnosticRead("getMetadataForTables");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getAllTimestampsTest() {
        when(kvs.getAllTimestamps(TABLE_REF, CELLS, TIMESTAMP)).thenReturn(longByCellMultimapOfSize);

        assertThat(trackingKvs.getAllTimestamps(TABLE_REF, CELLS, TIMESTAMP)).isSameAs(longByCellMultimapOfSize);

        verify(kvs, times(1)).getAllTimestamps(TABLE_REF, CELLS, TIMESTAMP);
        validateReadInfoForReadForTable("getAllTimestamps");
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void getRowsKeyInRangeTest() {
        when(kvs.getRowKeysInRange(TABLE_REF, START_BYTES, END_BYTES, MAX_RESULTS))
                .thenReturn(byteArrayListOfSize);

        assertThat(trackingKvs.getRowKeysInRange(TABLE_REF, START_BYTES, END_BYTES, MAX_RESULTS))
                .isSameAs(byteArrayListOfSize);

        verify(kvs, times(1)).getRowKeysInRange(TABLE_REF, START_BYTES, END_BYTES, MAX_RESULTS);
        validateReadInfoForReadForTable("getRowKeysInRange");
        verifyNoMoreInteractions(kvs);
    }

    private void validateReadInfoForReadForTable(String methodName) {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .methodName(methodName)
                        .bytesRead(size)
                        .build())
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).containsOnlyKeys(List.of(TABLE_REF));
    }

    private void validateReadInfoForPartialRead() {
        TransactionReadInfo readInfoForPartialReads = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfoForPartialReads);
        assertThat(trackingKvs.getReadInfoByTable()).containsOnlyKeys(List.of(TABLE_REF));
    }

    private void validateReadInfoForTableAgnosticRead(String methodName) {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .methodName(methodName)
                        .bytesRead(size)
                        .build())
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).isEmpty();
    }

    private Map<byte[], RowColumnRangeIterator> createRowColumnRangeIteratorByByteArrayMapOfSize() {
        return backingRowColumnRangeMapOfSize.entrySet().stream()
                .map(entry -> new SimpleEntry<>(
                        entry.getKey(),
                        new LocalRowColumnRangeIterator(
                                entry.getValue().entrySet().iterator())))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private RowColumnRangeIterator createRowColumnRangeIteratorOfSize() {
        return new LocalRowColumnRangeIterator(
                backingValueByCellMapOfSize.entrySet().stream().iterator());
    }

    private ClosableIterator<RowResult<Value>> createClosableValueRowResultIteratorOfSize() {
        return ClosableIterators.wrapWithEmptyClose(
                backingValueRowResultListOfSize.stream().iterator());
    }

    private ClosableIterator<List<CandidateCellForSweeping>>
            createClosableCandidateCellForSweepingListIteratorOfSize() {
        return ClosableIterators.wrapWithEmptyClose(
                backingCandidateForSweepingTableOfSize.stream().iterator());
    }

    private ClosableIterator<RowResult<Set<Long>>> createClosableLongSetRowResultIteratorOfSize() {
        return ClosableIterators.wrapWithEmptyClose(
                backingLongSetRowResultListOfSize.stream().iterator());
    }

    // this is in breach with comparator invariants but is only used for equality
    // todo(aalouane): I do not know how to do this differently
    private static <T> Comparator<T> identityComparator() {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }
            return 1;
        };
    }
}
