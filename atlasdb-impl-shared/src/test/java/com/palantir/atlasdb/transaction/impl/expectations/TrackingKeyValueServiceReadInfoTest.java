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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public final class TrackingKeyValueServiceReadInfoTest {
    @Parameterized.Parameters(name = "size = {0}")
    public static Object[] size() {
        // divisible by 12 to allow creation of TableReference objects of wanted sizes
        return new Object[] {12 * 129, 12 * 365, 12 * 411};
    }

    private static final long TIMESTAMP = 14L;

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private List<byte[]> rows;

    @Mock
    private RangeRequest rangeRequest;

    @Mock
    private KeyValueService kvs;

    private final int size;
    private TrackingKeyValueService trackingKvs;
    private final ImmutableMap<Cell, Value> valueByCellMapOfSize;

    public TrackingKeyValueServiceReadInfoTest(int size) {
        this.size = size;
        this.valueByCellMapOfSize = TrackingKeyValueServiceTestUtils.createValueByCellMapWithSize(size);
    }

    @Before
    public void setUp() {
        this.trackingKvs = new TrackingKeyValueServiceImpl(kvs);
    }

    @Mock
    private TableReference tableReference;

    @Mock
    private Map<Cell, Long> timestampByCellMap;

    @Test
    public void readInfoIsCorrectAfterGetAsyncCallAndFutureConsumption()
            throws ExecutionException, InterruptedException {
        when(kvs.getAsync(tableReference, timestampByCellMap))
                .thenReturn(Futures.immediateFuture(valueByCellMapOfSize));

        trackingKvs.getAsync(tableReference, timestampByCellMap).get();

        validateReadInfoForReadForTable("getAsync");
    }

    @Test
    public void readInfoIsCorrectAfterGetRowsCall() {
        ColumnSelection columnSelection = mock(ColumnSelection.class);
        when(kvs.getRows(tableReference, rows, columnSelection, TIMESTAMP)).thenReturn(valueByCellMapOfSize);

        trackingKvs.getRows(tableReference, rows, columnSelection, TIMESTAMP);

        validateReadInfoForReadForTable("getRows");
    }

    @Test
    public void readInfoIsCorrectAfterGetRowsBatchColumnRangeCallAndIteratorsConsumption() {
        BatchColumnRangeSelection batchColumnRangeSelection = mock(BatchColumnRangeSelection.class);
        when(kvs.getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP))
                .thenReturn(TrackingKeyValueServiceTestUtils.createRowColumnRangeIteratorByByteArrayMutableMapWithSize(
                        size));

        trackingKvs
                .getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP)
                .values()
                .forEach(iterator -> iterator.forEachRemaining(_unused -> {}));

        validateReadInfoForLazyRead();
    }

    @Test
    public void readInfoIsCorrectAfterGetRowsColumnRangeCallAndIteratorConsumption() {
        int cellBatchHint = 17;
        ColumnRangeSelection columnRangeSelection = mock(ColumnRangeSelection.class);
        when(kvs.getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP))
                .thenReturn(new LocalRowColumnRangeIterator(
                        valueByCellMapOfSize.entrySet().iterator()));

        trackingKvs
                .getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP)
                .forEachRemaining(_unused -> {});

        validateReadInfoForLazyRead();
    }

    @Test
    public void readInfoIsCorrectAfterGetCall() {
        when(kvs.get(tableReference, timestampByCellMap)).thenReturn(valueByCellMapOfSize);

        trackingKvs.get(tableReference, timestampByCellMap);

        validateReadInfoForReadForTable("get");
    }

    @Test
    public void readInfoIsCorrectAfterGetLatestTimestampsCall() {
        Map<Cell, Long> timestampByCellMapOfSize =
                TrackingKeyValueServiceTestUtils.createLongByCellMapWithSize(size, 0L);
        when(kvs.getLatestTimestamps(tableReference, timestampByCellMap)).thenReturn(timestampByCellMapOfSize);

        trackingKvs.getLatestTimestamps(tableReference, timestampByCellMap);

        validateReadInfoForReadForTable("getLatestTimestamps");
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test
    public void readInfoIsCorrectAfterGetRangeCallAndIteratorConsumption() {
        List<RowResult<Value>> backingValueRowResultListOfSize =
                TrackingKeyValueServiceTestUtils.createValueRowResultListWithSize(size);

        when(kvs.getRange(tableReference, rangeRequest, TIMESTAMP))
                .thenReturn(ClosableIterators.wrapWithEmptyClose(backingValueRowResultListOfSize.iterator()));

        trackingKvs.getRange(tableReference, rangeRequest, TIMESTAMP).forEachRemaining(_unused -> {});

        validateReadInfoForLazyRead();
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test
    public void readInfoIsCorrectAfterGetRangeOfTimestampsCallAndIteratorConsumption() {
        List<RowResult<Set<Long>>> backingLongSetRowResultListOfSize =
                TrackingKeyValueServiceTestUtils.createLongSetRowResultListWithSize(size);

        when(kvs.getRangeOfTimestamps(tableReference, rangeRequest, TIMESTAMP))
                .thenReturn(ClosableIterators.wrapWithEmptyClose(backingLongSetRowResultListOfSize.iterator()));

        trackingKvs
                .getRangeOfTimestamps(tableReference, rangeRequest, TIMESTAMP)
                .forEachRemaining(_unused -> {});

        validateReadInfoForLazyRead();
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test
    public void readInfoIsCorrectAfterGetCandidateCellsForSweepingCallAndIteratorConsumption() {
        CandidateCellForSweepingRequest candidateCellForSweepingRequest = mock(CandidateCellForSweepingRequest.class);

        ImmutableList<ImmutableList<CandidateCellForSweeping>> backingCandidateForSweepingTableOfSize =
                TrackingKeyValueServiceTestUtils.createCandidateCellForSweepingTableWithSize(size);

        when(kvs.getCandidateCellsForSweeping(tableReference, candidateCellForSweepingRequest))
                .thenReturn(ClosableIterators.wrapWithEmptyClose(backingCandidateForSweepingTableOfSize.iterator()));

        trackingKvs
                .getCandidateCellsForSweeping(tableReference, candidateCellForSweepingRequest)
                .forEachRemaining(_unused -> {});

        validateReadInfoForLazyRead();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void readInfoIsCorrectAfterGetFirstBatchForRangesCall() {
        Iterable<RangeRequest> rangeRequests = mock(Iterable.class);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRangeRequestMapOfSize =
                TrackingKeyValueServiceTestUtils.createPageByRangeRequestMapWithSize(size);

        when(kvs.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP))
                .thenReturn(pageByRangeRequestMapOfSize);

        trackingKvs.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP);

        validateReadInfoForReadForTable("getFirstBatchForRanges");
    }

    @Test
    public void readInfoIsCorrectAfterGetAllTableNamesCall() {
        when(kvs.getAllTableNames()).thenReturn(TrackingKeyValueServiceTestUtils.createTableReferenceSetWithSize(size));

        trackingKvs.getAllTableNames();

        validateReadInfoForTableAgnosticRead("getAllTableNames");
    }

    @Test
    public void readInfoIsCorrectAfterGetMetadataForTableCall() {
        when(kvs.getMetadataForTable(tableReference)).thenReturn(new byte[size]);

        trackingKvs.getMetadataForTable(tableReference);

        validateReadInfoForTableAgnosticRead("getMetadataForTable");
    }

    @Test
    public void readInfoIsCorrectAfterGetMetadataForTablesCall() {
        Map<TableReference, byte[]> metadataForTablesOfSize =
                TrackingKeyValueServiceTestUtils.createByteArrayByTableReferenceMapWithSize(size);

        when(kvs.getMetadataForTables()).thenReturn(metadataForTablesOfSize);

        trackingKvs.getMetadataForTables();

        validateReadInfoForTableAgnosticRead("getMetadataForTables");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void readInfoIsCorrectAfterGetAllTimestampsCall() {
        Set<Cell> cells = mock(Set.class);
        Multimap<Cell, Long> longByCellMultimapOfSize =
                TrackingKeyValueServiceTestUtils.createLongByCellMultimapWithSize(size);

        when(kvs.getAllTimestamps(tableReference, cells, TIMESTAMP)).thenReturn(longByCellMultimapOfSize);

        trackingKvs.getAllTimestamps(tableReference, cells, TIMESTAMP);

        validateReadInfoForReadForTable("getAllTimestamps");
    }

    @Test
    public void readInfoIsCorrectAfterGetRowsKeysInRangeCall() {
        int maxResults = 13;
        byte[] startRow = new byte[1];
        byte[] endRow = new byte[3];
        ImmutableList<byte[]> byteArrayListOfSize =
                TrackingKeyValueServiceTestUtils.createListOfByteArraysWithSize(size);

        when(kvs.getRowKeysInRange(tableReference, startRow, endRow, maxResults))
                .thenReturn(byteArrayListOfSize);

        trackingKvs.getRowKeysInRange(tableReference, startRow, endRow, maxResults);

        validateReadInfoForReadForTable("getRowKeysInRange");
    }

    private void validateReadInfoForReadForTable(String methodName) {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of(methodName, size))
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).containsOnlyKeys(ImmutableList.of(tableReference));
    }

    private void validateReadInfoForLazyRead() {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).containsOnlyKeys(ImmutableList.of(tableReference));
    }

    private void validateReadInfoForTableAgnosticRead(String methodName) {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of(methodName, size))
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).isEmpty();
    }
}
