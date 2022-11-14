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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.common.base.ClosableIterators;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class TrackingKeyValueServiceDelegationTest {
    private static final long TIMESTAMP = 12L;

    @Mock
    private TableReference tableReference;

    @Mock
    private RangeRequest rangeRequest;

    @Mock
    private List<byte[]> rows;

    @Mock
    private Map<Cell, Long> timestampByCellMap;

    @Mock
    private KeyValueService delegate;

    private TrackingKeyValueService trackingKvs;

    @Before
    public void setUp() {
        trackingKvs = new TrackingKeyValueServiceImpl(delegate);
    }

    @Test
    public void getAsyncDelegates() {
        when(delegate.getAsync(tableReference, timestampByCellMap))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of()));

        trackingKvs.getAsync(tableReference, timestampByCellMap);

        verify(delegate).getAsync(tableReference, timestampByCellMap);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsDelegates() {
        ColumnSelection columnSelection = mock(ColumnSelection.class);
        when(delegate.getRows(tableReference, rows, columnSelection, TIMESTAMP)).thenReturn(ImmutableMap.of());

        trackingKvs.getRows(tableReference, rows, columnSelection, TIMESTAMP);

        verify(delegate).getRows(tableReference, rows, columnSelection, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsBatchColumnRangeDelegates() {
        BatchColumnRangeSelection batchColumnRangeSelection = mock(BatchColumnRangeSelection.class);
        when(delegate.getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP))
                .thenReturn(ImmutableMap.of());

        trackingKvs.getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP);

        verify(delegate).getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getDelegates() {
        when(delegate.get(tableReference, timestampByCellMap)).thenReturn(ImmutableMap.of());

        trackingKvs.get(tableReference, timestampByCellMap);

        verify(delegate).get(tableReference, timestampByCellMap);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsColumnRangeDelegates() {
        int cellBatchHint = 14;
        ColumnRangeSelection columnRangeSelection = mock(ColumnRangeSelection.class);
        when(delegate.getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP))
                .thenReturn(new LocalRowColumnRangeIterator(
                        ImmutableList.<Entry<Cell, Value>>of().iterator()));

        trackingKvs.getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP);

        verify(delegate).getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getLatestTimestampsDelegates() {
        when(delegate.getLatestTimestamps(tableReference, timestampByCellMap)).thenReturn(ImmutableMap.of());

        trackingKvs.getLatestTimestamps(tableReference, timestampByCellMap);

        verify(delegate).getLatestTimestamps(tableReference, timestampByCellMap);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("MustBeClosedChecker")
    public void getRangeDelegates() {
        when(delegate.getRange(tableReference, rangeRequest, TIMESTAMP))
                .thenReturn(ClosableIterators.emptyImmutableClosableIterator());

        trackingKvs.getRange(tableReference, rangeRequest, TIMESTAMP);

        verify(delegate).getRange(tableReference, rangeRequest, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("MustBeClosedChecker")
    public void getRangeOfTimestampsDelegates() {
        when(delegate.getRangeOfTimestamps(tableReference, rangeRequest, TIMESTAMP))
                .thenReturn(ClosableIterators.emptyImmutableClosableIterator());

        trackingKvs.getRangeOfTimestamps(tableReference, rangeRequest, TIMESTAMP);

        verify(delegate).getRangeOfTimestamps(tableReference, rangeRequest, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("MustBeClosedChecker")
    public void getCandidateCellsForSweepingTest() {
        CandidateCellForSweepingRequest candidateCellForSweepingRequest = mock(CandidateCellForSweepingRequest.class);
        when(delegate.getCandidateCellsForSweeping(tableReference, candidateCellForSweepingRequest))
                .thenReturn(ClosableIterators.emptyImmutableClosableIterator());

        trackingKvs.getCandidateCellsForSweeping(tableReference, candidateCellForSweepingRequest);

        verify(delegate).getCandidateCellsForSweeping(tableReference, candidateCellForSweepingRequest);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getFirstBatchForRangesDelegates() {
        Iterable<RangeRequest> rangeRequests = mock(Iterable.class);
        when(delegate.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP))
                .thenReturn(ImmutableMap.of());

        trackingKvs.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP);

        verify(delegate).getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getAllTableNamesDelegates() {
        when(delegate.getAllTableNames()).thenReturn(ImmutableSet.of());

        trackingKvs.getAllTableNames();

        verify(delegate).getAllTableNames();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMetadataForTableDelegates() {
        when(delegate.getMetadataForTable(tableReference)).thenReturn(PtBytes.EMPTY_BYTE_ARRAY);

        trackingKvs.getMetadataForTable(tableReference);

        verify(delegate).getMetadataForTable(tableReference);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMetadataForTablesDelegates() {
        when(delegate.getMetadataForTables()).thenReturn(ImmutableMap.of());

        trackingKvs.getMetadataForTables();

        verify(delegate).getMetadataForTables();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getAllTimestampsDelegates() {
        Set<Cell> cells = mock(Set.class);
        when(delegate.getAllTimestamps(tableReference, cells, TIMESTAMP)).thenReturn(ImmutableSetMultimap.of());

        trackingKvs.getAllTimestamps(tableReference, cells, TIMESTAMP);

        verify(delegate).getAllTimestamps(tableReference, cells, TIMESTAMP);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getRowsKeyInRangeDelegates() {
        int maxResults = 19;
        byte[] startRow = new byte[10];
        byte[] endRow = new byte[20];
        when(delegate.getRowKeysInRange(tableReference, startRow, endRow, maxResults))
                .thenReturn(ImmutableList.of());

        trackingKvs.getRowKeysInRange(tableReference, startRow, endRow, maxResults);

        verify(delegate).getRowKeysInRange(tableReference, startRow, endRow, maxResults);
        verifyNoMoreInteractions(delegate);
    }
}
