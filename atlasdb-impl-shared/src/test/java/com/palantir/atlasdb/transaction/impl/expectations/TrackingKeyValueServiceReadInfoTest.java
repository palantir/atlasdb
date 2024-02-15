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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueService;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * size must be divisible by 12 due to
 * {@link TrackingKeyValueServiceTestUtils#createByteArrayByTableReferenceMapWithSize}.
 * Refer to {@link TrackingKeyValueServiceTestUtils} javadoc.
 * Also, the range of permitted sizes is constrained as some objects have size bounds.
 * Note that the range of possible sizes will be larger if we use different sizes for different tests.
 */
@ExtendWith(MockitoExtension.class)
public final class TrackingKeyValueServiceReadInfoTest {
    private static final String PARAMETERIZED_TEST_NAME = "size = {0}";

    public static List<Integer> sizes() {
        return List.of(12 * 129, 12 * 365, 12 * 411);
    }

    private static final long TIMESTAMP = 14L;

    @Mock
    private List<byte[]> rows;

    @Mock
    private RangeRequest rangeRequest;

    @Mock
    private TransactionKeyValueService kvs;

    private TrackingKeyValueService trackingKvs;
    private ImmutableMap<Cell, Value> valueByCellMapOfSize;

    @BeforeEach
    public void beforeEach() {
        this.trackingKvs = new TrackingKeyValueServiceImpl(kvs);
    }

    @Mock
    private TableReference tableReference;

    @Mock
    private Map<Cell, Long> timestampByCellMap;

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetAsyncCallAndFutureConsumption(int size) {
        setup(size);
        when(kvs.getAsync(tableReference, timestampByCellMap))
                .thenReturn(Futures.immediateFuture(valueByCellMapOfSize));

        trackingKvs.getAsync(tableReference, timestampByCellMap);

        validateReadInfoForReadForTable("getAsync", size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void getAsyncTracksNothingBeforeDelegateResultCompletionAndTracksReadsAfterCompletion(int size) {
        setup(size);
        SettableFuture<Map<Cell, Value>> delegateFuture = SettableFuture.create();
        when(kvs.getAsync(tableReference, timestampByCellMap)).thenReturn(delegateFuture);
        trackingKvs.getAsync(tableReference, timestampByCellMap);

        // tracking nothing
        assertThat(trackingKvs.getOverallReadInfo())
                .isEqualTo(ImmutableTransactionReadInfo.builder()
                        .kvsCalls(0)
                        .bytesRead(0)
                        .build());
        assertThat(trackingKvs.getReadInfoByTable()).isEmpty();

        // completes the future
        delegateFuture.set(valueByCellMapOfSize);

        validateReadInfoForReadForTable("getAsync", size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void throwingGetAsyncResultTracksNothing(int size) {
        setup(size);
        when(kvs.getAsync(tableReference, timestampByCellMap))
                .thenReturn(Futures.immediateFailedFuture(new RuntimeException()));

        // ignore exception
        assertThatThrownBy(
                () -> trackingKvs.getAsync(tableReference, timestampByCellMap).get());

        assertThat(trackingKvs.getOverallReadInfo())
                .isEqualTo(ImmutableTransactionReadInfo.builder()
                        .kvsCalls(0)
                        .bytesRead(0)
                        .build());
        assertThat(trackingKvs.getReadInfoByTable()).isEmpty();
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetRowsCall(int size) {
        setup(size);
        ColumnSelection columnSelection = mock(ColumnSelection.class);
        when(kvs.getRows(tableReference, rows, columnSelection, TIMESTAMP)).thenReturn(valueByCellMapOfSize);

        trackingKvs.getRows(tableReference, rows, columnSelection, TIMESTAMP);

        validateReadInfoForReadForTable("getRows", size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetRowsBatchColumnRangeCallAndIteratorsConsumption(int size) {
        setup(size);
        BatchColumnRangeSelection batchColumnRangeSelection = mock(BatchColumnRangeSelection.class);
        when(kvs.getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP))
                .thenReturn(TrackingKeyValueServiceTestUtils.createRowColumnRangeIteratorByByteArrayMutableMapWithSize(
                        size));

        trackingKvs
                .getRowsColumnRange(tableReference, rows, batchColumnRangeSelection, TIMESTAMP)
                .values()
                .forEach(iterator -> iterator.forEachRemaining(_unused -> {}));

        validateReadInfoForLazyRead(size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetRowsColumnRangeCallAndIteratorConsumption(int size) {
        setup(size);
        int cellBatchHint = 17;
        ColumnRangeSelection columnRangeSelection = mock(ColumnRangeSelection.class);
        when(kvs.getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP))
                .thenReturn(new LocalRowColumnRangeIterator(
                        valueByCellMapOfSize.entrySet().iterator()));

        trackingKvs
                .getRowsColumnRange(tableReference, rows, columnRangeSelection, cellBatchHint, TIMESTAMP)
                .forEachRemaining(_unused -> {});

        validateReadInfoForLazyRead(size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetCall(int size) {
        setup(size);
        when(kvs.get(tableReference, timestampByCellMap)).thenReturn(valueByCellMapOfSize);

        trackingKvs.get(tableReference, timestampByCellMap);

        validateReadInfoForReadForTable("get", size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetLatestTimestampsCall(int size) {
        setup(size);
        Map<Cell, Long> timestampByCellMapOfSize = TrackingKeyValueServiceTestUtils.createLongByCellMapWithSize(size);
        when(kvs.getLatestTimestamps(tableReference, timestampByCellMap)).thenReturn(timestampByCellMapOfSize);

        trackingKvs.getLatestTimestamps(tableReference, timestampByCellMap);

        validateReadInfoForReadForTable("getLatestTimestamps", size);
    }

    @SuppressWarnings("MustBeClosedChecker")
    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetRangeCallAndIteratorConsumption(int size) {
        setup(size);
        List<RowResult<Value>> backingValueRowResultListOfSize =
                TrackingKeyValueServiceTestUtils.createValueRowResultListWithSize(size);

        when(kvs.getRange(tableReference, rangeRequest, TIMESTAMP))
                .thenReturn(ClosableIterators.wrapWithEmptyClose(backingValueRowResultListOfSize.iterator()));

        trackingKvs.getRange(tableReference, rangeRequest, TIMESTAMP).forEachRemaining(_unused -> {});

        validateReadInfoForLazyRead(size);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("sizes")
    public void readInfoIsCorrectAfterGetFirstBatchForRangesCall(int size) {
        setup(size);
        Iterable<RangeRequest> rangeRequests = mock(Iterable.class);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRangeRequestMapOfSize =
                TrackingKeyValueServiceTestUtils.createPageByRangeRequestMapWithSize(size);

        when(kvs.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP))
                .thenReturn(pageByRangeRequestMapOfSize);

        trackingKvs.getFirstBatchForRanges(tableReference, rangeRequests, TIMESTAMP);

        validateReadInfoForReadForTable("getFirstBatchForRanges", size);
    }

    private void setup(int size) {
        valueByCellMapOfSize = TrackingKeyValueServiceTestUtils.createValueByCellMapWithSize(size);
    }

    private void validateReadInfoForReadForTable(String methodName, int size) {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of(methodName, size))
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).containsOnlyKeys(ImmutableList.of(tableReference));
    }

    private void validateReadInfoForLazyRead(int size) {
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(size)
                .kvsCalls(1)
                .build();
        assertThat(trackingKvs.getOverallReadInfo()).isEqualTo(readInfo);
        assertThat(trackingKvs.getReadInfoByTable()).containsOnlyKeys(ImmutableList.of(tableReference));
    }
}
