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
package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ImmutableGetRangesQuery;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.IterableView;
import com.palantir.common.collect.MapEntries;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.util.Pair;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Assume;
import org.junit.Test;

@SuppressWarnings({"checkstyle:all","DefaultCharset"}) // TODO(someonebored): clean this horrible test class up!
public abstract class AbstractTransactionTest extends TransactionTestSetup {
    private static final TransactionConfig TRANSACTION_CONFIG = ImmutableTransactionConfig.builder().build();
    private static final BatchColumnRangeSelection ALL_COLUMNS = BatchColumnRangeSelection.create(
            PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 3);

    public AbstractTransactionTest(KvsManager kvsManager, TransactionManagerManager tmManager) {
        super(kvsManager, tmManager);
    }

    protected boolean supportsReverse() {
        return true;
    }

    // Duplicates of TransactionTestConstants since this is currently (incorrectly) in main
    // rather than test. Can use the former once we resolve the dependency issues.
    public static final int GET_RANGES_THREAD_POOL_SIZE = 16;
    public static final int DEFAULT_GET_RANGES_CONCURRENCY = 4;

    protected static final ExecutorService GET_RANGES_EXECUTOR =
            Executors.newFixedThreadPool(GET_RANGES_THREAD_POOL_SIZE);

    protected Transaction startTransaction() {
        long startTimestamp = timestampService.getFreshTimestamp();
        return new SnapshotTransaction(metricsManager,
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                NoOpLockWatchManager.INSTANCE,
                transactionService,
                NoOpCleaner.INSTANCE,
                () -> startTimestamp,
                ConflictDetectionManagers.create(keyValueService),
                SweepStrategyManagers.createDefault(keyValueService),
                startTimestamp,
                Optional.empty(),
                PreCommitConditions.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false,
                timestampCache,
                GET_RANGES_EXECUTOR,
                DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService(),
                true,
                () -> TRANSACTION_CONFIG,
                ConflictTracer.NO_OP);
    }

    @Test
    public void testMultipleBigValues() {
        testBigValue(0);
        testBigValue(1);
    }

    private void testBigValue(int i) {
        byte[] bytes = new byte[64 * 1024];
        new Random().nextBytes(bytes);
        String encodeHexString = BaseEncoding.base16().lowerCase().encode(bytes);
        putDirect("row" + i, "col" + i, encodeHexString, 0);
        Pair<String, Long> pair = getDirect("row" + i, "col" + i, 1);
        assertEquals(0L, (long) pair.getRhSide());
        assertEquals(encodeHexString, pair.getLhSide());
    }

    @Test
    public void testSpecialValues() {
        String eight = "00000000";
        String sixteen = eight + eight;
        putDirect("row1", "col1", eight, 0);
        putDirect("row2", "col1", sixteen, 0);
        Pair<String, Long> direct1 = getDirect("row1", "col1", 1);
        assertEquals(eight, direct1.lhSide);
        Pair<String, Long> direct2 = getDirect("row2", "col1", 1);
        assertEquals(sixteen, direct2.lhSide);
    }

    @Test
    public void testKeyValueRows() {
        putDirect("row1", "col1", "v1", 0);
        Pair<String, Long> pair = getDirect("row1", "col1", 1);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());

        putDirect("row1", "col1", "v2", 2);
        pair = getDirect("row1", "col1", 2);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());

        pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("v2", pair.getLhSide());
    }

    // we want PK violations on the Transaction table
    @Test
    public void testPrimaryKeyViolation() {
        Cell cell = Cell.create("r1".getBytes(), TransactionConstants.COMMIT_TS_COLUMN);
        keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE,
            ImmutableMap.of(cell, "v1".getBytes()));

        assertThatThrownBy(() ->
                keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE,
                        ImmutableMap.of(cell, "v2".getBytes())))
                .isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void testEmptyValue() {
        putDirect("row1", "col1", "v1", 0);
        Pair<String, Long> pair = getDirect("row1", "col1", 1);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());

        putDirect("row1", "col1", "", 2);
        pair = getDirect("row1", "col1", 2);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());

        pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("", pair.getLhSide());
    }

    @Test
    public void testKeyValueRange() {
        putDirect("row1", "col1", "v1", 0);
        putDirect("row1", "col2", "v2", 2);
        putDirect("row1", "col4", "v5", 3);
        putDirect("row1a", "col4", "v5", 100);
        putDirect("row2", "col2", "v3", 1);
        putDirect("row2", "col4", "v4", 6);

        ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().build(), 1));
        assertEquals(1, list.size());
        RowResult<Value> row = list.iterator().next();
        assertEquals(1, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().build(), 2));
        assertEquals(2, list.size());
        row = list.iterator().next();
        assertEquals(1, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().build(), 3));
        assertEquals(2, list.size());
        row = list.iterator().next();
        assertEquals(2, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().endRowExclusive(PtBytes.toBytes("row2")).build(), 3));
        assertEquals(1, list.size());
        row = list.iterator().next();
        assertEquals(2, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().startRowInclusive(PtBytes.toBytes("row1a")).build(), 3));
        assertEquals(1, list.size());
        row = list.iterator().next();
        assertEquals(1, row.getColumns().size());
    }

    @Test
    public void testKeyValueEmptyRange() {
        putDirect("row1", "col1", "v1", 0);

        byte[] rowBytes = PtBytes.toBytes("row1");
        ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().startRowInclusive(rowBytes).endRowExclusive(rowBytes).build(), 1));
        assertTrue(list.isEmpty());
    }

    @Test
    public void testRowsColumnRange_allResultsPostFiltered() {
        putDirect("row1", "col1", "v1", 5);
        putDirect("row1", "col2", "v2", 5);

        byte[] rowBytes = PtBytes.toBytes("row1");
        RowColumnRangeIterator iterator = keyValueService.getRowsColumnRange(
                TEST_TABLE,
                ImmutableList.of(rowBytes),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                1);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testRowsColumnRange_postFilteredFirstPage() {
        putDirect("row1", "col1", "v1", 5);
        putDirect("row1", "col1", "v1a", 6);
        putDirect("row1", "col2", "v2", 5);
        putDirect("row1", "col3", "v3", 0);

        byte[] rowBytes = PtBytes.toBytes("row1");
        RowColumnRangeIterator iterator = keyValueService.getRowsColumnRange(
                TEST_TABLE,
                ImmutableList.of(rowBytes),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                1);
        assertThat(PtBytes.toBytes("v3")).isEqualTo(iterator.next().getValue().getContents());
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testRowsColumnRange_forwardProgressAfterValidResult() {
        putDirect("row1", "col1", "v1a", 1);
        putDirect("row1", "col1", "v1b", 2);
        putDirect("row1", "col1", "v1c", 3);
        putDirect("row1", "col1", "v1d", 4);
        putDirect("row1", "col1", "v1e", 5);
        putDirect("row1", "col2", "v2", 5);

        byte[] rowBytes = PtBytes.toBytes("row1");
        RowColumnRangeIterator iterator = keyValueService.getRowsColumnRange(
                TEST_TABLE,
                ImmutableList.of(rowBytes),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                6);
        assertThat(PtBytes.toBytes("v1e")).isEqualTo(iterator.next().getValue().getContents());
        assertThat(PtBytes.toBytes("v2")).isEqualTo(iterator.next().getValue().getContents());
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testRowsColumnRange_abortsCorrectlyHalfway() {
        putDirect("row1", "col1", "v1a", 3);
        putDirect("row1", "col1", "v1b", 4);
        putDirect("row1", "col1", "v1c", 5);
        putDirect("row1", "col1", "v1d", 7);
        putDirect("row1", "col1", "v1e", 8);
        putDirect("row1", "col2", "v2", 5);

        byte[] rowBytes = PtBytes.toBytes("row1");
        RowColumnRangeIterator iterator = keyValueService.getRowsColumnRange(
                TEST_TABLE,
                ImmutableList.of(rowBytes),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                6);
        assertThat(PtBytes.toBytes("v1c")).isEqualTo(iterator.next().getValue().getContents());
        assertThat(PtBytes.toBytes("v2")).isEqualTo(iterator.next().getValue().getContents());
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testKeyValueRangeColumnSelection() {
        putDirect("row1", "col1", "v1", 0);
        putDirect("row1", "col2", "v2", 2);
        putDirect("row1", "col4", "v5", 3);
        putDirect("row1a", "col4", "v5", 100);
        putDirect("row2", "col2", "v3", 1);
        putDirect("row2", "col4", "v4", 6);

        List<byte[]> selectedColumns = ImmutableList.of(PtBytes.toBytes("col2"));
        RangeRequest simpleRange = RangeRequest.builder().retainColumns(ColumnSelection.create(selectedColumns)).build();
        ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, simpleRange, 1));
        assertEquals(0, list.size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, simpleRange, 2));
        assertEquals(1, list.size());
        RowResult<Value> row = list.iterator().next();
        assertEquals(1, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, simpleRange, 3));
        assertEquals(2, list.size());
        row = list.iterator().next();
        assertEquals(1, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, simpleRange.getBuilder().endRowExclusive(PtBytes.toBytes("row2")).build(), 3));
        assertEquals(1, list.size());
        row = list.iterator().next();
        assertEquals(1, row.getColumns().size());

        list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, simpleRange.getBuilder().startRowInclusive(PtBytes.toBytes("row1a")).build(), 3));
        assertEquals(1, list.size());
        row = list.iterator().next();
        assertEquals(1, row.getColumns().size());
    }

    @Test
    public void testKeyValueRangeWithDeletes() {
        putDirect("row1", "col1", "", 0);

        ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(keyValueService.getRange(TEST_TABLE, RangeRequest.builder().build(), 1));
        assertEquals(1, list.size());
        RowResult<Value> row = list.iterator().next();
        assertEquals(1, row.getColumns().size());
    }

    @Test
    public void testKeyValueRanges() {
        putDirect("row1", "col1", "", 0);
        putDirect("row2", "col1", "", 0);
        putDirect("row2", "col2", "", 0);

        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, ImmutableList.of(RangeRequest.builder().build(), RangeRequest.builder().build()), 1);
        assertTrue(ranges.size() >= 1);
    }

    @Test
    public void testKeyValueRanges2() {
        putDirect("row1", "col1", "", 0);
        putDirect("row2", "col1", "", 0);
        putDirect("row2", "col2", "", 0);

        final RangeRequest allRange = RangeRequest.builder().build();
        final RangeRequest oneRange = RangeRequest.builder().startRowInclusive("row2".getBytes()).build();
        final RangeRequest allRangeBatch = RangeRequest.builder().batchHint(3).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, ImmutableList.of(allRange, oneRange, allRangeBatch), 1);
        assertTrue(ranges.get(allRange).getResults().size()>=1);
        assertEquals(2, ranges.get(allRangeBatch).getResults().size());
        assertFalse(ranges.get(allRangeBatch).moreResultsAvailable());
        assertEquals(1, ranges.get(oneRange).getResults().size());
    }

    @Test
    public void testKeyValueRangesMany2() {
        putDirect("row1", "col1", "", 0);
        putDirect("row2", "col1", "", 0);
        putDirect("row2", "col2", "", 0);

        RangeRequest allRange = RangeRequest.builder().batchHint(3).build();
        for (int i = 0 ; i < 1000 ; i++) {
            ClosableIterator<RowResult<Value>> range = keyValueService.getRange(TEST_TABLE, allRange, 1);
            ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(range);
            assertEquals(2, list.size());
        }
    }

    @Test
    public void testKeyValueRangesMany3() {
        putDirect("row1", "col1", "", 0);
        putDirect("row2", "col1", "", 0);
        putDirect("row2", "col2", "", 0);

        RangeRequest allRange = RangeRequest.builder().prefixRange("row1".getBytes()).batchHint(3).build();
        for (int i = 0 ; i < 1000 ; i++) {
            ClosableIterator<RowResult<Value>> range = keyValueService.getRange(TEST_TABLE, allRange, 1);
            ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(range);
            assertEquals(1, list.size());
        }
    }

    @Test
    public void testKeyValueRangeReverse() {
        if (!supportsReverse()) {
            return;
        }
        putDirect("row1", "col1", "", 0);
        putDirect("row2", "col1", "", 0);
        putDirect("row2", "col2", "", 0);

        RangeRequest allRange = RangeRequest.reverseBuilder().batchHint(3).build();
        ClosableIterator<RowResult<Value>> range = keyValueService.getRange(TEST_TABLE, allRange, 1);
        ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(range);
        assertEquals(2, list.size());
        assertEquals("row2", PtBytes.toString(list.iterator().next().getRowName()));
    }

    @Test
    public void testRangePagingBatches() {
        int totalPuts = 101;
        for (int i = 0 ; i < totalPuts ; i++) {
            putDirect("row"+i, "col1", "v1", 0);
        }

        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(RangeRequest.builder().batchHint(1000).build()), 100), 1);
        assertEquals(1, ranges.keySet().size());
        assertEquals(totalPuts, ranges.values().iterator().next().getResults().size());
    }

    @Test
    public void testRangePagingBatchesReverse() {
        if (!supportsReverse()) {
            return;
        }
        int totalPuts = 101;
        for (int i = 0 ; i < totalPuts ; i++) {
            putDirect("row"+i, "col1", "v1", 0);
        }

        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(RangeRequest.reverseBuilder().batchHint(1000).build()), 100), 1);
        assertEquals(1, ranges.keySet().size());
        assertEquals(totalPuts, ranges.values().iterator().next().getResults().size());
    }

    @Test
    public void testRangePagingBatchSizeOne() {
        int totalPuts = 100;
        for (int i = 0 ; i < totalPuts ; i++) {
            putDirect("row"+i, "col1", "v1", 0);
        }

        RangeRequest rangeRequest = RangeRequest.builder().batchHint(1).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(rangeRequest), 100), 1);
        assertEquals(1, ranges.keySet().size());
        assertEquals(1, ranges.values().iterator().next().getResults().size());
        assertEquals("row0", PtBytes.toString(ranges.values().iterator().next().getResults().iterator().next().getRowName()));
    }

    @Test
    public void testRangePagingBatchSizeOneReverse() {
        if (!supportsReverse()) {
            return;
        }
        int totalPuts = 100;
        for (int i = 0 ; i < totalPuts ; i++) {
            putDirect("row"+i, "col1", "v1", 0);
        }

        RangeRequest rangeRequest = RangeRequest.reverseBuilder().batchHint(1).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(rangeRequest), 100), 1);
        assertEquals(1, ranges.keySet().size());
        assertEquals(1, ranges.values().iterator().next().getResults().size());
        assertEquals("row99", PtBytes.toString(ranges.values().iterator().next().getResults().iterator().next().getRowName()));
    }

    @Test
    public void testRangePageBatchSizeOne() {
        RangeRequest rangeRequest = RangeRequest.builder().batchHint(1).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Collections.singleton(rangeRequest), 1);
        assertEquals(1, ranges.keySet().size());
        assertEquals(0, ranges.values().iterator().next().getResults().size());
        assertEquals(false, ranges.values().iterator().next().moreResultsAvailable());
    }

    @Test
    public void testRangeAfterTimestamp() {
        putDirect("row1", "col2", "", 5);
        putDirect("row2", "col2", "", 0);
        RangeRequest rangeRequest = RangeRequest.builder().batchHint(1).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Collections.singleton(rangeRequest), 1);
        assertEquals(1, ranges.keySet().size());
        TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page = ranges.values().iterator().next();
        assertTrue(!page.getResults().isEmpty() || page.moreResultsAvailable());
    }

    @Test
    public void testPostfilter() {
        long ts;
        do {
            ts = new Random().nextLong();
        } while (ts <= 1000);
        String rowName = "row1";
        String columnName = "col2";
        String value = "asdf";
        putDirect(rowName, columnName, value, ts);
        putDirect(rowName, columnName, value, ts-1);
        putDirect(rowName, columnName, value, ts-1000);

        Pair<String, Long> get = getDirect(rowName, columnName, ts + 1);
        assertEquals(Pair.create(value, ts), get);

        get = getDirect(rowName, columnName, ts);
        assertEquals(Pair.create(value, ts-1), get);

        get = getDirect(rowName, columnName, ts-1);
        assertEquals(Pair.create(value, ts-1000), get);

        get = getDirect(rowName, columnName, ts-999);
        assertEquals(Pair.create(value, ts-1000), get);

        get = getDirect(rowName, columnName, ts-1000);
        assertNull(get);
    }

    @Test
    public void testRangeAfterTimestamp2() {
        putDirect("row1", "col2", "", 5);
        putDirect("row2", "col2", "", 0);
        putDirect("row3", "col2", "", 0);
        RangeRequest rangeRequest = RangeRequest.builder().batchHint(1).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Collections.singleton(rangeRequest), 1);
        assertEquals(1, ranges.keySet().size());
        TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page = ranges.values().iterator().next();
        assertTrue(page.moreResultsAvailable());
    }

    @Test
    public void testRangeAfterTimestampReverse() {
        if (!supportsReverse()) {
            return;
        }
        putDirect("row1", "col2", "", 0);
        putDirect("row2", "col2", "", 0);
        putDirect("row3", "col2", "", 5);
        RangeRequest rangeRequest = RangeRequest.reverseBuilder().batchHint(1).build();
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ranges = keyValueService.getFirstBatchForRanges(TEST_TABLE, Collections.singleton(rangeRequest), 1);
        assertEquals(1, ranges.keySet().size());
        TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page = ranges.values().iterator().next();
        assertTrue(page.moreResultsAvailable());
    }

    @Test
    public void testRangeBatchSizeOne() {
        RangeRequest range = RangeRequest.builder().batchHint(1).build();
        ClosableIterator<RowResult<Value>> ranges = keyValueService.getRange(TEST_TABLE, range, 1);
        assertEquals(false, ranges.hasNext());
    }

    @Test
    public void testRangesTransaction() {
        Transaction t = startTransaction();
        put(t, "row1", "col1", "v1");
        t.commit();

        RangeRequest allRange = RangeRequest.builder().batchHint(3).build();
        t = startTransaction();

        verifyAllGetRangesImplsRangeSizes(t, allRange, 1);
    }

    @Test
    public void testRangesTransactionColumnSelection() {
        Transaction t = startTransaction();
        put(t, "row1", "col1", "v1");
        t.commit();

        RangeRequest range1 = RangeRequest.builder().batchHint(3).build();
        RangeRequest range2 = range1.getBuilder().retainColumns(ColumnSelection.create(ImmutableSet.of(PtBytes.toBytes("col1")))).build();
        t = startTransaction();
        Iterable<BatchingVisitable<RowResult<byte[]>>> ranges = t.getRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(range1, range2), 1000));
        for (BatchingVisitable<RowResult<byte[]>> batchingVisitable : ranges) {
            final List<RowResult<byte[]>> list = BatchingVisitables.copyToList(batchingVisitable);
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).getColumns().size());
        }
        RangeRequest range3 = range1.getBuilder().retainColumns(ColumnSelection.create(ImmutableSet.of(PtBytes.toBytes("col2")))).build();
        verifyAllGetRangesImplsRangeSizes(t, range3, 0);
    }

    @Test
    public void testRangePaging() {
        int totalPuts = 101;
        for (int i = 0 ; i < totalPuts ; i++) {
            putDirect("row"+i, "col1", "v1", 0);
        }

        ClosableIterator<RowResult<Value>> range = keyValueService.getRange(TEST_TABLE, RangeRequest.builder().batchHint(1000).build(), 1);
        try {
            int reads = Iterators.size(range);
            assertEquals(totalPuts, reads);
        } finally {
            range.close();
        }
    }

    @Test
    public void testEmptyColumnRangePagingTransaction() {
        byte[] row = PtBytes.toBytes("row1");
        Transaction t = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> columnRangeIterator =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        List<Map.Entry<Cell, byte[]>> expected = ImmutableList.of();
        verifyMatchingResult(expected, row, columnRange);
        verifyMatchingResultForIterator(expected, row, columnRangeIterator);

        put(t, "row1", "col1", "v1");
        t.commit();

        t = startTransaction();
        delete(t, "row1", "col1");
        columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        columnRangeIterator =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        verifyMatchingResult(expected, row, columnRange);
        verifyMatchingResultForIterator(expected, row, columnRangeIterator);
        t.commit();

        t = startTransaction();
        columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        columnRangeIterator =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        verifyMatchingResult(expected, row, columnRange);
        verifyMatchingResultForIterator(expected, row, columnRangeIterator);
    }

    @Test
    public void testColumnRangePagingTransaction_batchingVisitable() {
        Transaction t = startTransaction();
        int totalPuts = 101;
        byte[] row = PtBytes.toBytes("row1");
        // Record expected results using byte ordering
        ImmutableSortedMap.Builder<Cell, byte[]> writes = ImmutableSortedMap
                .orderedBy(Ordering.from(UnsignedBytes.lexicographicalComparator()).onResultOf(key -> key.getColumnName()));
        for (int i = 0 ; i < totalPuts ; i++) {
            put(t, "row1", "col" + i, "v" + i);
            writes.put(Cell.create(row, PtBytes.toBytes("col" + i)), PtBytes.toBytes("v" + i));
        }
        t.commit();

        t = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        List<Map.Entry<Cell, byte[]>> expected = ImmutableList.copyOf(writes.build().entrySet());
        verifyMatchingResult(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.EMPTY_BYTE_ARRAY, 1));
        verifyMatchingResult(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.EMPTY_BYTE_ARRAY, 101));
        verifyMatchingResult(expected, PtBytes.toBytes("row1"), columnRange);

        columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                        RangeRequests.nextLexicographicName(expected.get(expected.size() - 1).getKey().getColumnName()), 1));
        verifyMatchingResult(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                        expected.get(expected.size() - 1).getKey().getColumnName(), 1));
        verifyMatchingResult(ImmutableList.copyOf(Iterables.limit(expected, 100)), row, columnRange);
    }

    @Test
    public void testColumnRangePagingTransaction_iterator() {
        Transaction t = startTransaction();
        int totalPuts = 101;
        byte[] row = PtBytes.toBytes("row1");
        // Record expected results using byte ordering
        ImmutableSortedMap.Builder<Cell, byte[]> writes = ImmutableSortedMap
                .orderedBy(Ordering.from(UnsignedBytes.lexicographicalComparator()).onResultOf(key -> key.getColumnName()));
        for (int i = 0 ; i < totalPuts ; i++) {
            put(t, "row1", "col" + i, "v" + i);
            writes.put(Cell.create(row, PtBytes.toBytes("col" + i)), PtBytes.toBytes("v" + i));
        }
        t.commit();

        t = startTransaction();
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> columnRange =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        List<Map.Entry<Cell, byte[]>> expected = ImmutableList.copyOf(writes.build().entrySet());
        verifyMatchingResultForIterator(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.EMPTY_BYTE_ARRAY, 1));
        verifyMatchingResultForIterator(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.EMPTY_BYTE_ARRAY, 101));
        verifyMatchingResultForIterator(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                        RangeRequests.nextLexicographicName(expected.get(expected.size() - 1).getKey().getColumnName()), 1));
        verifyMatchingResultForIterator(expected, row, columnRange);

        columnRange =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                        expected.get(expected.size() - 1).getKey().getColumnName(), 1));
        verifyMatchingResultForIterator(ImmutableList.copyOf(Iterables.limit(expected, 100)), row, columnRange);
    }

    protected void verifyMatchingResult(List<Map.Entry<Cell, byte[]>> expected, byte[] row, Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange) {
        assertEquals(1, columnRange.size());
        assertArrayEquals(row, Iterables.getOnlyElement(columnRange.keySet()));
        BatchingVisitable<Map.Entry<Cell, byte[]>> batchingVisitable = Iterables.getOnlyElement(columnRange.values());
        List<Map.Entry<Cell, byte[]>> results = BatchingVisitables.copyToList(batchingVisitable);
        assertEquals(expected.size(), results.size());
        for (int i = 0 ; i < expected.size() ; i++) {
            assertEquals(expected.get(i).getKey(), results.get(i).getKey());
            assertArrayEquals(expected.get(i).getValue(), results.get(i).getValue());
        }
    }

    protected void verifyMatchingResultForIterator(List<Map.Entry<Cell, byte[]>> expected, byte[] row, Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> columnRange) {
        assertEquals(1, columnRange.size());
        assertArrayEquals(row, Iterables.getOnlyElement(columnRange.keySet()));
        Iterator<Map.Entry<Cell, byte[]>> iterator = Iterables.getOnlyElement(columnRange.values());
        List<Map.Entry<Cell, byte[]>> results = Lists.newArrayList(iterator);
        assertEquals(expected.size(), results.size());
        for (int i = 0 ; i < expected.size() ; i++) {
            assertEquals(expected.get(i).getKey(), results.get(i).getKey());
            assertArrayEquals(expected.get(i).getValue(), results.get(i).getValue());
        }
    }

    @Test
    public void testReadMyWritesColumnRangePagingTransaction() {
        Transaction t = startTransaction();
        int totalPuts = 101;
        byte[] row = PtBytes.toBytes("row1");
        // Record expected results using byte ordering
        ImmutableSortedMap.Builder<Cell, byte[]> writes = ImmutableSortedMap
                .orderedBy(Ordering.from(UnsignedBytes.lexicographicalComparator()).onResultOf(key -> key.getColumnName()));
        for (int i = 0 ; i < totalPuts ; i++) {
            put(t, "row1", "col" + i, "v" + i);
            if (i % 2 == 0) {
                writes.put(Cell.create(row, PtBytes.toBytes("col" + i)), PtBytes.toBytes("v" + i));
            }
        }
        t.commit();

        t = startTransaction();

        for (int i = 0 ; i < totalPuts ; i++) {
            if (i % 2 == 1) {
                put(t, "row1", "col" + i, "t_v" + i);
                writes.put(Cell.create(row, PtBytes.toBytes("col" + i)), PtBytes.toBytes("t_v" + i));
            }
        }

        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        List<Map.Entry<Cell, byte[]>> expected = ImmutableList.copyOf(writes.build().entrySet());
        verifyMatchingResult(expected, row, columnRange);

        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> columnRangeIterator =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        verifyMatchingResultForIterator(expected, row, columnRangeIterator);
    }

    @Test
    public void testReadMyDeletesColumnRangePagingTransaction() {
        Transaction t = startTransaction();
        int totalPuts = 101;
        byte[] row = PtBytes.toBytes("row1");
        // Record expected results using byte ordering
        ImmutableSortedMap.Builder<Cell, byte[]> writes = ImmutableSortedMap
                .orderedBy(Ordering.from(UnsignedBytes.lexicographicalComparator()).onResultOf(key -> key.getColumnName()));
        for (int i = 0 ; i < totalPuts ; i++) {
            put(t, "row1", "col" + i, "v" + i);
            if (i % 2 == 0) {
                writes.put(Cell.create(row, PtBytes.toBytes("col" + i)), PtBytes.toBytes("v" + i));
            }
        }
        t.commit();

        t = startTransaction();

        for (int i = 0 ; i < totalPuts ; i++) {
            if (i % 2 == 1) {
                delete(t, "row1", "col" + i);
            }
        }

        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        List<Map.Entry<Cell, byte[]>> expected = ImmutableList.copyOf(writes.build().entrySet());
        verifyMatchingResult(expected, row, columnRange);

        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> columnRangeIterator =
                t.getRowsColumnRangeIterator(TEST_TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        verifyMatchingResultForIterator(expected, row, columnRangeIterator);
    }

    @Test
    public void testKeyValueMultiput() {
        TableReference table = TableReference.createWithEmptyNamespace("table2");
        keyValueService.createTable(table, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Cell k = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));
        String value = "whatever";
        byte[] v = PtBytes.toBytes(value);
        Map<Cell, byte[]> map = ImmutableMap.of(k, v);
        keyValueService.multiPut(ImmutableMap.of(TEST_TABLE, map, table, map), 0);
        assertEquals(value, getDirect("row", "col", 1).lhSide);
        assertEquals(value, getDirect(table, "row", "col", 1).lhSide);
        keyValueService.dropTable(table);
    }

    @Test
    public void testKeyValueDelete() {
        putDirect("row1", "col1", "v1", 0);
        Pair<String, Long> pair = getDirect("row1", "col1", 2);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 0L)));
        pair = getDirect("row1", "col1", 2);
        assertNull(pair);
    }

    @Test
    public void testKeyValueDelete2() {
        putDirect("row1", "col1", "v1", 1);
        putDirect("row1", "col1", "v2", 2);
        Pair<String, Long> pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("v2", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 2L)));
        pair = getDirect("row1", "col1", 3);
        assertEquals(1L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
    }

    @Test
    public void testKeyValueDelete3() {
        putDirect("row1", "col1", "v0", 0);
        putDirect("row1", "col1", "v1", 1);
        Pair<String, Long> pair = getDirect("row1", "col1", 2);
        assertEquals(1L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 1L)));
        pair = getDirect("row1", "col1", 2);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v0", pair.getLhSide());
    }

    @Test
    public void testKeyValueDelete4() {
        putDirect("row1", "col1", "v0", 0);
        putDirect("row1", "col1", "v1", 10);
        Pair<String, Long> pair = getDirect("row1", "col1", 11);
        assertEquals(10L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
        pair = getDirect("row1", "col1", 2);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v0", pair.getLhSide());
    }

    @Test
    public void testKeyValueDelete5() {
        putDirect("row1", "col1", "v0", 0);
        putDirect("row1", "col1", "v1", 1);
        Pair<String, Long> pair = getDirect("row1", "col1", 2);
        assertEquals(1L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 0L)));
        pair = getDirect("row1", "col1", 2);
        assertEquals(1L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
    }

    @Test
    public void testKeyValueDelete6() {
        putDirect("row1", "col1", "v1", 1);
        putDirect("row1", "col1", "v2", 2);
        Pair<String, Long> pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("v2", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 1L)));
        pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("v2", pair.getLhSide());
    }

    @Test
    public void testKeyValueDelete7() {
        putDirect("row1", "col1", "v2", 2);
        Pair<String, Long> pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("v2", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 1L)));
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(createCell("row1", "col1"), 3L)));
        pair = getDirect("row1", "col1", 3);
        assertEquals(2L, (long)pair.getRhSide());
        assertEquals("v2", pair.getLhSide());
    }

    @Test
    public void testKeyValueRetainVersions() {
        putDirect("row1", "col1", "v1", 1);
        putDirect("row1", "col1", "v2", 2);
        putDirect("row1", "col1", "v3", 3);
        putDirect("row1", "col1", "v4", 4);
        putDirect("row1", "col1", "v5", 5);
        Pair<String, Long> pair = getDirect("row1", "col1", 6);
        assertEquals(5L, (long)pair.getRhSide());
        assertEquals("v5", pair.getLhSide());
        pair = getDirect("row1", "col1", 2);
        assertEquals(1L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
    }

    // This test is required to pass if you want your KV store to support hard delete
    @Test
    public void testNegativeTimestamps() {
        Cell k = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("col1"));
        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(k));
        putDirect("row1", "col1", "v3", 3);
        Pair<String, Long> pair = getDirect("row1", "col1", Long.MAX_VALUE);
        assertEquals("v3", pair.getLhSide());
        pair = getDirect("row1", "col1", 0);
        assertEquals("", pair.getLhSide());
        assertEquals(-1L, (long)pair.getRhSide());

        keyValueService.delete(TEST_TABLE, ImmutableMultimap.of(k, 3L));
        pair = getDirect("row1", "col1", Long.MAX_VALUE);
        assertEquals("", pair.getLhSide());
        assertEquals(-1L, (long)pair.getRhSide());
        Multimap<Cell, Long> allTimestamps = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(k), 0);
        assertEquals(1, allTimestamps.size());
        allTimestamps = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(k), Long.MAX_VALUE);
        assertEquals(1, allTimestamps.size());
    }

    @Test
    public void testGetAtDifferentVersions() {
        putDirect("row1", "col1", "v1", 1);
        putDirect("row1", "col1", "v2", 5);
        putDirect("row2", "col1", "v3", 3);
        putDirect("row2", "col1", "v4", 8);
        Cell cell1 = Cell.create("row1".getBytes(), "col1".getBytes());
        Cell cell2 = Cell.create("row2".getBytes(), "col1".getBytes());
        Map<Cell, Value> results = keyValueService.get(TEST_TABLE, ImmutableMap.of(cell1, 5L, cell2, 8L));

        Value v = results.get(cell1);
        assertEquals(1L, v.getTimestamp());
        assertEquals("v1", new String(v.getContents()));
        v = results.get(cell2);
        assertEquals(3L, v.getTimestamp());
        assertEquals("v3", new String(v.getContents()));
    }

    @Test
    public void testReadMyWrites() {
        Transaction t = startTransaction();
        put(t, "row1", "col1", "v1");
        put(t, "row1", "col2", "v2");
        put(t, "row2", "col1", "v3");
        assertEquals("v1", get(t, "row1", "col1"));
        assertEquals("v2", get(t, "row1", "col2"));
        assertEquals("v3", get(t, "row2", "col1"));
        t.commit();

        t = startTransaction();
        assertEquals("v1", get(t, "row1", "col1"));
        assertEquals("v2", get(t, "row1", "col2"));
        assertEquals("v3", get(t, "row2", "col1"));
    }

    @Test
    public void testReadMyWritesRange() {
        Transaction t = startTransaction();
        put(t, "row1", "col1", "v1");
        put(t, "row1", "col2", "v2");
        put(t, "row2", "col1", "v3");
        put(t, "row4", "col1", "v4");
        t.commit();

        t = startTransaction();
        assertEquals("v1", get(t, "row1", "col1"));
        assertEquals("v2", get(t, "row1", "col2"));
        assertEquals("v3", get(t, "row2", "col1"));
        BatchingVisitable<RowResult<byte[]>> visitable = t.getRange(TEST_TABLE, RangeRequest.builder().build());
        put(t, "row0", "col1", "v5");
        put(t, "row1", "col1", "v5");
        put(t, "row1", "col3", "v6");
        put(t, "row3", "col1", "v7");
        delete(t, "row2", "col1");
        put(t, "row2", "col2", "v8");

        final Map<Cell, byte[]> vals = Maps.newHashMap();
        visitable.batchAccept(100, AbortingVisitors.batching((RowVisitor) item -> {
            MapEntries.putAll(vals, item.getCells());
            if (Arrays.equals(item.getRowName(), "row1".getBytes())) {
                assertEquals(3, IterableView.of(item.getCells()).size());
                assertEquals("v5", new String(item.getColumns().get("col1".getBytes())));
            }
            return true;
        }));
        assertTrue(vals.containsKey(Cell.create("row1".getBytes(), "col1".getBytes())));
        assertTrue(Arrays.equals("v5".getBytes(), vals.get(Cell.create("row1".getBytes(), "col1".getBytes()))));
        assertFalse(vals.containsKey(Cell.create("row2".getBytes(), "col1".getBytes())));
    }

    @Test
    public void testReadMyWritesAfterGetRange() throws InterruptedException, ExecutionException {
        Transaction t = startTransaction();
        put(t, "row0", "col1", "v0"); // this will come first in the range
        put(t, "row1", "col1", "v1");
        put(t, "row1", "col2", "v2");
        put(t, "row2", "col1", "v3");
        put(t, "row4", "col1", "v4");
        t.commit();

        t = startTransaction();
        assertEquals("v1", get(t, "row1", "col1"));
        assertEquals("v2", get(t, "row1", "col2"));
        assertEquals("v3", get(t, "row2", "col1"));

        // we need a bunch of buffer writes so that we don't exhaust the local iterator right away
        // because of peeking iterators looking ahead
        put(t, "a0", "col2", "v0");
        put(t, "b1", "col3", "v0");
        put(t, "c2", "col3", "v0");
        put(t, "d3", "col3", "v0");

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final BatchingVisitable<RowResult<byte[]>> visitable = t.getRange(TEST_TABLE, RangeRequest.builder().build());

        FutureTask<Void> futureTask = new FutureTask<Void>(() -> {
            final Map<Cell, byte[]> vals = Maps.newHashMap();
            try {
                visitable.batchAccept(1, AbortingVisitors.batching((RowVisitor) item -> {
                    try {
                        latch.countDown();
                        latch2.await();
                    } catch (InterruptedException e) {
                        throw Throwables.throwUncheckedException(e);
                    }
                    MapEntries.putAll(vals, item.getCells());
                    if (Arrays.equals(item.getRowName(), "row1".getBytes())) {
                        assertEquals("v5", new String(item.getColumns().get("col1".getBytes())));
                        assertEquals(3, IterableView.of(item.getCells()).size());
                    }
                    return true;
                }));
                assertTrue(vals.containsKey(Cell.create("row1".getBytes(), "col1".getBytes())));
                assertTrue(Arrays.equals("v5".getBytes(), vals.get(Cell.create("row1".getBytes(), "col1".getBytes()))));
                assertFalse(vals.containsKey(Cell.create("row2".getBytes(), "col1".getBytes())));
                return null;
            } catch (Throwable t1) {
                latch.countDown();
                Throwables.throwIfInstance(t1, Exception.class);
                throw Throwables.throwUncheckedException(t1);
            }
        });
        Thread thread = new Thread(futureTask);
        thread.setName("testReadMyWritesAfterGetRange");
        thread.start();

        latch.await();

        // These puts will be seen by the range scan happening on the other thread
        put(t, "row1", "col1", "v5"); // this put is checked to exist
        put(t, "row1", "col3", "v6"); // it is checked there are 3 cells for this
        put(t, "row3", "col1", "v7");
        delete(t, "row2", "col1"); // this delete is checked
        put(t, "row2", "col2", "v8");
        latch2.countDown();
        futureTask.get();
    }

    @Test
    public void testReadMyWritesManager() {
        getManager().runTaskWithRetry((TransactionTask<Void, RuntimeException>) t -> {
            put(t, "row1", "col1", "v1");
            put(t, "row1", "col2", "v2");
            put(t, "row2", "col1", "v3");
            assertEquals("v1", get(t, "row1", "col1"));
            assertEquals("v2", get(t, "row1", "col2"));
            assertEquals("v3", get(t, "row2", "col1"));
            return null;
        });

        getManager().runTaskWithRetry((TransactionTask<Void, RuntimeException>) t -> {
            assertEquals("v1", get(t, "row1", "col1"));
            assertEquals("v2", get(t, "row1", "col2"));
            assertEquals("v3", get(t, "row2", "col1"));
            return null;
        });
    }

    @Test
    public void testWriteFailsOnReadOnly() {
        try {
            getManager().runTaskReadOnly((TransactionTask<Void, RuntimeException>) t -> {
                put(t, "row1", "col1", "v1");
                return null;
            });
            fail();
        } catch (RuntimeException e) {
            // we want this to throw
        }
    }

    @Test
    public void testDelete() {
        getManager().runTaskWithRetry((TransactionTask<Void, RuntimeException>) t -> {
            put(t, "row1", "col1", "v1");
            assertEquals("v1", get(t, "row1", "col1"));
            delete(t, "row1", "col1");
            assertEquals(null, get(t, "row1", "col1"));
            return null;
        });

        getManager().runTaskWithRetry((TransactionTask<Void, RuntimeException>) t -> {
            put(t, "row1", "col1", "v1");
            return null;
        });

        getManager().runTaskWithRetry((TxTask) t -> {
            delete(t, "row1", "col1");
            return null;
        });

        getManager().runTaskWithRetry((TxTask) t -> {
            assertEquals(null, get(t, "row1", "col1"));
            return null;
        });

        getManager().runTaskWithRetry((TransactionTask<Void, RuntimeException>) t -> {
            put(t, "row1", "col1", "v1");
            return null;
        });

        getManager().runTaskWithRetry((TxTask) t -> {
            delete(t, "row1", "col1");
            return null;
        });

        getManager().runTaskWithRetry((TxTask) t -> {
            assertEquals(null, get(t, "row1", "col1"));
            return null;
        });
    }

    @Test
    public void testNoDirtyReads() {
        Transaction t1 = startTransaction();

        Transaction t2 = startTransaction();
        put(t2, "row1", "col1", "v1");
        t2.commit();

        assertNull(get(t1, "row1", "col1"));
    }

    @Test
    public void testNoNonRepeatableReads() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "v1");
        t0.commit();

        Transaction t1 = startTransaction();
        assertEquals("v1", get(t1, "row1", "col1"));

        Transaction t2 = startTransaction();
        put(t2, "row1", "col1", "v2");
        t2.commit();

        // Repeated read: should see original value.
        assertEquals("v1", get(t1, "row1", "col1"));

        Transaction t3 = startTransaction();
        assertEquals("v2", get(t3, "row1", "col1"));
    }

    @Test
    public void testWriteWriteConflict() {
        Transaction t1 = startTransaction();
        Transaction t2 = startTransaction();

        put(t1, "row1", "col1", "v1");
        put(t2, "row1", "col1", "v2");

        t1.commit();
        try {
            t2.commit();
            fail("Expected write-write conflict.");
        } catch (TransactionConflictException e) {
            // expected
        }
    }

    @Test
    public void testWriteWriteConflict2() {
        Transaction t2 = startTransaction();
        Transaction t1 = startTransaction();

        put(t1, "row1", "col1", "v1");
        put(t2, "row1", "col1", "v2");

        t1.commit();
        try {
            t2.commit();
            fail("Expected write-write conflict.");
        } catch (TransactionConflictException e) {
            // expected
        }
    }

    @Test
    public void testGetRanges() {
        Transaction t = startTransaction();
        byte[] row1Bytes = PtBytes.toBytes("row1");
        Cell row1Key = Cell.create(row1Bytes, PtBytes.toBytes("col"));
        byte[] row1Value = PtBytes.toBytes("value1");
        byte[] row2Bytes = PtBytes.toBytes("row2");
        Cell row2Key = Cell.create(row2Bytes, PtBytes.toBytes("col"));
        byte[] row2Value = PtBytes.toBytes("value2");
        t.put(TEST_TABLE, ImmutableMap.of(row1Key, row1Value, row2Key, row2Value));
        t.commit();

        t = startTransaction();
        List<RangeRequest> ranges = ImmutableList.of(
                RangeRequest.builder().prefixRange(row1Bytes).build(),
                RangeRequest.builder().prefixRange(row2Bytes).build());
        verifyAllGetRangesImplsNumRanges(t, ranges, ImmutableList.of("value1", "value2"));
    }

    @Test
    public void testGetRangesPaging() {
        Transaction t = startTransaction();
        byte[] row0Bytes = PtBytes.toBytes("row0");
        byte[] row00Bytes = PtBytes.toBytes("row00");
        byte[] colBytes = PtBytes.toBytes("col");
        Cell k1 = Cell.create(row00Bytes, colBytes);
        byte[] row1Bytes = PtBytes.toBytes("row1");
        Cell k2 = Cell.create(row1Bytes, colBytes);
        byte[] v = PtBytes.toBytes("v");
        t.put(TEST_TABLE, ImmutableMap.of(Cell.create(row0Bytes, colBytes), v));
        t.put(TEST_TABLE, ImmutableMap.of(k1, v));
        t.put(TEST_TABLE, ImmutableMap.of(k2, v));
        t.commit();

        t = startTransaction();
        t.delete(TEST_TABLE, ImmutableSet.of(k1));
        t.commit();

        t = startTransaction();
        byte[] rangeEnd = RangeRequests.nextLexicographicName(row00Bytes);
        List<RangeRequest> ranges = ImmutableList.of(RangeRequest.builder().prefixRange(row0Bytes).endRowExclusive(rangeEnd).batchHint(1).build());
        verifyAllGetRangesImplsNumRanges(t, ranges, ImmutableList.of("v"));
    }

    @Test
    public void getRowsAccessibleThroughCopies() {
        Transaction t = startTransaction();
        byte[] rowKey = row(0);
        byte[] value = value(0);
        t.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(rowKey, column(0)), value));
        t.commit();

        t = startTransaction();
        SortedMap<byte[], RowResult<byte[]>> result =
                t.getRows(TEST_TABLE, ImmutableList.of(rowKey), ColumnSelection.all());
        assertThat(result.get(rowKey))
                .as("it should be possible to get a row from getRows with a passed-in byte array")
                .isNotNull()
                .satisfies(rowResult ->
                        assertThat(rowResult.getOnlyColumnValue()).isEqualTo(value));

        byte[] rowKeyCopy = rowKey.clone();
        assertThat(rowKeyCopy).isNotSameAs(rowKey);
        assertThat(result.get(rowKeyCopy))
                .as("it should be possible to get a row from getRows with a copy of a passed-in byte array")
                .isNotNull()
                .satisfies(rowResult ->
                        assertThat(rowResult.getOnlyColumnValue()).isEqualTo(value));
    }

    @Test
    public void getRowsSortedByByteOrder() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        byte[] row1 = row(1);
        byte[] col0 = column(0);
        t.put(TEST_TABLE, ImmutableMap.of(Cell.create(row0, col0), value(0), Cell.create(row1, col0), value(1)));
        t.commit();

        t = startTransaction();
        SortedMap<byte[], RowResult<byte[]>> readRows =
                t.getRows(TEST_TABLE, ImmutableList.of(row0, row1), ColumnSelection.all());
        assertThat(readRows.firstKey()).containsExactly(row0);
        assertThat(readRows.lastKey()).containsExactly(row1);
    }

    @Test
    public void getRowsWithDuplicateQueries() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        byte[] anotherRow0 = row(0);
        byte[] col0 = column(0);
        t.put(TEST_TABLE, ImmutableMap.of(Cell.create(row0, col0), value(0)));
        t.commit();

        t = startTransaction();
        SortedMap<byte[], RowResult<byte[]>> readRows =
                t.getRows(TEST_TABLE, ImmutableList.of(row0, anotherRow0), ColumnSelection.all());
        assertThat(readRows.firstKey()).containsExactly(row0);
        assertThat(readRows).hasSize(1);
    }

    @Test
    public void getRowsAppliesColumnSelection() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        byte[] col0 = column(0);
        byte[] col1 = column(1);
        t.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(row0, col0), value(0),
                Cell.create(row0, col1), value(1)));
        t.commit();

        t = startTransaction();
        SortedMap<byte[], RowResult<byte[]>> readRows =
                t.getRows(TEST_TABLE, ImmutableList.of(row0), ColumnSelection.create(ImmutableList.of(col0)));
        assertThat(readRows.firstKey()).containsExactly(row0);
        assertThat(readRows.get(row0).getColumns().keySet()).containsExactly(col0);
        assertThat(readRows.get(row0).getColumns().get(col0)).containsExactly(value(0));
    }

    @Test
    public void getRowsIncludesLocalWrites() {
        Transaction t = startTransaction();
        byte[] rowKey = row(0);
        byte[] value = value(0);
        SortedMap<byte[], RowResult<byte[]>> prePut =
                t.getRows(TEST_TABLE, ImmutableList.of(row(0)), ColumnSelection.all());
        assertThat(prePut).isEmpty();
        t.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(rowKey, column(0)), value));
        SortedMap<byte[], RowResult<byte[]>> postPut =
                t.getRows(TEST_TABLE, ImmutableList.of(row(0)), ColumnSelection.all());
        assertThat(postPut)
                .hasSize(1)
                .containsKey(row(0));
    }

    @Test
    public void getRowsDoesNotIncludePersistedRowsWithLocalDeletes() {
        Transaction t = startTransaction();
        t.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(row(0), column(0)), value(0),
                Cell.create(row(1), column(0)), value(1),
                Cell.create(row(1), column(1)), value(2)));
        t.commit();

        t = startTransaction();
        SortedMap<byte[], RowResult<byte[]>> preDelete =
                t.getRows(TEST_TABLE, ImmutableList.of(row(0), row(1)), ColumnSelection.all());
        // cannot use containsOnlyKeys because that internally uses a LinkedHashSet
        assertThat(preDelete)
                .hasSize(2)
                .containsKey(row(0))
                .containsKey(row(1));
        t.delete(TEST_TABLE, ImmutableSet.of(Cell.create(row(0), column(0)), Cell.create(row(1), column(0))));
        SortedMap<byte[], RowResult<byte[]>> postDelete =
                t.getRows(TEST_TABLE, ImmutableList.of(row(0), row(1)), ColumnSelection.all());
        assertThat(postDelete)
                .hasSize(1)
                .containsKey(row(1));
        assertThat(postDelete.get(row(1)))
                .isNotNull()
                .satisfies(rowResult -> assertThat(
                        Arrays.equals(rowResult.getColumns().get(column(1)), value(2))).isTrue());
    }

    @Test
    public void lookupFromGetRowsColumnRange() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        byte[] row1 = row(1);
        byte[] col0 = column(0);
        t.put(TEST_TABLE, ImmutableMap.of(Cell.create(row0, col0), value(0), Cell.create(row1, col0), value(1)));
        t.commit();

        t = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> result = t.getRowsColumnRange(
                TEST_TABLE, ImmutableList.of(row0, row1), ALL_COLUMNS);

        Map<Cell, byte[]> directLookupResults = KeyedStream.ofEntries(
                BatchingVisitables.copyToList(result.get(row0)).stream()).collectToMap();
        assertThat(directLookupResults).hasSize(1);
        assertThat(Arrays.equals(directLookupResults.get(Cell.create(row0, col0)), value(0))).isTrue();

        Map<Cell, byte[]> indirectLookupResults = KeyedStream.ofEntries(
                BatchingVisitables.copyToList(result.get(row1.clone())).stream()).collectToMap();
        assertThat(indirectLookupResults).hasSize(1);
        assertThat(Arrays.equals(indirectLookupResults.get(Cell.create(row1.clone(), col0)), value(1))).isTrue();
    }

    @Test
    public void getRowsColumnRangeAbsentRow() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        t.commit();

        t = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> result = t.getRowsColumnRange(
                TEST_TABLE, ImmutableList.of(row0), ALL_COLUMNS);

        assertThat(result.keySet()).containsExactly(row0);

        Map<Cell, byte[]> results = KeyedStream.ofEntries(
                BatchingVisitables.copyToList(result.get(row0)).stream()).collectToMap();
        assertThat(results).hasSize(0);
    }

    @Test
    public void lookupFromGetRowsColumnRangeIterator() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        byte[] row1 = row(1);
        byte[] col0 = column(0);
        t.put(TEST_TABLE, ImmutableMap.of(Cell.create(row0, col0), value(0), Cell.create(row1, col0), value(1)));
        t.commit();

        t = startTransaction();
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> result = t.getRowsColumnRangeIterator(
                TEST_TABLE, ImmutableList.of(row0, row1), ALL_COLUMNS);

        Map<Cell, byte[]> directLookupResults = Maps.newHashMap();
        result.get(row0).forEachRemaining(entry -> directLookupResults.put(entry.getKey(), entry.getValue()));
        assertThat(directLookupResults).hasSize(1);
        assertThat(Arrays.equals(directLookupResults.get(Cell.create(row0, col0)), value(0))).isTrue();

        Map<Cell, byte[]> indirectLookupResults = Maps.newHashMap();
        result.get(row1.clone()).forEachRemaining(entry -> indirectLookupResults.put(entry.getKey(), entry.getValue()));
        assertThat(indirectLookupResults).hasSize(1);
        assertThat(Arrays.equals(indirectLookupResults.get(Cell.create(row1.clone(), col0)), value(1))).isTrue();
    }

    @Test
    public void getRowsColumnRangeIteratorAbsentLookup() {
        Transaction t = startTransaction();
        byte[] row0 = row(0);
        t.commit();

        t = startTransaction();
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> result = t.getRowsColumnRangeIterator(
                TEST_TABLE, ImmutableList.of(row0), ALL_COLUMNS);

        assertThat(result.keySet()).containsExactly(row0);

        Map<Cell, byte[]> results = Maps.newHashMap();
        result.get(row0).forEachRemaining(entry -> results.put(entry.getKey(), entry.getValue()));
        assertThat(results).hasSize(0);
    }

    @Test
    public void resilientToUnstableRowOrderingsForGetRowsColumnRange() {
        Assume.assumeTrue(canGetRowsColumnRangeOnTestTable());
        Transaction t = startTransaction();

        Map<Cell, byte[]> valuesToPut = KeyedStream.of(IntStream.range(0, 100).boxed())
                .flatMapKeys(index ->
                        Stream.of(Cell.create(row(index), column(0)), Cell.create(row(index), column(1))))
                .map(AbstractTransactionTest::value)
                .collectToMap();

        t.put(TEST_TABLE, valuesToPut);
        t.commit();

        t = startTransaction();
        Set<byte[]> rowNames = valuesToPut.keySet().stream().map(Cell::getRowName).collect(Collectors.toCollection(
                () -> new TreeSet<>(UnsignedBytes.lexicographicalComparator())));
        Iterator<Map.Entry<Cell, byte[]>> result = t.getRowsColumnRange(
                TEST_TABLE,
                UnstableOrderedIterable.create(rowNames, UnsignedBytes.lexicographicalComparator()),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                5);

        List<Map.Entry<Cell, byte[]>> resultAsList = Lists.newArrayList(result);
        assertThat(resultAsList).hasSize(200);
        for (int index = 0; index < 200; index += 2) {
            Map.Entry<Cell, byte[]> first = resultAsList.get(index);
            Map.Entry<Cell, byte[]> second = resultAsList.get(index + 1);
            assertMatchingRowAndInColumnOrder(first, second);
        }
    }

    private boolean canGetRowsColumnRangeOnTestTable() {
        try {
            Transaction t = startTransaction();
            t.getRowsColumnRange(
                    TEST_TABLE,
                    ImmutableList.of(row(0)),
                    new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                    42);
            return true;
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    @Test
    public void testTableMetadata() {
        keyValueService.dropTable(TEST_TABLE);
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        byte[] metadataForTable = keyValueService.getMetadataForTable(TEST_TABLE);
        assertTrue(metadataForTable == null || Arrays.equals(AtlasDbConstants.GENERIC_TABLE_METADATA, metadataForTable));
        byte[] bytes = TableMetadata.allDefault().persistToBytes();
        keyValueService.putMetadataForTable(TEST_TABLE, bytes);
        byte[] bytesRead = keyValueService.getMetadataForTable(TEST_TABLE);
        assertTrue(Arrays.equals(bytes, bytesRead));
        bytes = new TableDefinition() {{
            rowName();
            rowComponent("row", ValueType.FIXED_LONG);
            columns();
            column("col", "c", ValueType.VAR_STRING);
            conflictHandler(ConflictHandler.RETRY_ON_VALUE_CHANGED);
            negativeLookups();
            rangeScanAllowed();
            sweepStrategy(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            explicitCompressionRequested();
            explicitCompressionBlockSizeKB(128);
        }}.toTableMetadata().persistToBytes();
        keyValueService.putMetadataForTable(TEST_TABLE, bytes);
        bytesRead = keyValueService.getMetadataForTable(TEST_TABLE);
        assertTrue(Arrays.equals(bytes, bytesRead));
    }

    @Test
    public void getRangesProcessesVisitableOnOriginalElements() {
        UnaryOperator<RangeRequest> fiveElementLimit = range -> range.withBatchHint(5);
        RangeRequest sevenElementRequest = RangeRequest.builder()
                .startRowInclusive(PtBytes.toBytes("tom"))
                .batchHint(7)
                .build();
        RangeRequest nineElementRequest = RangeRequest.builder()
                .startRowInclusive(PtBytes.toBytes("tom"))
                .batchHint(9)
                .build();
        BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, RangeRequest> exposingProcessor
                = (rangeRequest, $) -> rangeRequest;

        Transaction transaction = startTransaction();
        List<RangeRequest> visited = transaction.getRanges(ImmutableGetRangesQuery.<RangeRequest>builder()
                .tableRef(TEST_TABLE)
                .rangeRequests(ImmutableList.of(sevenElementRequest, nineElementRequest))
                .rangeRequestOptimizer(fiveElementLimit)
                .visitableProcessor(exposingProcessor)
                .build())
                .collect(Collectors.toList());

        assertThat(visited).containsExactlyInAnyOrder(sevenElementRequest, nineElementRequest);
    }

    @Test
    public void getRangesSendsQueriesThatHaveGoneThroughTheOptimizer() {
        RangeRequest goldenRequest = RangeRequest.builder().startRowInclusive(PtBytes.toBytes("tom")).build();
        RangeRequest otherRequest = RangeRequest.builder().startRowInclusive(PtBytes.toBytes("zzzz")).build();

        // Contract is not entirely valid, but we don't have a good way of mocking out the KVS.
        UnaryOperator<RangeRequest> goldenForcingOperator = $ -> goldenRequest;

        putDirect("tom", "col", "value", 0);

        BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, byte[]> singleValueExtractor
                = ($, visitable) -> Iterables.getOnlyElement(BatchingVisitables.copyToList(visitable))
                        .getOnlyColumnValue();

        Transaction transaction = startTransaction();
        List<byte[]> extractedValue = transaction.getRanges(ImmutableGetRangesQuery.<byte[]>builder()
                .tableRef(TEST_TABLE)
                .rangeRequests(ImmutableList.of(otherRequest))
                .rangeRequestOptimizer(goldenForcingOperator)
                .visitableProcessor(singleValueExtractor)
                .build())
                .collect(Collectors.toList());
        assertThat(extractedValue).containsExactly(PtBytes.toBytes("value"));
    }

    private void verifyAllGetRangesImplsRangeSizes(Transaction t, RangeRequest templateRangeRequest, int expectedRangeSize) {
        Iterable<RangeRequest> rangeRequests = Iterables.limit(Iterables.cycle(templateRangeRequest), 1000);

        List<BatchingVisitable<RowResult<byte[]>>> getRangesWithPrefetchingImpl = ImmutableList.copyOf(
                t.getRanges(TEST_TABLE, rangeRequests));
        List<BatchingVisitable<RowResult<byte[]>>> getRangesInParallelImpl =
                t.getRanges(TEST_TABLE, rangeRequests, 2, (rangeRequest, visitable) -> visitable).collect(Collectors.toList());
        List<BatchingVisitable<RowResult<byte[]>>> getRangesLazyImpl =
                t.getRangesLazy(TEST_TABLE, rangeRequests).collect(Collectors.toList());

        assertEquals(getRangesWithPrefetchingImpl.size(), getRangesLazyImpl.size());
        assertEquals(getRangesLazyImpl.size(), getRangesInParallelImpl.size());

        for (int i = 0; i < getRangesWithPrefetchingImpl.size(); i++) {
            assertEquals(expectedRangeSize, BatchingVisitables.copyToList(getRangesWithPrefetchingImpl.get(i)).size());
            assertEquals(expectedRangeSize, BatchingVisitables.copyToList(getRangesInParallelImpl.get(i)).size());
            assertEquals(expectedRangeSize, BatchingVisitables.copyToList(getRangesLazyImpl.get(i)).size());
        }
    }

    private void verifyAllGetRangesImplsNumRanges(Transaction t, Iterable<RangeRequest> rangeRequests, List<String> expectedValues) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesWithPrefetchingImpl =
                t.getRanges(TEST_TABLE, rangeRequests);
        Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesInParallelImpl =
                t.getRanges(TEST_TABLE, rangeRequests, 2, (rangeRequest, visitable) -> visitable).collect(Collectors.toList());
        Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesLazyImpl =
                t.getRangesLazy(TEST_TABLE, rangeRequests).collect(Collectors.toList());

        assertEquals(expectedValues, extractStringsFromVisitables(getRangesWithPrefetchingImpl));
        assertEquals(expectedValues, extractStringsFromVisitables(getRangesInParallelImpl));
        assertEquals(expectedValues, extractStringsFromVisitables(getRangesLazyImpl));
    }

    private List<String> extractStringsFromVisitables(Iterable<BatchingVisitable<RowResult<byte[]>>> visitables) {
        return BatchingVisitables.concat(visitables)
                .transform(RowResult::getOnlyColumnValue)
                .transform(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .immutableCopy();
    }

    private static byte[] row(int index) {
        return toBytes("row" + index);
    }

    private static byte[] column(int index) {
        return toBytes("col" + index);
    }

    private static byte[] value(int index) {
        return toBytes("value" + index);
    }

    private static byte[] toBytes(String string) {
        return PtBytes.toBytes(string);
    }

    private static void assertMatchingRowAndInColumnOrder(Map.Entry<Cell, byte[]> fst, Map.Entry<Cell, byte[]> snd) {
        assertThat(Arrays.equals(fst.getKey().getRowName(), snd.getKey().getRowName())).isTrue();
        assertThat(UnsignedBytes.lexicographicalComparator().compare(
                fst.getKey().getColumnName(), snd.getKey().getColumnName())).isLessThan(0);
    }
}
