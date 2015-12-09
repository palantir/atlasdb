/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.transaction.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.IterableView;
import com.palantir.common.collect.MapEntries;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.Pair;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public abstract class AbstractTransactionTest {
    protected static final String TEST_TABLE = "ns.table1";
    protected static final TableMetadata TEST_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription(
                    "row_name",
                    ValueType.STRING))),
            new ColumnMetadataDescription(new DynamicColumnDescription(
                    NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription(
                            "col_name",
                            ValueType.STRING))),
                    ColumnValueDescription.forType(ValueType.STRING))),
            ConflictHandler.RETRY_ON_WRITE_WRITE);

    protected static LockClient lockClient = null;
    protected static LockServiceImpl lockService = null;

    protected KeyValueService keyValueService;
    protected TimestampService timestampService;
    protected TransactionService transactionService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;

    @BeforeClass
    public static void setupLockClient() {
        if (lockClient == null) {
            lockClient = LockClient.of("fake lock client");
        }
    }

    @BeforeClass
    public static void setupLockService() {
        if (lockService == null) {
            lockService = LockServiceImpl.create(new LockServerOptions() {
                private static final long serialVersionUID = 1L;

                @Override
                public boolean isStandaloneServer() {
                    return false;
                }

            });
        }
    }

    @Before
    public void setUp() throws Exception {
        keyValueService = getKeyValueService();
        timestampService = new InMemoryTimestampService();
        keyValueService.initializeFromFreshInstance();
        keyValueService.createTables(ImmutableMap.of(
                TEST_TABLE, TEST_TABLE_METADATA.persistToBytes(),
                TransactionConstants.TRANSACTION_TABLE, TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes()));
        keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE, TransactionConstants.TRANSACTION_TABLE));
        transactionService = TransactionServices.createTransactionService(keyValueService);
        conflictDetectionManager = ConflictDetectionManagers.createDefault(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
    }

    @After
    public void tearDown() {
        keyValueService.teardown();
    }

    @AfterClass
    public static void tearDownClass() {
        if (lockService != null) {
            lockService.close();
            lockService = null;
        }
    }

    protected abstract KeyValueService getKeyValueService();

    protected boolean supportsReverse() {
        return true;
    }

    protected Set<String> getTestTables() {
        return ImmutableSet.of(TEST_TABLE);
    }

    protected Transaction startTransaction() {
        return new SnapshotTransaction(
                keyValueService,
                lockService,
                timestampService,
                transactionService,
                NoOpCleaner.INSTANCE,
                timestampService.getFreshTimestamp(),
                ImmutableMap.of(
                        TEST_TABLE,
                        ConflictHandler.RETRY_ON_WRITE_WRITE,
                        TransactionConstants.TRANSACTION_TABLE,
                        ConflictHandler.IGNORE_ALL),
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                TransactionReadSentinelBehavior.THROW_EXCEPTION);
    }

    protected TransactionManager getManager() {
        return new TestTransactionManagerImpl(keyValueService, timestampService, lockClient, lockService,
                transactionService, conflictDetectionManager, sweepStrategyManager);
    }

    protected void put(Transaction t,
                     String rowName,
                     String columnName,
                     String value) {
        put(t, TEST_TABLE, rowName, columnName, value);
    }

    private void put(Transaction t,
                     String tableName,
                     String rowName,
                     String columnName,
                     String value) {
        Cell k = Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
        byte[] v = value == null ? null : PtBytes.toBytes(value);
        HashMap<Cell, byte[]> map = Maps.newHashMap();
        map.put(k, v);
        t.put(tableName, map);
    }

    protected String get(Transaction t,
                       String rowName,
                       String columnName) {
        return get(t, TEST_TABLE, rowName, columnName);
    }

    protected String getCell(Transaction t,
                             String rowName,
                             String columnName) {
        return getCell(t, TEST_TABLE, rowName, columnName);
    }

    private String getCell(Transaction t,
                           String tableName,
                           String rowName,
                           String columnName) {
        byte[] row = PtBytes.toBytes(rowName);
        byte[] column = PtBytes.toBytes(columnName);
        Cell cell = Cell.create(row, column);
        Map<Cell, byte[]> map = t.get(tableName, ImmutableSet.of(cell));
        byte[] v = map.get(cell);
        return v != null ? PtBytes.toString(v) : null;
    }

    private String get(Transaction t,
                       String tableName,
                       String rowName,
                       String columnName) {
        byte[] row = PtBytes.toBytes(rowName);
        byte[] column = PtBytes.toBytes(columnName);
        Cell k = Cell.create(row, column);
        byte[] v = Cells.convertRowResultsToCells(
                t.getRows(tableName,
                          ImmutableSet.of(row),
                          ColumnSelection.create(ImmutableSet.of(column))).values()).get(k);
        return v != null ? PtBytes.toString(v) : null;
    }

    private void putDirect(String rowName,
                     String columnName,
                     String value, long timestamp) {
        Cell k = Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
        byte[] v = PtBytes.toBytes(value);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(k, v), timestamp);
    }

    private Pair<String, Long> getDirect(String rowName, String columnName, long timestamp) {
        return getDirect(TEST_TABLE, rowName, columnName, timestamp);
    }

    private Pair<String, Long> getDirect(String tableName,
                                         String rowName,
                                         String columnName,
                                         long timestamp) {
        byte[] row = PtBytes.toBytes(rowName);
        Cell k = Cell.create(row, PtBytes.toBytes(columnName));
        Value v = keyValueService.get(tableName, ImmutableMap.of(k, timestamp)).get(k);
        return v != null ? Pair.create(PtBytes.toString(v.getContents()), v.getTimestamp()) : null;
    }

    private Cell getCell(String rowName, String colName) {
        byte[] row = PtBytes.toBytes(rowName);
        return Cell.create(row, PtBytes.toBytes(colName));
    }

    @Test
    public void testBigValue() {
        byte[] bytes = new byte[64*1024];
        new Random().nextBytes(bytes);
        String encodeHexString = BaseEncoding.base16().lowerCase().encode(bytes);
        putDirect("row1", "col1", encodeHexString, 0);
        Pair<String, Long> pair = getDirect("row1", "col1", 1);
        Assert.assertEquals(0L, (long)pair.getRhSide());
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
        try {
            keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE,
                ImmutableMap.of(cell, "v2".getBytes()));
            fail();
        } catch (KeyAlreadyExistsException e) {
            //expected
        }
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
    public void testRangeAfterTimestmap() {
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
    public void testRangeAfterTimestmapReverse() {
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
        final Iterable<BatchingVisitable<RowResult<byte[]>>> ranges = t.getRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(allRange), 1000));
        for (BatchingVisitable<RowResult<byte[]>> batchingVisitable : ranges) {
            final List<RowResult<byte[]>> list = BatchingVisitables.copyToList(batchingVisitable);
            assertEquals(1, list.size());
        }
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
        ranges = t.getRanges(TEST_TABLE, Iterables.limit(Iterables.cycle(range3), 1000));
        for (BatchingVisitable<RowResult<byte[]>> batchingVisitable : ranges) {
            final List<RowResult<byte[]>> list = BatchingVisitables.copyToList(batchingVisitable);
            assertEquals(0, list.size());
        }
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
    public void testKeyValueMultiput() {
        keyValueService.createTable("table2", AtlasDbConstants.GENERIC_TABLE_METADATA);
        Cell k = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));
        String value = "whatever";
        byte[] v = PtBytes.toBytes(value);
        Map<Cell, byte[]> map = ImmutableMap.of(k, v);
        keyValueService.multiPut(ImmutableMap.of(TEST_TABLE, map, "table2", map), 0);
        assertEquals(value, getDirect("row", "col", 1).lhSide);
        assertEquals(value, getDirect("table2", "row", "col", 1).lhSide);
        keyValueService.dropTable("table2");
    }

    @Test
    public void testKeyValueDelete() {
        putDirect("row1", "col1", "v1", 0);
        Pair<String, Long> pair = getDirect("row1", "col1", 2);
        assertEquals(0L, (long)pair.getRhSide());
        assertEquals("v1", pair.getLhSide());
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 0L)));
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
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 2L)));
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
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 1L)));
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
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 0L)));
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
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 1L)));
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
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 1L)));
        keyValueService.delete(TEST_TABLE, Multimaps.forMap(ImmutableMap.of(getCell("row1", "col1"), 3L)));
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
        put(t, "row2", "col1", "");
        put(t, "row2", "col2", "v8");

        final Map<Cell, byte[]> vals = Maps.newHashMap();
        visitable.batchAccept(100, AbortingVisitors.batching(new RowVisitor() {
            @Override
            public boolean visit(RowResult<byte[]> item) throws RuntimeException {
                MapEntries.putAll(vals, item.getCells());
                if (Arrays.equals(item.getRowName(), "row1".getBytes())) {
                    assertEquals(3, IterableView.of(item.getCells()).size());
                    assertEquals("v5", new String(item.getColumns().get("col1".getBytes())));
                }
                return true;
            }
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

        FutureTask<Void> futureTask = new FutureTask<Void>(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final Map<Cell, byte[]> vals = Maps.newHashMap();
                try {
                    visitable.batchAccept(1, AbortingVisitors.batching(new RowVisitor() {
                        @Override
                        public boolean visit(RowResult<byte[]> item) throws RuntimeException {
                            try {
                                latch.countDown();
                                latch2.await();
                            } catch (InterruptedException e) {
                                throw Throwables.throwUncheckedException(e);
                            }
                            MapEntries.putAll(vals, item.getCells());
                            if (Arrays.equals(item.getRowName(),"row1".getBytes())) {
                                assertEquals("v5", new String(item.getColumns().get("col1".getBytes())));
                                assertEquals(3, IterableView.of(item.getCells()).size());
                            }
                            return true;
                        }
                    }));
                    assertTrue(vals.containsKey(Cell.create("row1".getBytes(), "col1".getBytes())));
                    assertTrue(Arrays.equals("v5".getBytes(), vals.get(Cell.create("row1".getBytes(), "col1".getBytes()))));
                    assertFalse(vals.containsKey(Cell.create("row2".getBytes(), "col1".getBytes())));
                    return null;
                } catch (Throwable t) {
                    latch.countDown();
                    Throwables.throwIfInstance(t, Exception.class);
                    throw Throwables.throwUncheckedException(t);
                }
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
        put(t, "row2", "col1", ""); // this delete is checked
        put(t, "row2", "col2", "v8");
        latch2.countDown();
        futureTask.get();
    }

    @Test
    public void testReadMyWritesManager() {
        getManager().runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                put(t, "row1", "col2", "v2");
                put(t, "row2", "col1", "v3");
                assertEquals("v1", get(t, "row1", "col1"));
                assertEquals("v2", get(t, "row1", "col2"));
                assertEquals("v3", get(t, "row2", "col1"));
                return null;
            }
        });

        getManager().runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                assertEquals("v1", get(t, "row1", "col1"));
                assertEquals("v2", get(t, "row1", "col2"));
                assertEquals("v3", get(t, "row2", "col1"));
                return null;
            }
        });
    }

    @Test
    public void testWriteFailsOnReadOnly() {
        try {
            getManager().runTaskReadOnly(new TransactionTask<Void, RuntimeException>() {
                @Override
                public Void execute(Transaction t) throws RuntimeException {
                    put(t, "row1", "col1", "v1");
                    return null;
                }
            });
            fail();
        } catch (RuntimeException e) {
            // we want this to throw
        }
    }

    @Test
    public void testNullDelete() {
        getManager().runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                assertEquals("v1", get(t, "row1", "col1"));
                put(t, "row1", "col1", null);
                assertEquals(null, get(t, "row1", "col1"));
                return null;
            }
        });

        getManager().runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                assertEquals("v1", get(t, "row1", "col1"));
                put(t, "row1", "col1", "");
                assertEquals(null, get(t, "row1", "col1"));
                return null;
            }
        });

        getManager().runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                return null;
            }
        });

        getManager().runTaskWithRetry(new TxTask() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "");
                return null;
            }
        });

        getManager().runTaskWithRetry(new TxTask() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                assertEquals(null, get(t, "row1", "col1"));
                return null;
            }
        });

        getManager().runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                return null;
            }
        });

        getManager().runTaskWithRetry(new TxTask() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", null);
                return null;
            }
        });

        getManager().runTaskWithRetry(new TxTask() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                assertEquals(null, get(t, "row1", "col1"));
                return null;
            }
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
    public void testTableMetadata() {
        byte[] metadataForTable = keyValueService.getMetadataForTable(TEST_TABLE);
        assertTrue(metadataForTable == null || Arrays.equals(TEST_TABLE_METADATA.persistToBytes(), metadataForTable));
        byte[] bytes = new TableMetadata().persistToBytes();
        keyValueService.putMetadataForTable(TEST_TABLE, bytes);
        byte[] bytesRead = keyValueService.getMetadataForTable(TEST_TABLE);
        assertTrue(Arrays.equals(bytes, bytesRead));
    }

    @Test
    public void testReadsBlockOnEarlierWrites() {
        //
    }

    @Test
    public void testReadsDontBlockOnLaterWrites() {
        //
    }
}
