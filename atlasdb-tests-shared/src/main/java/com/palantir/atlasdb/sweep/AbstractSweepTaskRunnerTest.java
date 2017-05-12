/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public abstract class AbstractSweepTaskRunnerTest {
    private static final String FULL_TABLE_NAME = "test_table.xyz_atlasdb_sweeper_test";
    protected static final TableReference TABLE_NAME = TableReference.createFromFullyQualifiedName(FULL_TABLE_NAME);
    private static final String COL = "c";
    protected static final int DEFAULT_BATCH_SIZE = 1000;
    protected static final int DEFAULT_CELL_BATCH_SIZE = 1_000_000;

    protected KeyValueService kvs;
    protected final AtomicLong sweepTimestamp = new AtomicLong();
    protected SweepTaskRunner sweepRunner;
    protected LockAwareTransactionManager txManager;
    protected TransactionService txService;
    protected PersistentLockManager persistentLockManager;

    @Before
    public void setup() {
        TimestampService tsService = new InMemoryTimestampService();
        kvs = SweepStatsKeyValueService.create(getKeyValueService(), tsService);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        txService = TransactionServices.createTransactionService(kvs);
        txManager = SweepTestUtils.setupTxManager(kvs, tsService, ssm, txService);
        LongSupplier tsSupplier = sweepTimestamp::get;
        persistentLockManager = new PersistentLockManager(
                SweepTestUtils.getPersistentLockService(kvs),
                AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS);
        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, persistentLockManager, ImmutableList.of());
        sweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);
    }

    @After
    public void close() {
        kvs.close();
    }

    /**
     * Called once before each test.
     *
     * @return the KVS used for testing
     */
    protected abstract KeyValueService getKeyValueService();

    @Test
    public void testSweepOneConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("baz", get("foo", 150));
        Assert.assertEquals("", get("foo", 80));
        Assert.assertEquals(ImmutableSet.of(-1L, 100L), getAllTs("foo"));
    }

    @Test
    public void testDontSweepLatestConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        SweepResults results = completeSweep(75);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepUncommittedConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 750));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyValuesConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("buzz", get("foo", 200));
        Assert.assertEquals("", get("foo", 124));
        Assert.assertEquals(ImmutableSet.of(-1L, 125L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyRowsConservative() {
        testSweepManyRows(SweepStrategy.CONSERVATIVE);
    }

    @Test
    public void testDontSweepFutureConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(110);
        Assert.assertEquals(2, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(-1L, 100L, 125L, 150L), getAllTs("foo"));
    }

    @Test
    public void testSweepOneThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("baz", get("foo", 150));
        Assert.assertNull(get("foo", 80));
        Assert.assertEquals(ImmutableSet.of(100L), getAllTs("foo"));
    }

    @Test
    public void testDontSweepLatestThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        SweepResults results = completeSweep(75);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepLatestDeletedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "", 50);
        SweepResults results = completeSweep(75);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertNull(get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test
    public void testSweepUncommittedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 750));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyValuesThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("buzz", get("foo", 200));
        Assert.assertNull(get("foo", 124));
        Assert.assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyRowsThorough() {
        testSweepManyRows(SweepStrategy.THOROUGH);
    }

    @Test
    public void testSweepManyLatestDeletedThorough1() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("", get("foo", 200));
        Assert.assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
        results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertNull(get("foo", 200));
        Assert.assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test
    public void testSweepManyLatestDeletedThorough2() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "foo", 125);
        putUncommitted("foo", "", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("foo", get("foo", 200));
        Assert.assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
    }

    @Test
    public void testDontSweepFutureThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(110);
        Assert.assertEquals(2, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(100L, 125L, 150L), getAllTs("foo"));
    }

    @Test
    public void testSweepStrategyNothing() {
        createTable(SweepStrategy.NOTHING);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(200);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(0, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(50L, 75L, 100L, 125L, 150L), getAllTs("foo"));
    }

    @Test
    public void testSweepResultsIncludeMinimumTimestamp() {
        createTable(SweepStrategy.CONSERVATIVE);
        SweepResults results = completeSweep(200);
        Assert.assertEquals(200, results.getSweptTimestamp());
    }

    @Test
    public void testSweeperFailsHalfwayThroughOnDeleteTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo2", "bang", 75);
        putIntoDefaultColumn("foo3", "baz", 100);
        putIntoDefaultColumn("foo4", "buzz", 125);
        partialSweep(150, 2);

        kvs.dropTable(TABLE_NAME);

        SweepResults results = completeSweep(175);

        Assert.assertEquals(results, SweepResults.createEmptySweepResult(0L));
    }

    @Test
    public void testSweepingAlreadySweptTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("row", "val", 10);
        putIntoDefaultColumn("row", "val", 20);
        completeSweep(30);

        SweepResults secondSweepResults = completeSweep(40);

        Assert.assertEquals(secondSweepResults.getCellsDeleted(), 0);
    }

    @Test
    public void testSweepOnMixedCaseTable() {
        TableReference mixedCaseTable = TableReference.create(Namespace.create("someNamespace"), "someTable");
        createTable(mixedCaseTable, SweepStrategy.CONSERVATIVE);
        put(mixedCaseTable, "row", "col", "val", 10);
        put(mixedCaseTable, "row", "col", "val", 20);

        SweepResults sweepResults = completeSweep(mixedCaseTable, 30);

        Assert.assertEquals(sweepResults.getCellsDeleted(), 1);
    }

    private void testSweepManyRows(SweepStrategy strategy) {
        createTable(strategy);
        putIntoDefaultColumn("foo", "bar1", 5);
        putIntoDefaultColumn("foo", "bar2", 10);
        putIntoDefaultColumn("baz", "bar3", 15);
        putIntoDefaultColumn("baz", "bar4", 20);

        SweepResults results = completeSweep(175);

        Assert.assertEquals(2, results.getCellsDeleted());
        Assert.assertEquals(2, results.getCellsExamined());
    }

    protected SweepResults completeSweep(long ts) {
        return completeSweep(TABLE_NAME, ts);
    }

    protected SweepResults completeSweep(TableReference tableReference, long ts) {
        SweepResults results = sweep(tableReference, ts, DEFAULT_BATCH_SIZE);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        return results;
    }

    private SweepResults sweep(long ts, int batchSize) {
        return sweep(TABLE_NAME, ts, batchSize);
    }

    private SweepResults sweep(TableReference tableReference, long ts, int batchSize) {
        return sweep(tableReference, ts, batchSize, DEFAULT_CELL_BATCH_SIZE);
    }

    protected SweepResults sweep(TableReference tableReference, long ts, int batchSize, int cellBatchSize) {
        sweepTimestamp.set(ts);
        return sweepRunner.run(tableReference, batchSize, cellBatchSize, new byte[0]);
    }

    private SweepResults partialSweep(long ts, int batchSize) {
        SweepResults results = sweep(ts, batchSize);
        assertTrue(results.getNextStartRow().isPresent());
        return results;
    }

    private String get(String row, long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        Value val = kvs.get(TABLE_NAME, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents(), StandardCharsets.UTF_8);
    }

    private Set<Long> getAllTs(String row) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        return ImmutableSet.copyOf(kvs.getAllTimestamps(TABLE_NAME, ImmutableSet.of(cell), Long.MAX_VALUE).get(cell));
    }

    protected void putIntoDefaultColumn(final String row, final String val, final long ts) {
        put(row, COL, val, ts);
    }

    protected void put(final String row, final String column, final String val, final long ts) {
        put(TABLE_NAME, row, column, val, ts);
    }

    protected void put(final TableReference tableRef,
            final String row,
            final String column,
            final String val,
            final long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), column.getBytes(StandardCharsets.UTF_8));
        kvs.put(tableRef, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8)), ts);
        putTimestampIntoTransactionTable(ts);
    }

    private void putTimestampIntoTransactionTable(long ts) {
        txService.putUnlessExists(ts, ts);
    }

    private void putUncommitted(final String row, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        kvs.put(TABLE_NAME, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8)), ts);
    }


    protected void createTable(SweepStrategy sweepStrategy) {
        createTable(TABLE_NAME, sweepStrategy);
    }

    protected void createTable(TableReference tableReference, SweepStrategy sweepStrategy) {
        kvs.createTable(tableReference,
                new TableDefinition() {
                    {
                        rowName();
                        rowComponent("row", ValueType.BLOB);
                        columns();
                        column("col", COL, ValueType.BLOB);
                        conflictHandler(ConflictHandler.IGNORE_ALL);
                        sweepStrategy(sweepStrategy);
                    }
                }.toTableMetadata().persistToBytes()
        );
    }
}
