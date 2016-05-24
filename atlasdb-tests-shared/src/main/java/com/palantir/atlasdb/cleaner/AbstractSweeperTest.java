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
package com.palantir.atlasdb.cleaner;

import static com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRowResult;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public abstract class AbstractSweeperTest {
    protected static final TableReference TABLE_NAME = TableReference.createWithEmptyNamespace("table");
    private static final String COL = "c";
    protected static final int DEFAULT_BATCH_SIZE = 1000;

    protected KeyValueService kvs;
    protected final AtomicLong sweepTimestamp = new AtomicLong();
    protected SweepTaskRunner sweepRunner;
    protected LockAwareTransactionManager txManager;
    protected BackgroundSweeperImpl backgroundSweeper;
    protected LockService lockService;
    protected TransactionService txService;

    @Before
    public void setup() {
        TimestampService tsService = new InMemoryTimestampService();
        this.kvs = new SweepStatsKeyValueService(getKeyValueService(), tsService);
        LockClient lockClient = LockClient.of("sweep client");
        lockService = LockServiceImpl.create(new LockServerOptions() { @Override public boolean isStandaloneServer() { return false; }});
        txService = TransactionServices.createTransactionService(kvs);
        Supplier<AtlasDbConstraintCheckingMode> constraints = Suppliers.ofInstance(AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING);
        ConflictDetectionManager cdm = ConflictDetectionManagers.createDefault(kvs);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        Cleaner cleaner = new NoOpCleaner();
        txManager = new SerializableTransactionManager(kvs, tsService, lockClient, lockService, txService, constraints, cdm, ssm, cleaner, false);
        setupTables(kvs);
        Supplier<Long> tsSupplier = sweepTimestamp::get;
        sweepRunner = new SweepTaskRunnerImpl(txManager, kvs, tsSupplier, tsSupplier, txService, ssm, ImmutableList.<Follower>of());
        setupBackgroundSweeper(DEFAULT_BATCH_SIZE);
    }

    protected static void setupTables(KeyValueService kvs) {
        tearDownTables(kvs);
        TransactionTables.createTables(kvs);
        Schemas.createTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    private static void tearDownTables(KeyValueService kvs) {
        kvs.dropTable(TABLE_NAME);
        TransactionTables.deleteTables(kvs);
        Schemas.deleteTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    protected void setupBackgroundSweeper(int batchSize) {
        Supplier<Boolean> sweepEnabledSupplier = () -> true;
        Supplier<Long> sweepNoPause = () -> 0L;
        Supplier<Integer> batchSizeSupplier = () -> batchSize;
        backgroundSweeper = new BackgroundSweeperImpl(txManager, kvs, sweepRunner, sweepEnabledSupplier, sweepNoPause, batchSizeSupplier, SweepTableFactory.of());
    }

    @After
    public void tearDown() {
        kvs.teardown();
    }

    /**
     * Called once before each test
     * @return the KVS used for testing
     */
    protected abstract KeyValueService getKeyValueService();

    @Test
    public void testSweepOneConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
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
        put("foo", "bar", 50);
        SweepResults results = completeSweep(75);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepUncommittedConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 750));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("buzz", get("foo", 200));
        Assert.assertEquals("", get("foo", 124));
        Assert.assertEquals(ImmutableSet.of(-1L, 125L), getAllTs("foo"));
    }

    @Test
    public void testDontSweepFutureConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(110);
        Assert.assertEquals(2, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(-1L, 100L, 125L, 150L), getAllTs("foo"));
    }

    @Test
    public void testSweepOneThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("baz", get("foo", 150));
        Assert.assertEquals(null, get("foo", 80));
        Assert.assertEquals(ImmutableSet.of(100L), getAllTs("foo"));
    }

    @Test
    public void testDontSweepLatestThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        SweepResults results = completeSweep(75);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepLatestDeletedThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "", 50);
        SweepResults results = completeSweep(75);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(null, get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test
    public void testSweepUncommittedThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 750));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("buzz", get("foo", 200));
        Assert.assertEquals(null, get("foo", 124));
        Assert.assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
    }

    @Test
    public void testSweepManyLatestDeletedThorough1() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("", get("foo", 200));
        Assert.assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
        results = completeSweep(175);
        Assert.assertEquals(1, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(null, get("foo", 200));
        Assert.assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test
    public void testSweepManyLatestDeletedThorough2() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "foo", 125);
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
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(110);
        Assert.assertEquals(2, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(100L, 125L, 150L), getAllTs("foo"));
    }

    @Test
    public void testSweepStrategyNothing() {
        createTable(SweepStrategy.NOTHING);
        put("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
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
    @Ignore
    /*
     * These tests have been ignored pending internal ticket 94009
     * They are fragile when run with test suites that do not properly
     * clean up tables from the kvs.
     */
    public void testBackgroundSweepWritesPriorityTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        runBackgroundSweep(120, 3);
        List<SweepPriorityRowResult> results = txManager.runTaskReadOnly(t -> {
            SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
            return BatchingVisitables.copyToList(priorityTable.getAllRowsUnordered());
        });

        for (SweepPriorityRowResult result : results) {
            Assert.assertEquals(new Long(120), result.getMinimumSweptTimestamp());
        }
    }

    @Test
    @Ignore
    public void testBackgroundSweepWritesPriorityTableWithDifferentTime() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        // the expectation is that the sweep tables will be chosen first
        runBackgroundSweep(110, 2);
        runBackgroundSweep(120, 1);
        List<SweepPriorityRowResult> results = getPriorityTable();
        for (SweepPriorityRowResult result : results) {
            switch (result.getRowName().getFullTableName()) {
                case "sweep.priority":
                    Assert.assertEquals(new Long(110), result.getMinimumSweptTimestamp());
                    break;
                case "sweep.progress":
                    Assert.assertEquals(new Long(110), result.getMinimumSweptTimestamp());
                    break;
                case "table":
                    Assert.assertEquals(new Long(120), result.getMinimumSweptTimestamp());
                    Assert.assertEquals(new Long(1), result.getCellsDeleted());
                    Assert.assertEquals(new Long(1), result.getCellsExamined());
                    break;
            }
        }
    }

    @Test
    @Ignore
    public void testBackgroundSweeperWritesToProgressTable() {
        setupBackgroundSweeper(2);
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo2", "bang", 75);
        put("foo3", "baz", 100);
        put("foo4", "buzz", 125);
        runBackgroundSweep(150, 3);

        confirmOnlyTableRowUnwritten();

        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();

        Assert.assertEquals(1, progressResults.size());
        SweepProgressRowResult result = Iterables.getOnlyElement(progressResults);
        Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
        Assert.assertEquals(TABLE_NAME.getQualifiedName(), result.getFullTableName());
        Assert.assertEquals(new Long(0), result.getCellsDeleted());
        Assert.assertEquals(new Long(2), result.getCellsExamined());
    }

    @Test
    @Ignore
    public void testBackgroundSweeperDoesNotOverwriteProgressMinimumTimestamp() {
        setupBackgroundSweeper(2);
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo2", "bang", 75);
        put("foo3", "baz", 100);
        put("foo4", "buzz", 125);
        put("foo5", "bing", 140);
        runBackgroundSweep(150, 3);
        runBackgroundSweep(175, 1);

        confirmOnlyTableRowUnwritten();

        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();
        Assert.assertEquals(1, progressResults.size());
        SweepProgressRowResult result = Iterables.getOnlyElement(progressResults);
        Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
        Assert.assertEquals(TABLE_NAME.getQualifiedName(), result.getFullTableName());
        Assert.assertEquals(new Long(0), result.getCellsDeleted());
        Assert.assertEquals(new Long(4), result.getCellsExamined());
    }

    private void confirmOnlyTableRowUnwritten() {
        List<SweepPriorityRowResult> results = getPriorityTable();
        for (SweepPriorityRowResult result : results) {
            switch (result.getRowName().getFullTableName()) {
                case "sweep.priority":
                    Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
                    break;
                case "sweep.progress":
                    Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
                    break;
                case "table":
                    Assert.assertNull(result.getMinimumSweptTimestamp());
                    Assert.assertNull(result.getCellsDeleted());
                    Assert.assertNull(result.getCellsExamined());
                    break;
            }
        }
    }

    @Test
    @Ignore
    public void testBackgroundSweeperWritesFromProgressToPriority() {
        setupBackgroundSweeper(3);
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo2", "bang", 75);
        put("foo3", "baz", 100);
        put("foo4", "buzz", 125);
        runBackgroundSweep(150, 3);
        runBackgroundSweep(175, 1);
        List<SweepPriorityRowResult> results = getPriorityTable();
        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();
        for (SweepPriorityRowResult result : results) {
            switch (result.getRowName().getFullTableName()) {
                case "sweep.priority":
                    Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
                    break;
                case "sweep.progress":
                    Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
                    break;
                case "table":
                    Assert.assertEquals(new Long(150), result.getMinimumSweptTimestamp());
                    Assert.assertEquals(new Long(0), result.getCellsDeleted());
                    Assert.assertEquals(new Long(4), result.getCellsExamined());
                    break;
            }
        }

        Assert.assertEquals(0, progressResults.size());
    }

    @Test
    @Ignore
    public void testBackgroundSweepCanHandleNegativeImmutableTimestamp() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
        put("foo", "buzz", 125);
        runBackgroundSweep(Long.MIN_VALUE, 3);
        List<SweepPriorityRowResult> results = txManager.runTaskReadOnly(t -> {
            SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
            return BatchingVisitables.copyToList(priorityTable.getAllRowsUnordered());
        });

        for (SweepPriorityRowResult result : results) {
            Assert.assertEquals(new Long(Long.MIN_VALUE), result.getMinimumSweptTimestamp());
        }
    }

    @Test
    public void testSweeperFailsHalfwayThroughOnDeleteTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo2", "bang", 75);
        put("foo3", "baz", 100);
        put("foo4", "buzz", 125);
        partialSweep(150, 2);

        kvs.dropTable(TABLE_NAME);

        SweepResults results = completeSweep(175);

        Assert.assertEquals(results, SweepResults.createEmptySweepResult(0L));
    }

    private List<SweepProgressRowResult> getProgressTable() {
        return txManager.runTaskReadOnly(t -> {
            SweepProgressTable progressTable = SweepTableFactory.of().getSweepProgressTable(t);
            return BatchingVisitables.copyToList(progressTable.getAllRowsUnordered());
        });
    }

    private List<SweepPriorityRowResult> getPriorityTable() {
        return txManager.runTaskReadOnly(t -> {
            SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
            return BatchingVisitables.copyToList(priorityTable.getAllRowsUnordered());
        });
    }

    private void runBackgroundSweep(long sweepTs, int numberOfTimes) {
        sweepTimestamp.set(sweepTs);
        for (int i = 0; i < numberOfTimes; i++) {
            backgroundSweeper.runOnce();
        }
    }

    private SweepResults completeSweep(long ts) {
        SweepResults results = sweep(ts, DEFAULT_BATCH_SIZE);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        return results;
    }

    private SweepResults sweep(long ts, int batchSize) {
        sweepTimestamp.set(ts);
        return sweepRunner.run(TABLE_NAME, batchSize, new byte[0]);
    }

    private SweepResults partialSweep(long ts, int batchSize) {
        SweepResults results = sweep(ts, batchSize);
        Assert.assertTrue(results.getNextStartRow().isPresent());
        return results;
    }

    private String get(String row, long ts) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        Value val = kvs.get(TABLE_NAME, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents());
    }

    private Set<Long> getAllTs(String row) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        return ImmutableSet.copyOf(kvs.getAllTimestamps(TABLE_NAME, ImmutableSet.of(cell), Long.MAX_VALUE).get(cell));
    }

    private void put(final String row, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        kvs.put(TABLE_NAME, ImmutableMap.of(cell, val.getBytes()), ts);
        txService.putUnlessExists(ts, ts);
    }

    private void putUncommitted(final String row, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        kvs.put(TABLE_NAME, ImmutableMap.of(cell, val.getBytes()), ts);
    }

    private void createTable(final SweepStrategy sweepStrategy) {
        kvs.createTable(TABLE_NAME,
                new TableDefinition() {{
                    rowName();
                    rowComponent("row", ValueType.BLOB);
                    columns();
                    column("col", COL, ValueType.BLOB);
                    conflictHandler(ConflictHandler.IGNORE_ALL);
                    sweepStrategy(sweepStrategy);
                }}.toTableMetadata().persistToBytes()
        );
    }
}
