/*
 * Copyright 2016 Palantir Technologies
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
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
    private static final String FULL_TABLE_NAME = "test_table.xyz_atlasdb_sweeper_test";
    protected static final TableReference TABLE_NAME = TableReference.createFromFullyQualifiedName(FULL_TABLE_NAME);
    private static final String COL = "c";
    protected static final int DEFAULT_BATCH_SIZE = 1000;
    protected static final int DEFAULT_CELL_BATCH_SIZE = 1_000_000;

    protected KeyValueService kvs;
    protected final AtomicLong sweepTimestamp = new AtomicLong();
    protected SweepTaskRunner sweepRunner;
    protected LockAwareTransactionManager txManager;
    protected BackgroundSweeperImpl backgroundSweeper;
    protected LockService lockService;
    protected TransactionService txService;
    protected SweepMetrics sweepMetrics = Mockito.mock(SweepMetrics.class);

    @Before
    public void setup() {
        TimestampService tsService = new InMemoryTimestampService();
        this.kvs = SweepStatsKeyValueService.create(getKeyValueService(), tsService);
        LockClient lockClient = LockClient.of("sweep client");
        lockService = LockServiceImpl.create(new LockServerOptions() {
            @Override
            public boolean isStandaloneServer() {
                return false;
            }
        });
        txService = TransactionServices.createTransactionService(kvs);
        Supplier<AtlasDbConstraintCheckingMode> constraints = Suppliers.ofInstance(
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING);
        ConflictDetectionManager cdm = ConflictDetectionManagers.createWithoutWarmingCache(kvs);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        Cleaner cleaner = new NoOpCleaner();
        txManager = new SerializableTransactionManager(kvs, tsService, lockClient, lockService, txService,
                constraints, cdm, ssm, cleaner, false);
        setupTables(kvs);
        LongSupplier tsSupplier = sweepTimestamp::get;
        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, ImmutableList.of());
        sweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);
        setupBackgroundSweeper(DEFAULT_BATCH_SIZE);
    }

    protected static void setupTables(KeyValueService kvs) {
        tearDownTables(kvs);
        TransactionTables.createTables(kvs);
        Schemas.createTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    private static void tearDownTables(KeyValueService kvs) {
        Awaitility.await()
                .timeout(Duration.FIVE_MINUTES)
                .until(() -> {
                    kvs.getAllTableNames().stream()
                            .forEach(tableRef -> kvs.dropTable(tableRef));
                    return true;
                });
        TransactionTables.deleteTables(kvs);
        Schemas.deleteTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    protected void setupBackgroundSweeper(int batchSize) {
        Supplier<Boolean> sweepEnabledSupplier = () -> true;
        Supplier<Long> sweepNoPause = () -> 0L;
        Supplier<Integer> batchSizeSupplier = () -> batchSize;
        Supplier<Integer> cellBatchSizeSupplier = () -> DEFAULT_CELL_BATCH_SIZE;

        backgroundSweeper = new BackgroundSweeperImpl(txManager, kvs, sweepRunner, sweepEnabledSupplier, sweepNoPause,
                batchSizeSupplier, cellBatchSizeSupplier, SweepTableFactory.of(),
                new NoOpBackgroundSweeperPerformanceLogger(), sweepMetrics);
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
    public void testBackgroundSweepWritesPriorityTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        runBackgroundSweep(120, 3);
        List<SweepPriorityRowResult> results = txManager.runTaskReadOnly(t -> {
            SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
            return BatchingVisitables.copyToList(priorityTable.getAllRowsUnordered());
        });

        for (SweepPriorityRowResult result : results) {
            Assert.assertEquals(Long.valueOf(120L), result.getMinimumSweptTimestamp());
        }
    }

    @Test
    public void testBackgroundSweepWritesPriorityTableWithDifferentTime() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        // the expectation is that the sweep tables will be chosen first
        // TODO ^ why? Alphabetical?
        runBackgroundSweep(110, 2);
        runBackgroundSweep(120, 1);
        List<SweepPriorityRowResult> results = getPriorityTable();
        for (SweepPriorityRowResult result : results) {
            switch (result.getRowName().getFullTableName()) {
                case "sweep.priority":
                    Assert.assertEquals("priority has wrong sweep timestamp",
                            Long.valueOf(110L),
                            result.getMinimumSweptTimestamp());
                    break;
                case "sweep.progress":
                    Assert.assertEquals("progress has wrong sweep timestamp",
                            Long.valueOf(110L),
                            result.getMinimumSweptTimestamp());
                    break;
                case FULL_TABLE_NAME:
                    Assert.assertEquals("table has wrong sweep timestamp",
                            Long.valueOf(120L),
                            result.getMinimumSweptTimestamp());
                    Assert.assertEquals(Long.valueOf(1L), result.getCellsDeleted());
                    Assert.assertEquals(Long.valueOf(1L), result.getCellsExamined());
                    break;
                default:
                    // Cruft table; nothing to check
                    break;
            }
        }
    }

    @Test
    public void testBackgroundSweeperWritesToProgressTable() {
        setupBackgroundSweeper(2);
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo2", "bang", 75);
        putIntoDefaultColumn("foo3", "baz", 100);
        putIntoDefaultColumn("foo4", "buzz", 125);
        runBackgroundSweep(150, 3);

        confirmOnlyTableRowUnwritten();

        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();

        Assert.assertEquals(1, progressResults.size());
        SweepProgressRowResult result = Iterables.getOnlyElement(progressResults);
        Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
        Assert.assertEquals(TABLE_NAME.getQualifiedName(), result.getFullTableName());
        Assert.assertEquals(Long.valueOf(0L), result.getCellsDeleted());
        Assert.assertEquals(Long.valueOf(2L), result.getCellsExamined());
    }

    @Test
    public void testBackgroundSweeperDoesNotOverwriteProgressMinimumTimestamp() {
        setupBackgroundSweeper(2);
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo2", "bang", 75);
        putIntoDefaultColumn("foo3", "baz", 100);
        putIntoDefaultColumn("foo4", "buzz", 125);
        putIntoDefaultColumn("foo5", "bing", 140);
        runBackgroundSweep(150, 3);
        runBackgroundSweep(175, 1);

        confirmOnlyTableRowUnwritten();

        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();
        Assert.assertEquals(1, progressResults.size());
        SweepProgressRowResult result = Iterables.getOnlyElement(progressResults);
        Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
        Assert.assertEquals(TABLE_NAME.getQualifiedName(), result.getFullTableName());
        Assert.assertEquals(Long.valueOf(0L), result.getCellsDeleted());
        Assert.assertEquals(Long.valueOf(4L), result.getCellsExamined());
    }

    private void confirmOnlyTableRowUnwritten() {
        List<SweepPriorityRowResult> results = getPriorityTable();
        for (SweepPriorityRowResult result : results) {
            switch (result.getRowName().getFullTableName()) {
                case "sweep.priority":
                    Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
                    break;
                case "sweep.progress":
                    Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
                    break;
                case FULL_TABLE_NAME:
                    Assert.assertNull(result.getMinimumSweptTimestamp());
                    Assert.assertNull(result.getCellsDeleted());
                    Assert.assertNull(result.getCellsExamined());
                    break;
                default:
                    // Cruft table; nothing to check
                    break;
            }
        }
    }

    @Test
    public void testBackgroundSweeperWritesFromProgressToPriority() {
        setupBackgroundSweeper(3);
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo2", "bang", 75);
        putIntoDefaultColumn("foo3", "baz", 100);
        putIntoDefaultColumn("foo4", "buzz", 125);
        runBackgroundSweep(150, 3);
        runBackgroundSweep(175, 1);
        List<SweepPriorityRowResult> results = getPriorityTable();
        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();
        for (SweepPriorityRowResult result : results) {
            switch (result.getRowName().getFullTableName()) {
                case "sweep.priority":
                    Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
                    break;
                case "sweep.progress":
                    Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
                    break;
                case FULL_TABLE_NAME:
                    Assert.assertEquals(Long.valueOf(150L), result.getMinimumSweptTimestamp());
                    Assert.assertEquals(Long.valueOf(0L), result.getCellsDeleted());
                    Assert.assertEquals(Long.valueOf(4L), result.getCellsExamined());
                    break;
                default:
                    // Cruft table; nothing to check
                    break;
            }
        }

        Assert.assertEquals(0, progressResults.size());
    }

    @Test
    public void testBackgroundSweepCanHandleNegativeImmutableTimestamp() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        runBackgroundSweep(Long.MIN_VALUE, 3);
        List<SweepPriorityRowResult> results = txManager.runTaskReadOnly(t -> {
            SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
            return BatchingVisitables.copyToList(priorityTable.getAllRowsUnordered());
        });

        for (SweepPriorityRowResult result : results) {
            Assert.assertEquals(Long.valueOf(Long.MIN_VALUE), result.getMinimumSweptTimestamp());
        }
    }

    @Test
    public void testBackgroundSweeperSendsMetrics() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "bang", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        runBackgroundSweep(150, 3);

        Mockito.verify(sweepMetrics, atLeastOnce()).recordMetrics(eq(TABLE_NAME), any(SweepResults.class));
    }

    @Test
    public void testMetricsAndBatching() {
        createTable(SweepStrategy.CONSERVATIVE);
        long sweepTs = 150L;

        // put many rows
        int numberOfRows = DEFAULT_BATCH_SIZE * 2 - 1;
        for (int i = 0; i < numberOfRows; i++) {
            String row = "foo" + i;
            putUncommitted(row, "bar", 50);
            putUncommitted(row, "baz", 75);
        }
        putTimestampIntoTransactionTable(50);
        putTimestampIntoTransactionTable(75);

        runBackgroundSweep(sweepTs, 3);
        // check that TABLE_NAME has been partially swept by this point, but metrics were not recorded
        List<SweepProgressTable.SweepProgressRowResult> progressResults = getProgressTable();
        boolean tableSwept = false;
        for (SweepProgressTable.SweepProgressRowResult result : progressResults) {
            String fullTableName = result.getFullTableName();
            if (fullTableName == null) {
                continue;
            }
            switch (fullTableName) {
                case FULL_TABLE_NAME:
                    tableSwept = true;
                    Assert.assertEquals(Long.valueOf(sweepTs), result.getMinimumSweptTimestamp());
                    Assert.assertEquals(Long.valueOf(DEFAULT_BATCH_SIZE), result.getCellsDeleted());
                    Assert.assertEquals(Long.valueOf(DEFAULT_BATCH_SIZE), result.getCellsExamined());
                    break;
                default:
                    // Cruft table; nothing to check
                    break;
            }
        }
        assertTrue(tableSwept);
        Mockito.verify(sweepMetrics, never()).recordMetrics(eq(TABLE_NAME), any(SweepResults.class));

        runBackgroundSweep(sweepTs, 1);

        // now metrics should have been recorded
        SweepResults sweepResults = SweepResults.builder()
                .cellsDeleted(numberOfRows)
                .cellsExamined(numberOfRows)
                .sweptTimestamp(sweepTs)
                .build();
        Mockito.verify(sweepMetrics).recordMetrics(TABLE_NAME, sweepResults);
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

    /**
     * Test case causing the sweep DbKvs OOM #982. Takes about an hour to run, so should be @Ignored unless specifically
     * needed
     */
    @Ignore
    @Test
    public void testOom() {
        createTable(SweepStrategy.CONSERVATIVE);

        for (int col = 0; col < 10000; ++col) {
            Map<Cell, byte[]> toPut = new HashMap<>();
            for (int row = 0; row < 1000; ++row) {
                Cell cell = Cell.create(
                        Integer.toString(row).getBytes(),
                        Integer.toString(col).getBytes());
                toPut.put(cell, "foo".getBytes());
            }
            kvs.put(TABLE_NAME, toPut, col + 1000);
            putTimestampIntoTransactionTable(col + 1000);
            if (col % 1000 == 0) {
                kvs.put(TABLE_NAME, toPut, col + 11000);
                putTimestampIntoTransactionTable(col + 11000);
            }
            System.out.println("inserted col " + col);
        }
        System.out.println("starting sweep");
        SweepResults results = sweep(TABLE_NAME, 30000, 1000, 1000);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        Assert.assertEquals(10000L, results.getCellsDeleted());
    }

    @Ignore
    @Test
    public void wideRowTest() {
        createTable(SweepStrategy.CONSERVATIVE);
        Map<Cell, byte[]> toPut = new HashMap<>();
        for (int col = 0; col < 10000; ++col) {
            Cell cell = Cell.create(
                    "1".getBytes(),
                    Integer.toString(col).getBytes());
            toPut.put(cell, "foo".getBytes());
        }
        for (int timestamp = 1; timestamp < 1001; ++timestamp) {
            kvs.put(TABLE_NAME, toPut, timestamp);
            putTimestampIntoTransactionTable(timestamp);
            System.out.println("inserted timestamp " + timestamp);
        }
        System.out.println("starting sweep");
        SweepResults results = sweep(TABLE_NAME, 30000, 1000, 1000);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        Assert.assertEquals(999 * 10000L, results.getCellsDeleted());
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
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        Value val = kvs.get(TABLE_NAME, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents());
    }

    private Set<Long> getAllTs(String row) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
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
        Cell cell = Cell.create(row.getBytes(), column.getBytes());
        kvs.put(tableRef, ImmutableMap.of(cell, val.getBytes()), ts);
        putTimestampIntoTransactionTable(ts);
    }

    private void putTimestampIntoTransactionTable(long ts) {
        txService.putUnlessExists(ts, ts);
    }

    private void putUncommitted(final String row, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        kvs.put(TABLE_NAME, ImmutableMap.of(cell, val.getBytes()), ts);
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
