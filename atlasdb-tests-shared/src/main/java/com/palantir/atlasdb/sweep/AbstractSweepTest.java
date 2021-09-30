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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers.CacheWarming;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractSweepTest {
    protected static final String FULL_TABLE_NAME = "test_table.xyz_atlasdb_sweeper_test";
    protected static final TableReference TABLE_NAME = TableReference.createFromFullyQualifiedName(FULL_TABLE_NAME);
    protected static final String COL = "c";

    protected static final List<Cell> SMALL_LIST_OF_CELLS = new ArrayList<>();
    protected static final List<Cell> BIG_LIST_OF_CELLS = new ArrayList<>();
    protected static final List<Cell> BIG_LIST_OF_CELLS_IN_DIFFERENT_ROWS = new ArrayList<>();

    static {
        for (int i = 0; i < 10; i++) {
            String zeroPaddedIndex = String.format("%05d", i);
            BIG_LIST_OF_CELLS.add(Cell.create(
                    "row".getBytes(StandardCharsets.UTF_8), (COL + zeroPaddedIndex).getBytes(StandardCharsets.UTF_8)));
            BIG_LIST_OF_CELLS_IN_DIFFERENT_ROWS.add(Cell.create(
                    ("row" + zeroPaddedIndex).getBytes(StandardCharsets.UTF_8),
                    (COL + zeroPaddedIndex).getBytes(StandardCharsets.UTF_8)));
        }
        SMALL_LIST_OF_CELLS.addAll(BIG_LIST_OF_CELLS.subList(0, 4));
    }

    private final KvsManager kvsManager;
    private final TransactionManagerManager tmManager;

    protected KeyValueService kvs;
    protected TransactionManager txManager;
    protected TransactionService txService;
    protected SweepStrategyManager ssm;

    private InMemoryTimelockServices services;

    protected AbstractSweepTest(KvsManager kvsManager, TransactionManagerManager tmManager) {
        this.kvsManager = kvsManager;
        this.tmManager = tmManager;
    }

    protected CacheWarming getSsmCacheWarming() {
        return CacheWarming.FULL;
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() {
        kvs = kvsManager.getDefaultKvs();
        ssm = SweepStrategyManagers.create(kvs, getSsmCacheWarming());
        services = InMemoryTimelockServices.create(tempFolder);
        txManager = createAndRegisterManager();
        txService = TransactionServices.createRaw(kvs, txManager.getTimestampService(), false);
        SweepTestUtils.setupTables(kvs);
    }

    @After
    public void tearDown() {
        services.close();
    }

    protected TransactionManager getManager() {
        return tmManager.getLastRegisteredTransactionManager().orElseGet(this::createAndRegisterManager);
    }

    protected TransactionManager createAndRegisterManager() {
        TransactionManager manager = SweepTestUtils.setupTxManager(
                kvs, services.getTimestampService(), services.getTimestampManagementService(), ssm, txService);
        tmManager.registerTransactionManager(manager);
        return manager;
    }

    @Test(timeout = 50000)
    public void testSweepOneConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);

        Optional<SweepResults> optResults = completeSweep(175);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isEqualTo("baz");
        assertThat(getFromDefaultColumn("foo", 80)).isEmpty();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(-1L, 100L));
    }

    @Test(timeout = 50000)
    public void testDontSweepLatestConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(0);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isEqualTo("bar");
        assertEqualsDisregardingExtraSentinels(ImmutableSet.of(50L), getAllTsFromDefaultColumn("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepManyValuesConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);

        Optional<SweepResults> optResults = completeSweep(175);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(2);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(3);
        });

        assertThat(getFromDefaultColumn("foo", 200)).isEqualTo("buzz");
        assertThat(getFromDefaultColumn("foo", 124)).isEmpty();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(-1L, 125L));
    }

    @Test(timeout = 50000)
    public void testSweepManyRowsConservative() {
        testSweepManyRows(SweepStrategy.CONSERVATIVE);
    }

    @Test(timeout = 50000)
    public void testDontSweepFutureConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);

        Optional<SweepResults> optResults = completeSweep(110);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(2);
            // Future timestamps don't count towards the examined count
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(3);
        });

        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(-1L, 100L, 125L, 150L));
    }

    @Test(timeout = 50000)
    public void testSweepOneThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);

        Optional<SweepResults> optResults = completeSweep(175);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isEqualTo("baz");
        assertThat(getFromDefaultColumn("foo", 80)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(100L));
    }

    @Test(timeout = 50000)
    public void testDontSweepLatestThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(0);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isEqualTo("bar");
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(50L));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiRowThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "", 50);
        put("foo-2", "other", "womp", 60);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEmpty();
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiColThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "other column", "other value", 40);
        putIntoDefaultColumn("foo", "", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        // The default column had its only value deleted
        assertThat(getFromDefaultColumn("foo", 150)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEmpty();

        // The other column was unaffected
        assertThat(get("foo", "other column", 150)).isEqualTo("other value");
        assertThat(getAllTs("foo", "other column")).isEqualTo(ImmutableSet.of(40L));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiValConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "old value", 40);
        putIntoDefaultColumn("foo", "", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isEmpty();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(-1L, 50L));
    }

    @Test(timeout = 50000)
    public void testSweepLatestNotDeletedMultiValThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "old value", 40);
        putIntoDefaultColumn("foo", "new value", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isEqualTo("new value");
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(50L));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiValThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "old value", 40);
        putIntoDefaultColumn("foo", "", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(2);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEmpty();

        // The second sweep has no cells to examine
        optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(0);
            assertThat(results.getCellTsPairsExamined()).isEqualTo(0);
        });
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "", 50);

        Optional<SweepResults> optResults = completeSweep(75);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });

        assertThat(getFromDefaultColumn("foo", 150)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEmpty();
    }

    @Test(timeout = 50000)
    public void testSweepManyRowsThorough() {
        testSweepManyRows(SweepStrategy.THOROUGH);
    }

    @Test(timeout = 50000)
    public void testSweepManyLatestDeletedThorough1() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "", 125);
        putIntoDefaultColumn("zzz", "bar", 51);

        Optional<SweepResults> optResults = completeSweep(175);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(3);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(4);
        });

        assertThat(getFromDefaultColumn("foo", 200)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEmpty();
    }

    @Test(timeout = 50000)
    public void testSweepManyLatestDeletedThorough2() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "foo", 125);

        Optional<SweepResults> optResults = completeSweep(175);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(2);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(3);
        });

        assertThat(getFromDefaultColumn("foo", 200)).isEqualTo("foo");
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(125L));
    }

    @Test(timeout = 50000)
    public void testDontSweepFutureThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);

        Optional<SweepResults> optResults = completeSweep(110);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(2);
            // Future timestamps don't count towards the examined count
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(3);
        });

        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(100L, 125L, 150L));
    }

    @Test(timeout = 50000)
    public void testSweepingAlreadySweptTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("row", "val", 10);
        putIntoDefaultColumn("row", "val", 20);

        Optional<SweepResults> optResults = completeSweep(30);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        });

        optResults = completeSweep(40);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(0);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        });
    }

    @Test(timeout = 50000)
    public void testSweepOnMixedCaseTable() {
        TableReference mixedCaseTable = TableReference.create(Namespace.create("someNamespace"), "someTable");
        createTable(mixedCaseTable, SweepStrategy.CONSERVATIVE);
        put(mixedCaseTable, "row", "col", "val", 10);
        put(mixedCaseTable, "row", "col", "val", 20);

        Optional<SweepResults> optResults = completeSweep(mixedCaseTable, 30);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        });
    }

    void assertEqualsDisregardingExtraSentinels(Set<Long> expectedTimestamps, Set<Long> actualTimestamps) {
        if (expectedTimestamps.contains(-1L)) {
            assertThat(actualTimestamps).isEqualTo(expectedTimestamps);
        } else {
            assertThat(Sets.difference(actualTimestamps, ImmutableSet.of(-1L))).isEqualTo(expectedTimestamps);
        }
    }

    protected void putTwoValuesInEachCell(List<Cell> cells) {
        createTable(SweepStrategy.CONSERVATIVE);

        int ts = 1;
        for (Cell cell : cells) {
            put(cell, "val1", ts);
            put(cell, "val2", ts + 5);
            ts += 10;
        }
    }

    private void testSweepManyRows(SweepStrategy strategy) {
        createTable(strategy);
        putIntoDefaultColumn("foo", "bar1", 5);
        putIntoDefaultColumn("foo", "bar2", 10);
        putIntoDefaultColumn("baz", "bar3", 15);
        putIntoDefaultColumn("baz", "bar4", 20);

        Optional<SweepResults> optResults = completeSweep(175);
        optResults.ifPresent(results -> {
            assertThat(results.getStaleValuesDeleted()).isEqualTo(2);
            assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(4);
        });
    }

    protected Optional<SweepResults> completeSweep(long ts) {
        return completeSweep(TABLE_NAME, ts);
    }

    protected abstract Optional<SweepResults> completeSweep(TableReference tableReference, long ts);

    protected String getFromDefaultColumn(String row, long ts) {
        return get(row, COL, ts);
    }

    private String get(String row, String column, long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), column.getBytes(StandardCharsets.UTF_8));
        Value val = kvs.get(TABLE_NAME, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents(), StandardCharsets.UTF_8);
    }

    protected Set<Long> getAllTsFromDefaultColumn(String row) {
        return getAllTs(row, COL);
    }

    private Set<Long> getAllTs(String row, String column) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), column.getBytes(StandardCharsets.UTF_8));
        return ImmutableSet.copyOf(kvs.getAllTimestamps(TABLE_NAME, ImmutableSet.of(cell), Long.MAX_VALUE)
                .get(cell));
    }

    protected void putIntoDefaultColumn(final String row, final String val, final long ts) {
        put(row, COL, val, ts);
    }

    protected void put(final String row, final String column, final String val, final long ts) {
        put(TABLE_NAME, row, column, val, ts);
    }

    protected void put(
            final TableReference tableRef, final String row, final String column, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), column.getBytes(StandardCharsets.UTF_8));
        put(tableRef, cell, val, ts);
    }

    protected void put(Cell cell, final String val, final long ts) {
        put(TABLE_NAME, cell, val, ts);
    }

    protected void put(final TableReference tableRef, Cell cell, final String val, final long ts) {
        Map<Cell, byte[]> writes = ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8));
        kvs.put(tableRef, writes, ts);
        putTimestampIntoTransactionTable(ts);
    }

    private void putTimestampIntoTransactionTable(long ts) {
        txService.putUnlessExists(ts, ts);
    }

    protected void putUncommitted(final String row, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        kvs.put(TABLE_NAME, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8)), ts);
    }

    protected void createTable(SweepStrategy sweepStrategy) {
        createTable(TABLE_NAME, sweepStrategy);
    }

    protected void createTable(TableReference tableReference, SweepStrategy sweepStrategy) {
        kvs.createTable(
                tableReference,
                new TableDefinition() {
                    {
                        rowName();
                        rowComponent("row", ValueType.BLOB);
                        columns();
                        column("col", COL, ValueType.BLOB);
                        conflictHandler(ConflictHandler.IGNORE_ALL);
                        sweepStrategy(sweepStrategy);
                    }
                }.toTableMetadata().persistToBytes());
    }
}
