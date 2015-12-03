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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.service.TransactionService;

public abstract class AbstractSweeperTest {
    protected static final String TABLE_NAME = "table";
    private static final String COL = "c";
    protected KeyValueService kvs;
    protected TransactionService txService;
    protected final AtomicLong sweepTimestamp = new AtomicLong();
    protected SweepTaskRunner sweepRunner;

    @Test
    public void testSweepOneConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(75);
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
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(110);
        Assert.assertEquals(2, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(-1L, 100L, 125L, 150L), getAllTs("foo"));
    }

    @Test
    public void testSweepOneThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "bar", 50);
        put("foo", "baz", 100);
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(75);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("bar", get("foo", 150));
        Assert.assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test
    public void testSweepLatestDeletedThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "", 50);
        SweepResults results = sweep(75);
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
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(175);
        Assert.assertEquals(4, results.getCellsDeleted());
        Assert.assertEquals(1, results.getCellsExamined());
        Assert.assertEquals("", get("foo", 200));
        Assert.assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
        results = sweep(175);
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
        SweepResults results = sweep(175);
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
        SweepResults results = sweep(110);
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
        SweepResults results = sweep(200);
        Assert.assertEquals(0, results.getCellsDeleted());
        Assert.assertEquals(0, results.getCellsExamined());
        Assert.assertEquals(ImmutableSet.of(50L, 75L, 100L, 125L, 150L), getAllTs("foo"));
    }

    private SweepResults sweep(long ts) {
        sweepTimestamp.set(ts);
        SweepResults results = sweepRunner.run(TABLE_NAME, 1000, new byte[0]);
        Assert.assertFalse(results.getNextStartRow().isPresent());
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
