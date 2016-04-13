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
package com.palantir.atlasdb.cli.command;

import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.cli.runner.RocksDbTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.cli.services.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.atlasdb.cli.services.TestAtlasDbServices;
import com.palantir.atlasdb.cli.services.TestSweeperModule;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.TimestampService;

public class TestSweepCommand {

    private static final Namespace NS1 = Namespace.create("test");
    private static final Namespace NS2 = Namespace.create("diff");
    private static final TableReference TABLE_ONE = TableReference.create(NS1, "one");
    private static final TableReference TABLE_TWO = TableReference.create(NS1, "two");
    private static final TableReference TABLE_THREE = TableReference.create(NS2, "one");
    private static final String COL = "c";

    private static AtomicLong sweepTimestamp;
    private static AtlasDbServicesFactory moduleFactory;

    @BeforeClass
    public static void setup() throws Exception {
        sweepTimestamp = new AtomicLong();
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .testSweeperModule(TestSweeperModule.create(sweepTimestamp::get))
                        .build();
            }
        };
    }

    private RocksDbTestRunner makeRunner(String... args) {
        return new RocksDbTestRunner(SweepCommand.class, args);
    }

    @Test
    public void testSweepTable() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner("-t", TABLE_ONE.getQualifiedName())) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getTimestampService();
            KeyValueService kvs = services.getKeyValueService();

            createTable(kvs, TABLE_ONE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            createTable(kvs, TABLE_TWO, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            long ts1 = put(txm, TABLE_ONE, "foo", "bar");
            long ts2 = put(txm, TABLE_TWO, "foo", "tar");
            long ts3 = put(txm, TABLE_ONE, "foo", "baz");
            long ts4 = put(txm, TABLE_TWO, "foo", "taz");
            long ts5 = tss.getFreshTimestamp();
            String stdout = sweep(runner, ts5);

            Scanner scanner = new Scanner(stdout);
            final long uniqueCells = Long.parseLong(scanner.findInLine("\\d+ unique cells").split(" ")[0]);
            final long deletedCells = Long.parseLong(scanner.findInLine("deleted \\d+ cells").split(" ")[1]);
            Assert.assertEquals(1, uniqueCells);
            Assert.assertEquals(1, deletedCells);

            Assert.assertEquals("baz", get(kvs, TABLE_ONE, "foo", ts5));
            Assert.assertEquals("", get(kvs, TABLE_ONE, "foo", mid(ts1, ts3)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts3), getAllTs(kvs, TABLE_ONE, "foo"));
            Assert.assertEquals("taz", get(kvs, TABLE_TWO, "foo", ts5));
            Assert.assertEquals("tar", get(kvs, TABLE_TWO, "foo", mid(ts3, ts4)));
            Assert.assertEquals(ImmutableSet.of(ts2, ts4), getAllTs(kvs, TABLE_TWO, "foo"));
        }
    }


    @Test
    public void testSweepNamespace() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner("-n", NS1.getName())) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getTimestampService();
            KeyValueService kvs = services.getKeyValueService();

            createTable(kvs, TABLE_ONE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            createTable(kvs, TABLE_TWO, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            createTable(kvs, TABLE_THREE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            long ts1 = put(txm, TABLE_ONE, "foo", "bar");
            long ts2 = put(txm, TABLE_TWO, "foo", "tar");
            long ts3 = put(txm, TABLE_THREE, "foo", "jar");
            long ts4 = put(txm, TABLE_ONE, "foo", "baz");
            long ts5 = put(txm, TABLE_THREE, "foo", "jaz");
            long ts6 = put(txm, TABLE_TWO, "foo", "taz");
            long ts7 = tss.getFreshTimestamp();
            sweep(runner, ts7);

            Assert.assertEquals("baz", get(kvs, TABLE_ONE, "foo", ts7));
            Assert.assertEquals("", get(kvs, TABLE_ONE, "foo", mid(ts1, ts2)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts4), getAllTs(kvs, TABLE_ONE, "foo"));
            Assert.assertEquals("taz", get(kvs, TABLE_TWO, "foo", ts7));
            Assert.assertEquals("", get(kvs, TABLE_TWO, "foo", mid(ts4, ts6)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts6), getAllTs(kvs, TABLE_TWO, "foo"));
            Assert.assertEquals("jaz", get(kvs, TABLE_THREE, "foo", ts7));
            Assert.assertEquals("jar", get(kvs, TABLE_THREE, "foo", mid(ts3, ts5)));
            Assert.assertEquals(ImmutableSet.of(ts3, ts5), getAllTs(kvs, TABLE_THREE, "foo"));
        }
    }

    @Test
    public void testSweepAll() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner("-a")) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getTimestampService();
            KeyValueService kvs = services.getKeyValueService();

            createTable(kvs, TABLE_ONE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            createTable(kvs, TABLE_TWO, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            createTable(kvs, TABLE_THREE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            long ts1 = put(txm, TABLE_ONE, "foo", "bar");
            long ts2 = put(txm, TABLE_TWO, "foo", "tar");
            long ts3 = put(txm, TABLE_THREE, "foo", "jar");
            long ts4 = put(txm, TABLE_ONE, "foo", "baz");
            long ts5 = put(txm, TABLE_THREE, "foo", "jaz");
            long ts6 = put(txm, TABLE_TWO, "foo", "taz");
            long ts7 = tss.getFreshTimestamp();
            sweep(runner, ts7);

            Assert.assertEquals("baz", get(kvs, TABLE_ONE, "foo", ts7));
            Assert.assertEquals("", get(kvs, TABLE_ONE, "foo", mid(ts1, ts2)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts4), getAllTs(kvs, TABLE_ONE, "foo"));
            Assert.assertEquals("taz", get(kvs, TABLE_TWO, "foo", ts7));
            Assert.assertEquals("", get(kvs, TABLE_TWO, "foo", mid(ts4, ts6)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts6), getAllTs(kvs, TABLE_TWO, "foo"));
            Assert.assertEquals("jaz", get(kvs, TABLE_THREE, "foo", ts7));
            Assert.assertEquals("", get(kvs, TABLE_THREE, "foo", mid(ts3, ts5)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts5), getAllTs(kvs, TABLE_THREE, "foo"));
        }
    }

    @Test
    public void testSweepStartRow() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner("-t", TABLE_ONE.getQualifiedName(), "-r", BaseEncoding.base16().encode("foo".getBytes()))) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getTimestampService();
            KeyValueService kvs = services.getKeyValueService();

            createTable(kvs, TABLE_ONE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            long ts1 = put(txm, TABLE_ONE, "foo", "bar");
            long ts2 = put(txm, TABLE_ONE, "foo", "biz");
            long ts3 = put(txm, TABLE_ONE, "boo", "biz");
            long ts4 = put(txm, TABLE_ONE, "foo", "baz");
            long ts5 = tss.getFreshTimestamp();
            sweep(runner, ts5);

            Assert.assertEquals("baz", get(kvs, TABLE_ONE, "foo", ts5));
            Assert.assertEquals("", get(kvs, TABLE_ONE, "foo", mid(ts1, ts3)));
            Assert.assertEquals("", get(kvs, TABLE_ONE, "foo", mid(ts2, ts4)));
            Assert.assertEquals("biz", get(kvs, TABLE_ONE, "boo", mid(ts3, ts5)));
            Assert.assertEquals(ImmutableSet.of(-1L, ts4), getAllTs(kvs, TABLE_ONE, "foo"));
            Assert.assertEquals(ImmutableSet.of(ts3), getAllTs(kvs, TABLE_ONE, "boo"));
        }
    }

    private long mid(long low, long high) {
        return low + ((high -low) / 2);
    }

    private String sweep(SingleBackendCliTestRunner runner, long ts) {
        sweepTimestamp.set(ts);
        return runner.run();
    }

    private String get(KeyValueService kvs, TableReference table, String row, long ts) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        Value val = kvs.get(table, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents());
    }

    private Set<Long> getAllTs(KeyValueService kvs, TableReference table, String row) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        return ImmutableSet.copyOf(kvs.getAllTimestamps(table, ImmutableSet.of(cell), Long.MAX_VALUE).get(cell));
    }

    private long put(SerializableTransactionManager txm, TableReference table, String row, String val) {
        Cell cell = Cell.create(row.getBytes(), COL.getBytes());
        return txm.runTaskWithRetry(t -> {
            t.put(table, ImmutableMap.of(cell, val.getBytes()));
            return t.getTimestamp();
        });
    }

    private void createTable(KeyValueService kvs, TableReference table, final TableMetadataPersistence.SweepStrategy sweepStrategy) {
        kvs.createTable(table,
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
