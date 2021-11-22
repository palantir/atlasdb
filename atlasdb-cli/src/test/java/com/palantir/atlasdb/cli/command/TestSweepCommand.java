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
package com.palantir.atlasdb.cli.command;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.keyvalue.api.*;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestSweeperModule;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.TimestampService;
import io.airlift.airline.Command;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class TestSweepCommand {

    private static final Namespace NS1 = Namespace.create("test");
    private static final Namespace NS2 = Namespace.create("diff");
    private static final TableReference TABLE_ONE = TableReference.create(NS1, "one");
    private static final TableReference TABLE_TWO = TableReference.create(NS1, "two");
    private static final TableReference NON_EXISTING_TABLE = TableReference.create(NS1, "non-existing");
    private static final TableReference TABLE_THREE = TableReference.create(NS2, "one");
    private static final String COL = "c";
    private static final String SWEEP_COMMAND =
            SweepCommand.class.getAnnotation(Command.class).name();

    private static AtomicLong sweepTimestamp;
    private static AtlasDbServicesFactory moduleFactory;

    @Parameterized.Parameter
    public boolean dryRun;

    @Parameterized.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(Boolean.FALSE, Boolean.TRUE);
    }

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

    private InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(SweepCommand.class, args);
    }

    @Test
    public void testSweepTable() throws Exception {
        try (SingleBackendCliTestRunner runner =
                makeRunner(paramsWithDryRunSet(SWEEP_COMMAND, "-t", TABLE_ONE.getQualifiedName()))) {
            TestAtlasDbServices services = (TestAtlasDbServices) runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getManagedTimestampService();
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
            final long cellValuesExamined =
                    Long.parseLong(Iterables.get(Splitter.on(' ').split(scanner.findInLine("\\d+ cell values")), 0));
            final long deletedCells = Long.parseLong(Iterables.get(
                    Splitter.on(' ').split(scanner.findInLine("deleted \\d+ stale versions of those cells")), 1));
            assertThat(cellValuesExamined).isEqualTo(2);
            assertThat(deletedCells).isEqualTo(1);

            assertThat(get(kvs, TABLE_ONE, "foo", ts5)).isEqualTo("baz");
            assertThat(get(kvs, TABLE_ONE, "foo", mid(ts1, ts3))).isEqualTo(deletedValue("bar"));
            assertThat(getAllTs(kvs, TABLE_ONE, "foo")).isEqualTo(ImmutableSet.of(deletedTimestamp(ts1), ts3));
            assertThat(get(kvs, TABLE_TWO, "foo", ts5)).isEqualTo("taz");
            assertThat(get(kvs, TABLE_TWO, "foo", mid(ts3, ts4))).isEqualTo("tar");
            assertThat(getAllTs(kvs, TABLE_TWO, "foo")).isEqualTo(ImmutableSet.of(ts2, ts4));
        }
    }

    @Test
    public void testSweepNonExistingTable() throws Exception {
        try (SingleBackendCliTestRunner runner =
                makeRunner(paramsWithDryRunSet(SWEEP_COMMAND, "-t", NON_EXISTING_TABLE.getQualifiedName()))) {
            TestAtlasDbServices services = (TestAtlasDbServices) runner.connect(moduleFactory);

            long ts5 = services.getManagedTimestampService().getFreshTimestamp();
            String stdout = sweep(runner, ts5);

            assertThat(stdout).doesNotContain("Swept from");
            assertThat(stdout)
                    .contains(String.format("The table %s passed in to sweep does not exist", NON_EXISTING_TABLE));
        }
    }

    @Test
    public void testSweepNamespace() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(paramsWithDryRunSet(SWEEP_COMMAND, "-n", NS1.getName()))) {
            TestAtlasDbServices services = (TestAtlasDbServices) runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getManagedTimestampService();
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

            assertThat(get(kvs, TABLE_ONE, "foo", ts7)).isEqualTo("baz");
            assertThat(get(kvs, TABLE_ONE, "foo", mid(ts1, ts2))).isEqualTo(deletedValue("bar"));
            assertThat(getAllTs(kvs, TABLE_ONE, "foo")).isEqualTo(ImmutableSet.of(deletedTimestamp(ts1), ts4));
            assertThat(get(kvs, TABLE_TWO, "foo", ts7)).isEqualTo("taz");
            assertThat(get(kvs, TABLE_TWO, "foo", mid(ts4, ts6))).isEqualTo(deletedValue("tar"));
            assertThat(getAllTs(kvs, TABLE_TWO, "foo")).isEqualTo(ImmutableSet.of(deletedTimestamp(ts2), ts6));
            assertThat(get(kvs, TABLE_THREE, "foo", ts7)).isEqualTo("jaz");
            assertThat(get(kvs, TABLE_THREE, "foo", mid(ts3, ts5))).isEqualTo("jar");
            assertThat(getAllTs(kvs, TABLE_THREE, "foo")).isEqualTo(ImmutableSet.of(ts3, ts5));
        }
    }

    @Test
    public void testSweepAll() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(paramsWithDryRunSet(SWEEP_COMMAND, "-a"))) {
            TestAtlasDbServices services = (TestAtlasDbServices) runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getManagedTimestampService();
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

            assertThat(get(kvs, TABLE_ONE, "foo", ts7)).isEqualTo("baz");
            assertThat(get(kvs, TABLE_ONE, "foo", mid(ts1, ts2))).isEqualTo(deletedValue("bar"));
            assertThat(getAllTs(kvs, TABLE_ONE, "foo")).isEqualTo(ImmutableSet.of(deletedTimestamp(ts1), ts4));
            assertThat(get(kvs, TABLE_TWO, "foo", ts7)).isEqualTo("taz");
            assertThat(get(kvs, TABLE_TWO, "foo", mid(ts4, ts6))).isEqualTo(deletedValue("tar"));
            assertThat(getAllTs(kvs, TABLE_TWO, "foo")).isEqualTo(ImmutableSet.of(deletedTimestamp(ts2), ts6));
            assertThat(get(kvs, TABLE_THREE, "foo", ts7)).isEqualTo("jaz");
            assertThat(get(kvs, TABLE_THREE, "foo", mid(ts3, ts5))).isEqualTo(deletedValue("jar"));
            assertThat(getAllTs(kvs, TABLE_THREE, "foo")).isEqualTo(ImmutableSet.of(deletedTimestamp(ts3), ts5));
        }
    }

    @Test
    public void testSweepStartRow() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(paramsWithDryRunSet(
                SWEEP_COMMAND,
                "-t",
                TABLE_ONE.getQualifiedName(),
                "-r",
                BaseEncoding.base16().encode("foo".getBytes(StandardCharsets.UTF_8))))) {
            TestAtlasDbServices services = (TestAtlasDbServices) runner.connect(moduleFactory);
            SerializableTransactionManager txm = services.getTransactionManager();
            TimestampService tss = services.getManagedTimestampService();
            KeyValueService kvs = services.getKeyValueService();

            createTable(kvs, TABLE_ONE, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            long ts1 = put(txm, TABLE_ONE, "foo", "bar");
            long ts2 = put(txm, TABLE_ONE, "foo", "biz");
            long ts3 = put(txm, TABLE_ONE, "boo", "biz");
            long ts4 = put(txm, TABLE_ONE, "foo", "baz");
            long ts5 = tss.getFreshTimestamp();
            sweep(runner, ts5);

            assertThat(get(kvs, TABLE_ONE, "foo", ts5)).isEqualTo("baz");
            assertThat(get(kvs, TABLE_ONE, "foo", mid(ts1, ts3))).isEqualTo(deletedValue("bar"));
            assertThat(get(kvs, TABLE_ONE, "foo", mid(ts2, ts4))).isEqualTo(deletedValue("biz"));
            assertThat(get(kvs, TABLE_ONE, "boo", mid(ts3, ts5))).isEqualTo("biz");
            assertThat(getAllTs(kvs, TABLE_ONE, "foo"))
                    .isEqualTo(ImmutableSet.of(deletedTimestamp(ts1), deletedTimestamp(ts2), ts4));
            assertThat(getAllTs(kvs, TABLE_ONE, "boo")).isEqualTo(ImmutableSet.of(ts3));
        }
    }

    private String[] paramsWithDryRunSet(String... params) {
        List<String> paramList = new ArrayList<>(Arrays.asList(params));
        if (dryRun) {
            paramList.add("--dry-run");
        }

        return paramList.toArray(new String[0]);
    }

    private String deletedValue(String oldValue) {
        return dryRun ? oldValue : "";
    }

    private long deletedTimestamp(long ts) {
        return dryRun ? ts : -1L;
    }

    private long mid(long low, long high) {
        return low + ((high - low) / 2);
    }

    private String sweep(SingleBackendCliTestRunner runner, long ts) {
        sweepTimestamp.set(ts);
        return runner.run();
    }

    private String get(KeyValueService kvs, TableReference table, String row, long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        Value val = kvs.get(table, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents(), StandardCharsets.UTF_8);
    }

    private Set<Long> getAllTs(KeyValueService kvs, TableReference table, String row) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        return ImmutableSet.copyOf(kvs.getAllTimestamps(table, ImmutableSet.of(cell), Long.MAX_VALUE)
                .get(cell));
    }

    private long put(SerializableTransactionManager txm, TableReference table, String row, String val) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        return txm.runTaskWithRetry(t -> {
            t.put(table, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8)));
            return t.getTimestamp();
        });
    }

    @SuppressWarnings("checkstyle:RightCurly") // TableDefinition syntax
    private void createTable(
            KeyValueService kvs, TableReference table, final TableMetadataPersistence.SweepStrategy sweepStrategy) {
        kvs.createTable(
                table,
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
