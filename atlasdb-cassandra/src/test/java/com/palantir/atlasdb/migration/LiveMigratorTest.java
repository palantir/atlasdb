/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionTestSetup;

public class LiveMigratorTest extends TransactionTestSetup {
    private static final TableReference OLD_TABLE_REF = TableReference.createFromFullyQualifiedName("old.table");
    private static final TableReference NEW_TABLE_REF = TableReference.createFromFullyQualifiedName("new.table");
    private static final Cell CELL = createCell(0, 0);

    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    private KeyValueService kvs;
    private TransactionManager transactionManager;
    private DeterministicScheduler executor = new DeterministicScheduler();

    private ProgressCheckPoint checkPoint;

    private LiveMigrator liveMigrator;

    public LiveMigratorTest() {
        super(TRM, TRM);
    }

    @Before
    public void before() {
        kvs = TRM.getDefaultKvs();
        kvs.createTable(OLD_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(NEW_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        transactionManager = getManager();
        checkPoint = new KvsProgressCheckPointImpl(kvs);
        liveMigrator = new LiveMigrator(transactionManager, OLD_TABLE_REF, NEW_TABLE_REF, checkPoint, executor);
    }

    @After
    public void after() {
        Schemas.truncateTablesAndIndexes(KvsProgressCheckPointImpl.SCHEMA, kvs);
        kvs.truncateTables(ImmutableSet.of(OLD_TABLE_REF, NEW_TABLE_REF));
    }

    @Test
    public void testMigrationRuns() {
        writeToOldTable(1, 1);

        byte[] value = transactionManager.runTaskWithRetry(
                transaction -> transaction.get(OLD_TABLE_REF, ImmutableSet.of(CELL)).get(CELL));

        assertThat(value).containsExactly(PtBytes.toBytes(0L));

        liveMigrator.startMigration(() -> {
        });
        executor.tick(1, TimeUnit.SECONDS);

        assertValuesInTargetTable(1, 1);

    }

    @Test
    public void migrationRunsMultipleIterations() {
        writeToOldTable(5, 5);
        liveMigrator.setBatchSize(1);

        liveMigrator.startMigration(() -> {
        });

        executor.tick(15, TimeUnit.SECONDS);

        assertValuesInTargetTable(2, 5);

        executor.tick(30, TimeUnit.SECONDS);

        assertValuesInTargetTable(5, 5);
    }

    private void writeToOldTable(int rows, int cols) {
        transactionManager.runTaskWithRetry(transaction -> {
            IntStream.range(0, rows)
                    .forEach(row -> IntStream.range(0, cols)
                            .forEach(col ->
                                    transaction.put(OLD_TABLE_REF, ImmutableMap.of(
                                            createCell(row, col),
                                            PtBytes.toBytes(row * col)))));
            return null;
        });
    }

    private void assertValuesInTargetTable(int rows, int cols) {
        Set<Cell> cells = IntStream.range(0, rows).boxed()
                .flatMap(n -> IntStream.range(0, cols)
                        .mapToObj(m -> createCell(n, m)))
                .collect(Collectors.toSet());

        Map<Cell, byte[]> valueInNewTable = transactionManager.runTaskWithRetry(
                transaction -> transaction.get(NEW_TABLE_REF, cells));

        assertThat(valueInNewTable.keySet()).containsExactlyInAnyOrderElementsOf(cells);

        IntStream.range(0, rows).forEach(n -> IntStream.range(0, cols)
                .forEach(m -> assertThat(valueInNewTable.get(createCell(n, m))).containsExactly(
                        PtBytes.toBytes(n * m))));

        assertThat(transactionManager.runTaskWithRetry(
                transaction -> transaction.get(NEW_TABLE_REF, ImmutableSet.of(createCell(rows, 0L)))).size()
        ).isEqualTo(0);
    }

    private static Cell createCell(long rowName, long columnName) {
        return Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
    }
}
