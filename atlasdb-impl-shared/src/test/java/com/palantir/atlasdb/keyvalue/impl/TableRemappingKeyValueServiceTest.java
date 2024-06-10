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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableMappingCacheConfiguration;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TableRemappingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("namespace");
    private static final String TABLENAME = "test";
    private static final TableReference DATA_TABLE_REF = TableReference.create(NAMESPACE, TABLENAME);

    private static final int NUM_THREADS = 100;
    private static final int NUM_ITERATIONS = 1_000;

    private final AtomicLong timestamp = new AtomicLong();
    private final KeyValueService rawKvs = new InMemoryKeyValueService(true);
    private final KvTableMappingService tableMapper = KvTableMappingService.create(rawKvs, timestamp::incrementAndGet);
    private final KeyValueService kvs = TableRemappingKeyValueService.create(rawKvs, tableMapper);

    @Test
    public void canConcurrentlyDropDisjointSetsOfTables() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String firstWorld = generateRandomTablesPrefixed();
            String secondWorld = generateRandomTablesPrefixed();
            final Set<Future<Void>> futures = new HashSet<>();
            futures.add(executor.submit(() -> dropTablesWithPrefix(firstWorld)));
            futures.add(executor.submit(() -> dropTablesWithPrefix(secondWorld)));
            futures.forEach(Futures::getUnchecked);
        }
    }

    @Test
    public void canConcurrentlyReadFromStableTablesWhileCreatingAndDroppingOtherTables() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        kvs.createTable(DATA_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Cell testCell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));
        byte[] value = PtBytes.toBytes("value");
        long testTimestamp = 1L;
        kvs.put(DATA_TABLE_REF, ImmutableMap.of(testCell, value), testTimestamp);
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String prefix = generateRandomTablesPrefixed();
            final Set<Future<Void>> futures = new HashSet<>();
            futures.add(executor.submit(() -> dropTablesWithPrefix(prefix)));
            futures.add(executor.submit(() -> {
                Map<Cell, Value> read = kvs.get(DATA_TABLE_REF, ImmutableMap.of(testCell, 2L));
                assertThat(read).containsExactly(Maps.immutableEntry(testCell, Value.create(value, testTimestamp)));
                return null;
            }));
            futures.forEach(Futures::getUnchecked);
        }
    }

    @Test
    public void concurrentlyCreateAndDeleteTablesAcrossManyWriters() {
        int maxWritersMaybeWeAreSatisfiedTheBugIsGone = 4;
        int createAndDeleteWriters = 1;
        int currentAttempt = 0;
        int attemptsUntilIncrement = 10;
        int tableCount = 1000;

        for (int i = 0; i < tableCount; i++) {
            kvs.createTable(
                    TableReference.create(NAMESPACE, TABLENAME + "_" + i), AtlasDbConstants.GENERIC_TABLE_METADATA);
        }

        while (true) {
            if (currentAttempt == attemptsUntilIncrement) {
                if (createAndDeleteWriters == maxWritersMaybeWeAreSatisfiedTheBugIsGone) {
                    return;
                }

                createAndDeleteWriters++;
                currentAttempt = 0;
                System.err.println("More readers and writers: " + createAndDeleteWriters);
            } else {
                currentAttempt++;
            }

            ExecutorService executor = Executors.newFixedThreadPool(createAndDeleteWriters);

            // Try and ensure the read-write party happens as close to simultaneously as possible
            CyclicBarrier waitUntilAllFuturesAreReady = new CyclicBarrier(createAndDeleteWriters);
            final Set<Future<Void>> futures = new HashSet<>();
            for (int i = 0; i < createAndDeleteWriters; i++) {
                futures.add(executor.submit(() -> {
                    AtomicLong timestamp = new AtomicLong();
                    KvTableMappingService tableMapper = KvTableMappingService.createWithCustomNamespaceValidation(
                            rawKvs,
                            timestamp::incrementAndGet,
                            Namespace.STRICTLY_CHECKED_NAME,
                            new TableMappingCacheConfiguration() {
                                @Override
                                public Predicate<TableReference> cacheableTablePredicate() {
                                    return tableReference -> false;
                                }
                            });
                    KeyValueService kvs = TableRemappingKeyValueService.create(rawKvs, tableMapper);
                    waitUntilAllFuturesAreReady.await();
                    for (int j = 0; j < tableCount; j++) {
                        kvs.dropTable(TableReference.create(NAMESPACE, TABLENAME + "_" + j));
                        kvs.createTable(
                                TableReference.create(NAMESPACE, TABLENAME + "_" + j),
                                AtlasDbConstants.GENERIC_TABLE_METADATA);
                    }

                    return null;
                }));
            }

            futures.forEach(Futures::getUnchecked);
        }
    }

    private Void dropTablesWithPrefix(String prefix) {
        String namespacedPrefix = NAMESPACE.getName() + "." + prefix + "__";
        Set<TableReference> tablesToDrop = kvs.getAllTableNames().stream()
                .filter(tableRef -> tableRef.getQualifiedName().startsWith(namespacedPrefix))
                .collect(Collectors.toSet());
        kvs.dropTables(tablesToDrop);
        return null;
    }

    @SuppressWarnings("checkstyle:all") // Table definition doesn't follow usual rules.
    private String generateRandomTablesPrefixed() {
        String worldName = UUID.randomUUID().toString().replace("-", "_");
        String tableName = UUID.randomUUID().toString().replace("-", "_");
        kvs.createTable(
                TableReference.create(NAMESPACE, worldName + "__" + tableName),
                new TableDefinition() {
                    {
                        javaTableName(tableName);
                        rowName();
                        rowComponent("row", ValueType.VAR_STRING);
                        columns();
                        column("col", "c", ValueType.FIXED_LONG);
                        conflictHandler(ConflictHandler.RETRY_ON_WRITE_WRITE);
                        rangeScanAllowed();
                        negativeLookups();
                        ignoreHotspottingChecks();
                        explicitCompressionBlockSizeKB(64);
                    }
                }.toTableMetadata().persistToBytes());
        return worldName;
    }
}
