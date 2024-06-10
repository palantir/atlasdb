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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TableRemappingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("namespace");
    private static final String TABLENAME = "test";
    private static final TableReference DATA_TABLE_REF = TableReference.create(NAMESPACE, TABLENAME);

    private static final int NUM_THREADS = 100;
    private static final int NUM_ITERATIONS = 1_000;

    private final AtomicLong timestamp = new AtomicLong();
    private final KeyValueService rawKvs = new InMemoryKeyValueService(true);
    private final KvTableMappingService tableMapper = KvTableMappingService.create(rawKvs, timestamp::incrementAndGet);
    ;
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

    @ParameterizedTest
    @ValueSource(booleans = {false}) // KvTableMappingService issues prevent cached lookups from working as intended
    public void testAddTableInOneNodeAfterDropInAnother(boolean cacheTableLookups) {
        // Simulate a pair of KVS clients hitting the same raw KVS
        KvTableMappingService tableMapper1 = KvTableMappingService.createWithCustomNamespaceValidation(
                rawKvs,
                timestamp::incrementAndGet,
                Namespace.STRICTLY_CHECKED_NAME,
                new TableMappingCacheConfiguration() {
                    @Override
                    public Predicate<TableReference> cacheableTablePredicate() {
                        return tableReference -> cacheTableLookups;
                    }
                });
        KvTableMappingService tableMapper2 = KvTableMappingService.createWithCustomNamespaceValidation(
                rawKvs,
                timestamp::incrementAndGet,
                Namespace.STRICTLY_CHECKED_NAME,
                new TableMappingCacheConfiguration() {
                    @Override
                    public Predicate<TableReference> cacheableTablePredicate() {
                        return tableReference -> cacheTableLookups;
                    }
                });
        TableRemappingKeyValueService kvs1 = TableRemappingKeyValueService.create(rawKvs, tableMapper1);
        TableRemappingKeyValueService kvs2 = TableRemappingKeyValueService.create(rawKvs, tableMapper2);
        // Create the table in the first node
        kvs1.createTable(DATA_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));
        // Try to get a value, creating the mapping in memory
        kvs1.get(DATA_TABLE_REF, ImmutableMap.of(cell, 1L));
        // Drop the table in node 2. This removes the mapping from the db, but the in memory mapping in node 1 isn't
        // removed
        kvs2.dropTable(DATA_TABLE_REF);
        // Create the table again in node 1. This will re-write the table entry in the db.
        kvs1.createTable(DATA_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        // Write some data to kvs2 and try to read it back from kvs1. This verifies that the table caches across 1 & 2
        // are consistent.
        kvs1.put(DATA_TABLE_REF, ImmutableMap.of(cell, new byte[] {0x42}), 1L);
        // Try and get a value on both nodes and make sure they are the same.
        Map<Cell, Value> result = kvs1.get(DATA_TABLE_REF, ImmutableMap.of(cell, 2L));
        Map<Cell, Value> result2 = kvs2.get(DATA_TABLE_REF, ImmutableMap.of(cell, 2L));
        assertThat(result).isNotEmpty();
        assertThat(result2).isNotEmpty();
        assertThat(result).isEqualTo(result2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false}) // KvTableMappingService issues prevent cached lookups from working as intended
    public void testAddTableInOneNodeAfterDropInAnother2(boolean cacheTableLookups) {
        // Simulate a pair of KVS clients hitting the same raw KVS
        KvTableMappingService tableMapper1 = KvTableMappingService.createWithCustomNamespaceValidation(
                rawKvs,
                timestamp::incrementAndGet,
                Namespace.STRICTLY_CHECKED_NAME,
                new TableMappingCacheConfiguration() {
                    @Override
                    public Predicate<TableReference> cacheableTablePredicate() {
                        return tableReference -> cacheTableLookups;
                    }
                });
        KvTableMappingService tableMapper2 = KvTableMappingService.createWithCustomNamespaceValidation(
                rawKvs,
                timestamp::incrementAndGet,
                Namespace.STRICTLY_CHECKED_NAME,
                new TableMappingCacheConfiguration() {
                    @Override
                    public Predicate<TableReference> cacheableTablePredicate() {
                        return tableReference -> cacheTableLookups;
                    }
                });
        TableRemappingKeyValueService kvs1 = TableRemappingKeyValueService.create(rawKvs, tableMapper1);
        TableRemappingKeyValueService kvs2 = TableRemappingKeyValueService.create(rawKvs, tableMapper2);
        // Create the table in the first node
        kvs1.createTable(DATA_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));
        // Try to get a value, creating the mapping in memory
        kvs1.get(DATA_TABLE_REF, ImmutableMap.of(cell, 1L));
        // Drop the table in node 2. This removes the mapping from the db, but the in memory mapping in node 1 isn't
        // removed
        kvs2.dropTable(DATA_TABLE_REF);
        // Create the table again in node 2. This will write a new mapping to the db, which doesn't match the mapping
        // that is still in memory on node 1.
        kvs2.createTable(DATA_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        // Write some data to kvs2 and try to read it back from kvs1. This verifies that the table caches across 1 & 2
        // are consistent.
        kvs2.put(DATA_TABLE_REF, ImmutableMap.of(cell, new byte[] {0x42}), 1L);
        // Try and get a value on both nodes and make sure they are the same.
        Map<Cell, Value> result = kvs1.get(DATA_TABLE_REF, ImmutableMap.of(cell, 2L));
        Map<Cell, Value> result2 = kvs2.get(DATA_TABLE_REF, ImmutableMap.of(cell, 2L));
        assertThat(result).isNotEmpty();
        assertThat(result2).isNotEmpty();
        assertThat(result).isEqualTo(result2);
    }

    @Test
    @Disabled("Issues with both table creation and metadata storage after table creation exist to cause this test to"
            + " fail.")
    public void fuzzTestConcurrentlyCreateAndDeleteTablesAcrossManyWriters() {
        int createAndDeleteWriters = 32;
        int totalAttempts = 10;
        int tableCount = 1000;

        ExecutorService executor = Executors.newFixedThreadPool(createAndDeleteWriters);
        for (int i = 0; i < totalAttempts; i++) {
            // Try and ensure the creates and deletes happen as close to simultaneously as possible
            CyclicBarrier waitUntilAllFuturesAreReady = new CyclicBarrier(createAndDeleteWriters);
            final Set<Future<Void>> futures = new HashSet<>();
            for (int j = 0; j < createAndDeleteWriters; j++) {
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
                    for (int k = 0; k < tableCount; k++) {
                        kvs.dropTable(TableReference.create(NAMESPACE, TABLENAME + "_" + k));
                        kvs.createTable(
                                TableReference.create(NAMESPACE, TABLENAME + "_" + k),
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
