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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class TableRemappingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("namespace");

    private final AtomicLong timestamp = new AtomicLong();
    private final KeyValueService rawKvs = new InMemoryKeyValueService(true);
    private final KvTableMappingService tableMapper = KvTableMappingService.create(rawKvs, timestamp::incrementAndGet);
    private final KeyValueService kvs = TableRemappingKeyValueService.create(rawKvs, tableMapper);

    @Test
    public void canConcurrentlyDropAndCreateDisjointSetsOfTables() {
        tableMapper.readTableMap();
        ExecutorService executor = Executors.newFixedThreadPool(1_000);
        for (int i = 0; i < 10_000; i++) {
            String firstWorld = generateRandomTablesPrefixed();
            String secondWorld = generateRandomTablesPrefixed();
            final Set<Future<Void>> futures = new HashSet<>();
            futures.add(executor.submit(() -> {
                String worldPrefix = NAMESPACE.getName() + "." + firstWorld + "__";
                Set<TableReference> tablesToDrop = Sets.newHashSet();
                for (TableReference tableRef : kvs.getAllTableNames()) {
                    if (tableRef.getQualifiedName().startsWith(worldPrefix)) {
                        tablesToDrop.add(tableRef);
                    }
                }
                kvs.dropTables(tablesToDrop);
                return null;
            }));
            futures.add(executor.submit(() -> {
                String worldPrefix = NAMESPACE.getName() + "." + secondWorld + "__";
                Set<TableReference> tablesToDrop = Sets.newHashSet();
                for (TableReference tableRef : kvs.getAllTableNames()) {
                    if (tableRef.getQualifiedName().startsWith(worldPrefix)) {
                        tablesToDrop.add(tableRef);
                    }
                }
                kvs.dropTables(tablesToDrop);
                return null;
            }));
            futures.forEach(Futures::getUnchecked);
        }
    }
    private String generateRandomTablesPrefixed() {
        String worldName = UUID.randomUUID().toString().replace("-", "_");
        String tableName = UUID.randomUUID().toString().replace("-", "_");
        kvs.createTable(TableReference.create(NAMESPACE, worldName + "__" + tableName), new TableDefinition() {{
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
        }}.toTableMetadata().persistToBytes());
        return worldName;
    }
}
