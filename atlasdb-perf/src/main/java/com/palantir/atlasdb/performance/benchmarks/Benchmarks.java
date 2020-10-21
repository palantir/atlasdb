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
package com.palantir.atlasdb.performance.benchmarks;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinition;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

/**
 * Static utilities class for common performance test procedures.
 */
public final class Benchmarks {

    private Benchmarks() {
        // uninstantiable
    }

    /**
     * Creates the table and returns a reference to it.
     * @param kvs the key-value service where the table is being created.
     * @param tableRef the table being created.
     * @param rowComponent the name of the row being created.
     * @param columnName the name of the column being created.
     */
    public static void createTable(
            KeyValueService kvs, TableReference tableRef, String rowComponent, String columnName) {
        createTable(kvs, tableRef, rowComponent, columnName, TableMetadataPersistence.SweepStrategy.NOTHING);
    }

    public static void createTable(
            KeyValueService kvs,
            TableReference tableRef,
            String rowComponent,
            String columnName,
            TableMetadataPersistence.SweepStrategy sweepStrategy) {
        TableDefinition tableDef = new TableDefinition() {
            {
                rowName();
                rowComponent(rowComponent, ValueType.STRING);
                columns();
                column(columnName, columnName, ValueType.BLOB);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                sweepStrategy(sweepStrategy);
            }
        };
        kvs.createTable(tableRef, tableDef.toTableMetadata().persistToBytes());
    }

    public static void createTableWithStreaming(
            KeyValueService kvs, TableReference tableRef, String rowComponent, String columnName) {
        createTable(kvs, tableRef, rowComponent, columnName);
        createStreamingTable(kvs, tableRef, columnName);
    }

    private static void createStreamingTable(KeyValueService kvs, TableReference parentTable, String columnName) {
        StreamStoreDefinition ssd = new StreamStoreDefinitionBuilder(columnName, "Value", ValueType.VAR_LONG)
                .inMemoryThreshold(1024 * 1024)
                .build();

        ssd.getTables().forEach((tableName, tableDefinition) -> {
            TableReference streamingTable = TableReference.create(parentTable.getNamespace(), tableName);
            kvs.createTable(streamingTable, tableDefinition.toTableMetadata().persistToBytes());
        });
    }

    public static TableReference createTableWithDynamicColumns(
            KeyValueService kvs, TableReference tableRef, String rowComponent, String columnComponent) {
        TableDefinition tableDef = new TableDefinition() {
            {
                rowName();
                rowComponent(rowComponent, ValueType.STRING);
                dynamicColumns();
                columnComponent(columnComponent, ValueType.FIXED_LONG);
                value(ValueType.FIXED_LONG);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            }
        };
        kvs.createTable(tableRef, tableDef.toTableMetadata().persistToBytes());
        return tableRef;
    }
}
