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
package com.palantir.atlasdb.schema;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.StoredWriteReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.io.File;
import java.util.function.Supplier;

public enum TargetedSweepSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("sweep");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(TargetedSweepSchema::generateSchema);

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = newSchemaObject();
        addSweepStateTables(schema);
        addTableIdentifierTables(schema);

        schema.validate();
        return schema;
    }

    /**
     * This is visible for the ETE tests, which must not truncate the table identifier tables or risk cache
     * inconsistencies. Note that it's always safe to just leave mappings around without issues; it's a
     * dictionary and we only ever access it in O(1) ways.
     */
    public static Schema schemaWithoutTableIdentifierTables() {
        Schema schema = newSchemaObject();
        addSweepStateTables(schema);
        return schema;
    }

    private static Schema newSchemaObject() {
        return new Schema(
                "TargetedSweep",
                TargetedSweepSchema.class.getPackage().getName() + ".generated",
                NAMESPACE,
                OptionalType.JAVA8);
    }

    private static void addSweepStateTables(Schema schema) {
        // Stores actual cells to be swept
        schema.addTableDefinition("sweepableCells", new TableDefinition() {
            {
                javaTableName("SweepableCells");
                allSafeForLoggingByDefault();
                rowName();
                hashFirstNRowComponents(2);
                rowComponent("timestamp_partition", ValueType.VAR_LONG);
                rowComponent("metadata", ValueType.BLOB);
                dynamicColumns();
                columnComponent("timestamp_modulus", ValueType.VAR_LONG);
                columnComponent("write_index", ValueType.VAR_SIGNED_LONG);
                value(StoredWriteReference.class);

                // we do our own cleanup
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        });

        schema.addTableDefinition("sweepableTimestamps", new TableDefinition() {
            {
                javaTableName("SweepableTimestamps");
                allSafeForLoggingByDefault();
                rowName();
                hashFirstRowComponent();
                rowComponent("shard", ValueType.VAR_LONG);
                rowComponent("timestamp_partition", ValueType.VAR_LONG);
                rowComponent("sweep_conservative", ValueType.BLOB);
                dynamicColumns();
                columnComponent("timestamp_modulus", ValueType.VAR_LONG);
                value(ValueType.BLOB);

                // we do our own cleanup
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        });

        schema.addTableDefinition("sweepProgressPerShard", new TableDefinition() {
            {
                javaTableName("SweepShardProgress");
                allSafeForLoggingByDefault();
                rowName();
                hashFirstRowComponent();
                rowComponent("shard", ValueType.VAR_SIGNED_LONG);
                rowComponent("sweep_conservative", ValueType.BLOB);
                columns();
                column("value", "v", ValueType.VAR_LONG);

                // we do our own cleanup
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        });
    }

    private static void addTableIdentifierTables(Schema schema) {
        schema.addTableDefinition("sweepNameToId", new TableDefinition() {
            {
                allSafeForLoggingByDefault();
                rowName();
                hashFirstRowComponent();
                rowComponent("table", ValueType.STRING);
                columns();
                column("id", "i", SweepTableIdentifier.class);

                // append-only + all writes are CAS
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        });

        schema.addTableDefinition("sweepIdToName", new TableDefinition() {
            {
                allSafeForLoggingByDefault();
                rowName();
                hashFirstRowComponent();
                rowComponent("singleton", ValueType.STRING);
                dynamicColumns();
                // descending lets us select the next table id in O(1) time
                columnComponent("tableId", ValueType.VAR_LONG, ValueByteOrder.DESCENDING);
                value(ValueType.STRING);

                // append-only + all writes are CAS
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        });

        schema.addTableDefinition("tableClears", new TableDefinition() {
            {
                allSafeForLoggingByDefault();
                rowName();
                rowComponent("table", ValueType.STRING);

                columns();
                column("lastClearedTimestamp", "l", ValueType.VAR_LONG);

                // all writes are CAS
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        });
    }

    @Override
    public Namespace getNamespace() {
        return NAMESPACE;
    }

    @Override
    public Schema getLatestSchema() {
        return SCHEMA.get();
    }

    public static void main(String[] _args) throws Exception {
        SCHEMA.get().renderTables(new File("src/main/java"));
    }
}
