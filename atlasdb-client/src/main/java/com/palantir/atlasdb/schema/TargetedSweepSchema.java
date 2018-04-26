/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.schema;

import java.io.File;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReferenceAndCell;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public enum TargetedSweepSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("sweep");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(() -> generateSchema());

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = new Schema("TargetedSweep",
                TargetedSweepSchema.class.getPackage().getName() + ".generated",
                NAMESPACE,
                OptionalType.JAVA8);

        // Stores actual cells to be swept
        schema.addTableDefinition("sweepableCells", new TableDefinition() {{
            javaTableName("SweepableCells");
            allSafeForLoggingByDefault();
            rowName();
                hashFirstRowComponent();
                rowComponent("timestamp_partition", ValueType.VAR_LONG);
                rowComponent("metadata", ValueType.BLOB);
            dynamicColumns();
                columnComponent("timestamp_modulus", ValueType.VAR_LONG);
                columnComponent("write_index", ValueType.VAR_SIGNED_LONG);
                value(TableReferenceAndCell.class);

            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }});

        schema.addTableDefinition("sweepableTimestamps", new TableDefinition() {{
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

            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }});

        schema.addTableDefinition("sweepProgressPerShard", new TableDefinition() {{
            javaTableName("SweepShardProgress");
            allSafeForLoggingByDefault();
            rowName();
                hashFirstRowComponent();
                rowComponent("shard", ValueType.VAR_SIGNED_LONG);
                rowComponent("sweep_conservative", ValueType.BLOB);
            columns();
                column("value", "v", ValueType.VAR_LONG);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }});

        schema.validate();
        return schema;
    }

    @Override
    public Namespace getNamespace() {
        return NAMESPACE;
    }

    @Override
    public Schema getLatestSchema() {
        return SCHEMA.get();
    }

    public static void main(String[] args) throws Exception {
        SCHEMA.get().renderTables(new File("src/main/java"));
    }
}
