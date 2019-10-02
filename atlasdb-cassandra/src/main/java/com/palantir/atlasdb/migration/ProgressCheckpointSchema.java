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

import java.io.File;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public enum ProgressCheckpointSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("migration");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(ProgressCheckpointSchema::generateSchema);

    private static Schema generateSchema() {
        Schema schema = new Schema("MigrationProgress",
                KvsProgressCheckPointImpl.class.getPackage().getName() + ".generated",
                NAMESPACE,
                OptionalType.JAVA8);

        // Stores actual cells to be swept
        schema.addTableDefinition("progress", new TableDefinition() {{
            rowName();
            rowComponent("row", ValueType.BLOB);
            columns();
            column("progress", "p", ValueType.BLOB);
            column("isDone", "d", ValueType.VAR_LONG);
            conflictHandler(ConflictHandler.SERIALIZABLE_CELL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH);
        }});

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
