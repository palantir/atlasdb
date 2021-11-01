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
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.io.File;
import java.util.function.Supplier;

public enum SweepSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("sweep");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(SweepSchema::generateSchema);

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = new Schema(
                "Sweep", SweepSchema.class.getPackage().getName() + ".generated", NAMESPACE, OptionalType.JAVA8);

        // This table tracks stats about tables that are relevant
        // in determining when and in which order they should be swept.
        schema.addTableDefinition("priority", new TableDefinition() {
            {
                javaTableName("SweepPriority");
                allSafeForLoggingByDefault();
                rowName();
                rowComponent("full_table_name", ValueType.STRING);
                columns();
                // The (approximate) number of writes to this table
                // since the last time it was swept.
                column("write_count", "w", ValueType.VAR_LONG);
                // The (wall clock) time of when this table was
                // last swept.
                column("last_sweep_time", "t", ValueType.VAR_LONG);
                // The minimum swept timestamp, used to determine min
                // timestamp for transaction table sweep
                column("minimum_swept_timestamp", "m", ValueType.VAR_SIGNED_LONG);
                // The number of cells deleted when this table was
                // last swept.
                column("cells_deleted", "d", ValueType.VAR_LONG);
                // The number of cells in the table when this table
                // was last swept.
                column("cells_examined", "e", ValueType.VAR_LONG);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                rangeScanAllowed();
            }
        });

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

    public static void main(String[] _args) throws Exception {
        SCHEMA.get().renderTables(new File("src/main/java"));
    }
}
