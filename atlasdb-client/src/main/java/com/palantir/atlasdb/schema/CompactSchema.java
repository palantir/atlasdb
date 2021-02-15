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

public enum CompactSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("compact");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(CompactSchema::generateSchema);

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = new Schema(
                "Compact", CompactSchema.class.getPackage().getName() + ".generated", NAMESPACE, OptionalType.JAVA8);

        // This table tracks stats about tables that are relevant
        // in determining when and in which order they should be compacted.
        schema.addTableDefinition("metadata", new TableDefinition() {
            {
                javaTableName("CompactMetadata");
                allSafeForLoggingByDefault();

                rowName();
                rowComponent("full_table_name", ValueType.STRING);

                columns();
                // The (wall clock) time of when this table was
                // last compacted.
                column("last_compact_time", "t", ValueType.VAR_LONG);

                conflictHandler(ConflictHandler.IGNORE_ALL);
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

    public static void main(String[] args) throws Exception {
        SCHEMA.get().renderTables(new File("src/main/java"));
    }
}
