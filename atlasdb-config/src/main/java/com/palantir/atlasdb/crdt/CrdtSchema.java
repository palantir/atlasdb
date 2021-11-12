/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.crdt;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

import java.io.File;
import java.util.function.Supplier;

public enum CrdtSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("crdt");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(CrdtSchema::generateSchema);

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = newSchemaObject();
        schema.addTableDefinition("crdt", new TableDefinition() {
            {
                javaTableName("Crdt");
                allSafeForLoggingByDefault();
                rowName();
                hashFirstRowComponent();
                rowComponent("series", ValueType.STRING);
                dynamicColumns();
                columnComponent("partition", ValueType.VAR_LONG);
                value(ValueType.BLOB);

                sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH);
                // TODO (jkong): can look at ways of doing this more efficiently
                conflictHandler(ConflictHandler.SERIALIZABLE_CELL);
            }
        });

        schema.validate();
        return schema;
    }

    private static Schema newSchemaObject() {
        return new Schema(
                "Crdt",
                CrdtSchema.class.getPackage().getName() + ".generated",
                NAMESPACE,
                OptionalType.JAVA8);
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
