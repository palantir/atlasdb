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
package com.palantir.atlasdb.timelock.benchmarks.schema;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.io.File;
import java.io.IOException;

public final class BenchmarksSchema implements AtlasSchema {
    public static final Namespace NAMESPACE = Namespace.create("benchmarks");
    public static final Schema SCHEMA = generateSchema();

    public static final TableReference BLOBS_TABLE_REF = TableReference.create(NAMESPACE, "Blobs");

    @Override public Namespace getNamespace() {
        return NAMESPACE;
    }

    @Override public Schema getLatestSchema() {
        return generateSchema();
    }

    private static Schema generateSchema() {
        Schema schema = new Schema("Benchmarks", BenchmarksSchema.class.getPackage().getName() + ".generated",
                NAMESPACE, OptionalType.JAVA8);
        createTables(schema);
        schema.validate();
        return schema;
    }

    private static void createTables(Schema schema) {
        schema.addTableDefinition("KvRows", new TableDefinition() {
            {
                rowName();
                hashFirstRowComponent();
                rowComponent("bucket", ValueType.VAR_STRING);
                rowComponent("key", ValueType.FIXED_LONG);

                columns();
                column("data", "d", ValueType.BLOB);

                rangeScanAllowed();
            }
        });

        schema.addTableDefinition("KvDynamicColumns", new TableDefinition() {
            {

                rowName();
                hashFirstRowComponent();
                rowComponent("bucket", ValueType.VAR_STRING);

                dynamicColumns();
                columnComponent("key", ValueType.FIXED_LONG);
                value(ValueType.BLOB);

                rangeScanAllowed();
            }
        });

        schema.addTableDefinition("Blobs", new TableDefinition() {
            {
                rowName();
                rowComponent("key", ValueType.BLOB);

                columns();
                column("data", "d", ValueType.BLOB);
            }
        });

        schema.addTableDefinition("BlobsSerializable", new TableDefinition() {
            {
                conflictHandler(ConflictHandler.SERIALIZABLE);

                rowName();
                rowComponent("key", ValueType.BLOB);

                columns();
                column("data", "d", ValueType.BLOB);
            }
        });

        schema.addTableDefinition("Metadata", new TableDefinition() {
            {

                rowName();
                hashFirstRowComponent();
                rowComponent("key", ValueType.VAR_STRING);

                columns();
                column("data", "d", ValueType.BLOB);

                rangeScanAllowed();
            }
        });
    }

    public static void main(String[] args) throws IOException {
        SCHEMA.renderTables(new File("src/main/java"));
    }
}
