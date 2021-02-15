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
package com.palantir.atlasdb.todo;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import java.io.File;

public class TodoSchema implements AtlasSchema {
    private static final Schema INDEX_TEST_SCHEMA = generateSchema();
    private static final String TODO_TABLE = "todo";
    private static final String NAMESPACED_TODO_TABLE = "namespacedTodo";
    private static final String TEXT_COLUMN = "text";
    private static final String LATEST_STREAM_TABLE = "latest_snapshot";
    private static final String STREAM_TABLE = "snapshots";

    private static Schema generateSchema() {
        Schema schema = new Schema(
                TodoSchema.class.getSimpleName(),
                TodoSchema.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE,
                OptionalType.JAVA8);

        schema.addTableDefinition(TODO_TABLE, new TableDefinition() {
            {
                rowName();
                rowComponent("id", ValueType.FIXED_LONG);
                columns();
                column(TEXT_COLUMN, "t", ValueType.STRING);

                sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH);
            }
        });

        schema.addTableDefinition(NAMESPACED_TODO_TABLE, new TableDefinition() {
            {
                rowName();
                rowComponent("namespace", ValueType.STRING);
                dynamicColumns();
                columnComponent("todoId", ValueType.FIXED_LONG);
                value(ValueType.STRING);
                sweepStrategy(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
            }
        });

        schema.addTableDefinition(LATEST_STREAM_TABLE, new TableDefinition() {
            {
                rowName();
                rowComponent("key", ValueType.FIXED_LONG);
                columns();
                column("stream_id", "i", ValueType.FIXED_LONG);
            }
        });

        schema.addStreamStoreDefinition(
                new StreamStoreDefinitionBuilder(STREAM_TABLE, "Snapshots", ValueType.FIXED_LONG)
                        .inMemoryThreshold(AtlasDbConstants.DEFAULT_STREAM_IN_MEMORY_THRESHOLD)
                        .build());

        return schema;
    }

    public static Schema getSchema() {
        return INDEX_TEST_SCHEMA;
    }

    public static TableReference todoTable() {
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, TODO_TABLE);
    }

    public static TableReference namespacedTodoTable() {
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, NAMESPACED_TODO_TABLE);
    }

    public static void main(String[] args) throws Exception {
        INDEX_TEST_SCHEMA.renderTables(new File("src/main/java"));
    }

    @Override
    public Schema getLatestSchema() {
        return INDEX_TEST_SCHEMA;
    }

    @Override
    public Namespace getNamespace() {
        return Namespace.DEFAULT_NAMESPACE;
    }

    public static byte[] todoTextColumn() {
        return PtBytes.toBytes(TEXT_COLUMN);
    }
}
