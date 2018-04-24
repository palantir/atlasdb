/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.todo;

import java.io.File;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;

public class TodoSchema implements AtlasSchema {
    private static final Schema INDEX_TEST_SCHEMA = generateSchema();
    public static final String TODO_TABLE = "todo";
    public static final String TEXT_COLUMN = "text";

    private static Schema generateSchema() {
        Schema schema = new Schema(
                TodoSchema.class.getSimpleName(),
                TodoSchema.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE,
                OptionalType.JAVA8);

        schema.addTableDefinition(TODO_TABLE, new TableDefinition() {{
                rowName();
                rowComponent("id", ValueType.FIXED_LONG);
                columns();
                column(TEXT_COLUMN, "t", ValueType.STRING);
            }
        });

        return schema;
    }

    public static Schema getSchema() {
        return INDEX_TEST_SCHEMA;
    }

    public static TableReference todoTable() {
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, TODO_TABLE);
    }

    public static void main(String[] args) throws Exception {
        INDEX_TEST_SCHEMA.renderTables(new File("src/test/java"));
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
