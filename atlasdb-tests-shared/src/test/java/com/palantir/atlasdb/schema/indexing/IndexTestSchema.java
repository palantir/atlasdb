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
package com.palantir.atlasdb.schema.indexing;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.IndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import java.io.File;

public class IndexTestSchema implements AtlasSchema {
    public static final AtlasSchema INSTANCE = new IndexTestSchema();

    private static final Schema INDEX_TEST_SCHEMA = generateSchema();

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"}) // Table/IndexDefinition syntax
    private static Schema generateSchema() {
        Schema schema = new Schema("IndexTest",
                IndexTest.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE,
                OptionalType.JAVA8);

        schema.addTableDefinition("data", new TableDefinition() {{
            rowName();
                rowComponent("id", ValueType.FIXED_LONG);
            columns();
                column("value", "v", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("index1", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromColumn("value", ValueType.FIXED_LONG, "value", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);
            rangeScanAllowed();
        }});

        schema.addIndexDefinition("index2", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromColumn("value", ValueType.FIXED_LONG, "value", "_value");
                componentFromRow("id", ValueType.FIXED_LONG);
            rangeScanAllowed();
        }});

        schema.addIndexDefinition("index3", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromIterableColumn("value",
                        ValueType.FIXED_LONG,
                        ValueByteOrder.ASCENDING,
                        "value",
                        "ImmutableList.of(_value)");
            rangeScanAllowed();
        }});

        schema.addIndexDefinition("index4", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromIterableColumn("value1",
                        ValueType.FIXED_LONG,
                        ValueByteOrder.ASCENDING,
                        "value",
                        "ImmutableList.of(_value)");
                componentFromIterableColumn("value2",
                        ValueType.FIXED_LONG,
                        ValueByteOrder.ASCENDING,
                        "value",
                        "ImmutableList.of(_value)");
            rangeScanAllowed();
        }});

        schema.addTableDefinition("two_columns", new TableDefinition() {{
            rowName();
                rowComponent("id", ValueType.FIXED_LONG);
            columns();
                column("foo", "f", ValueType.FIXED_LONG);
                column("bar", "b", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("foo_to_id", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("two_columns");
            rowName();
                hashFirstRowComponent();
                componentFromColumn("foo", ValueType.FIXED_LONG, "foo", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("foo_to_id_cond", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("two_columns");
            onCondition("foo", "_value > 1");
            rowName();
                componentFromColumn("foo", ValueType.FIXED_LONG, "foo", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);

        }});

        return schema;
    }

    public static Schema getSchema() {
        return INDEX_TEST_SCHEMA;
    }

    public static void main(String[]  args) throws Exception {
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
}
