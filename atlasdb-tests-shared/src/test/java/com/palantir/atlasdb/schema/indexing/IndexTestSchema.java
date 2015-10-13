/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.schema.indexing;

import java.io.File;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.description.CodeGeneratingIndexDefinition;
import com.palantir.atlasdb.table.description.CodeGeneratingIndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.CodeGeneratingSchema;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.CodeGeneratingTableDefinition;
import com.palantir.atlasdb.table.description.ValueType;

public class IndexTestSchema implements AtlasSchema {
    public static final AtlasSchema INSTANCE = new IndexTestSchema();

    private static final CodeGeneratingSchema INDEX_TEST_SCHEMA = generateSchema();

    private static CodeGeneratingSchema generateSchema() {
        CodeGeneratingSchema schema = new CodeGeneratingSchema("IndexTest",
                IndexTest.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE);

        schema.addTableDefinition("data", new CodeGeneratingTableDefinition() {{
            rowName();
                rowComponent("id", ValueType.FIXED_LONG);
            columns();
                column("value", "v", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("index1", new CodeGeneratingIndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromColumn("value", ValueType.FIXED_LONG, "value", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);
            rangeScanAllowed();
        }});

        schema.addIndexDefinition("index2", new CodeGeneratingIndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromColumn("value", ValueType.FIXED_LONG, "value", "_value");
                componentFromRow("id", ValueType.FIXED_LONG);
            rangeScanAllowed();
        }});

        schema.addIndexDefinition("index3", new CodeGeneratingIndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromIterableColumn("value", ValueType.FIXED_LONG, ValueByteOrder.ASCENDING, "value", "ImmutableList.of(_value)");
            rangeScanAllowed();
        }});

        schema.addIndexDefinition("index4", new CodeGeneratingIndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("data");
            rowName();
                componentFromIterableColumn("value1", ValueType.FIXED_LONG, ValueByteOrder.ASCENDING, "value", "ImmutableList.of(_value)");
                componentFromIterableColumn("value2", ValueType.FIXED_LONG, ValueByteOrder.ASCENDING, "value", "ImmutableList.of(_value)");
            rangeScanAllowed();
        }});

        schema.addTableDefinition("two_columns", new CodeGeneratingTableDefinition() {{
            rowName();
                rowComponent("id", ValueType.FIXED_LONG);
            columns();
                column("foo", "f", ValueType.FIXED_LONG);
                column("bar", "b", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("foo_to_id", new CodeGeneratingIndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("two_columns");
            rowName();
                componentFromColumn("foo", ValueType.FIXED_LONG, "foo", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("foo_to_id_cond", new CodeGeneratingIndexDefinition(IndexType.CELL_REFERENCING) {{
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
