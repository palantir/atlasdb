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
package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.AtlasSchema;
import java.io.File;

public class GenericTestSchema implements AtlasSchema {
    public static final AtlasSchema INSTANCE = new GenericTestSchema();

    private static final Schema GENERIC_TEST_SCHEMA = generateSchema();

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = new Schema(
                "GenericTestSchema",
                GenericTestSchema.class.getPackage().getName() + ".generated",
                Namespace.create("test"),
                OptionalType.JAVA8);

        // use for testing rangeScanAllowed code
        schema.addTableDefinition("rangeScanTest", new TableDefinition() {
            {
                javaTableName("RangeScanTest");

                rowName();
                rowComponent("component1", ValueType.STRING);

                columns();
                column("column1", "c", ValueType.VAR_LONG);

                rangeScanAllowed();
            }
        });

        schema.addTableDefinition("genericRangeScanTest", new TableDefinition() {
            {
                javaTableName("GenericRangeScanTest");

                rowName();
                rowComponent("component1", ValueType.SHA256HASH);

                columns();
                dynamicColumns();
                columnComponent("component2", ValueType.STRING);
                value(ValueType.STRING);

                rangeScanAllowed();
            }
        });

        return schema;
    }

    public static Schema getSchema() {
        return GENERIC_TEST_SCHEMA;
    }

    public static void main(String[] _args) throws Exception {
        GENERIC_TEST_SCHEMA.renderTables(new File("src/test/java"));
    }

    @Override
    public Schema getLatestSchema() {
        return GENERIC_TEST_SCHEMA;
    }

    @Override
    public Namespace getNamespace() {
        return Namespace.DEFAULT_NAMESPACE;
    }
}
