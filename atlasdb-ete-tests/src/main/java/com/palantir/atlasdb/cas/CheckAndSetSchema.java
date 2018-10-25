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
package com.palantir.atlasdb.cas;

import java.io.File;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;

public class CheckAndSetSchema implements AtlasSchema {
    private static final Schema GENERATED_SCHEMA = generateSchema();
    public static final String CAS_TABLE = "check_and_set";
    public static final String VALUES_COLUMN = "value";

    private static Schema generateSchema() {
        Schema schema = new Schema(
                CheckAndSetSchema.class.getSimpleName(),
                CheckAndSetSchema.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE,
                OptionalType.JAVA8);

        schema.addTableDefinition(CAS_TABLE, new TableDefinition() {{
                allSafeForLoggingByDefault();
                rowName();
                rowComponent("id", ValueType.FIXED_LONG);
                columns();
                column(VALUES_COLUMN, "v", ValueType.FIXED_LONG);
            }
        });

        return schema;
    }

    public static Schema getSchema() {
        return GENERATED_SCHEMA;
    }

    public static void main(String[] args) throws Exception {
        GENERATED_SCHEMA.renderTables(new File("src/main/java"));
    }

    @Override
    public Schema getLatestSchema() {
        return GENERATED_SCHEMA;
    }

    @Override
    public Namespace getNamespace() {
        return Namespace.DEFAULT_NAMESPACE;
    }
}
