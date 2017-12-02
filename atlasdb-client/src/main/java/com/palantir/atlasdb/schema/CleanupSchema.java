/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.schema;

import java.io.File;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.queue.SequentialTables;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;

public enum CleanupSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("cleanup");
    private static final Schema SCHEMA = generateSchema();

    private static Schema generateSchema() {
        Schema schema = new Schema("Cleanup",
                CleanupSchema.class.getPackage().getName() + ".generated",
                NAMESPACE,
                OptionalType.JAVA8);

        SequentialTables.addSequentialTableDefinitions(schema, "cleanup");

        schema.validate();
        return schema;
    }

    @Override
    public Namespace getNamespace() {
        return NAMESPACE;
    }

    @Override
    public Schema getLatestSchema() {
        return SCHEMA;
    }

    public static void main(String[] args) throws Exception {
        SCHEMA.renderTables(new File("src/main/java"));
    }
}
