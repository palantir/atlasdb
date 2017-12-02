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

package com.palantir.atlasdb.schema.cleanup;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.queue.PersistentSequentialSchemas;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;

public enum CleanupSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace CLEANUP_NAMESPACE = Namespace.create("cleanup");
    private static final Supplier<Schema> SCHEMA_SUPPLIER = Suppliers.memoize(CleanupSchema::createSchema);

    @Override
    public Namespace getNamespace() {
        return CLEANUP_NAMESPACE;
    }

    @Override
    public Schema getLatestSchema() {
        return SCHEMA_SUPPLIER.get();
    }

    private static Schema createSchema() {
        Schema schema = new Schema("Cleanup",
                CleanupSchema.class.getPackage().getName(),
                CLEANUP_NAMESPACE,
                OptionalType.JAVA8);

        PersistentSequentialSchemas.addSequentialTableDefinitions(schema, "cleanup");

        schema.validate();
        return schema;
    }

    public static void main(String[] args) throws IOException {
        SCHEMA_SUPPLIER.get().renderTables(new File("src/main/java"));
    }
}
