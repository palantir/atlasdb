/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import java.util.function.Supplier;

public enum TransactionsFourSchema implements AtlasSchema {
    INSTANCE;

    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(TransactionsFourSchema::generateSchema);
    private static final Namespace NAMESPACE = Namespace.create("TransactionsFour");

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private static Schema generateSchema() {
        Schema schema = new Schema(
                "TransactionsFour",
                TransactionsFourSchema.class.getPackage().getName() + ".generated",
                NAMESPACE,
                OptionalType.JAVA8);

        // Committed Timestamps is an ATOMIC table, and so is not included here.

        schema.validate();
        return schema;
    }

    @Override
    public Namespace getNamespace() {
        return NAMESPACE;
    }

    @Override
    public Schema getLatestSchema() {
        return SCHEMA.get();
    }
}
