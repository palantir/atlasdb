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
package com.palantir.atlasdb.keyvalue.impl;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;

public final class DynamicColumnTable {
    private DynamicColumnTable() {
        // uninstantiable
    }

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"}) // Table/IndexDefinition syntax
    private static final TableDefinition DYNAMIC_COLUMNS_TABLE = new TableDefinition() {
        {
            rowName();
            rowComponent("id", ValueType.FIXED_LONG);
            dynamicColumns();
            columnComponent("column_id", ValueType.FIXED_LONG);
            value(ValueType.FIXED_LONG);
        }
    };

    public static TableReference reference() {
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, "dynamicColsTable");
    }

    public static byte[] metadata() {
        return DYNAMIC_COLUMNS_TABLE.toTableMetadata().persistToBytes();
    }
}
