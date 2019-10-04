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
package com.palantir.atlasdb.table.description.render;

import static com.palantir.atlasdb.AtlasDbConstants.SCHEMA_V2_TABLE_NAME;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.logsafe.Preconditions;

public class TableRendererV2 {
    private final String packageName;
    private final Namespace namespace;

    public TableRendererV2(String packageName, Namespace namespace) {
        this.packageName = Preconditions.checkNotNull(packageName);
        this.namespace = Preconditions.checkNotNull(namespace);
    }

    public String getClassName(String rawTableName, TableDefinition table) {
        return Renderers.getClassTableName(rawTableName, table) + SCHEMA_V2_TABLE_NAME;
    }

    public String render(String rawTableName, TableDefinition table) {
        return new TableClassRendererV2(packageName, namespace, rawTableName, table).render();
    }
}
