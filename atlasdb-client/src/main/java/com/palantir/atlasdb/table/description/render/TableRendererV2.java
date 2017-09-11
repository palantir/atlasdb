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
package com.palantir.atlasdb.table.description.render;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.palantir.atlasdb.AtlasDbConstants.SCHEMA_V2_TABLE_NAME;

import java.util.List;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.squareup.javapoet.MethodSpec;

public class TableRendererV2 {
    private final String packageName;
    private final Namespace namespace;

    public TableRendererV2(String packageName, Namespace namespace) {
        this.packageName = checkNotNull(packageName);
        this.namespace = checkNotNull(namespace);
    }

    public String getClassName(String rawTableName, TableDefinition table) {
        return Renderers.getClassTableName(rawTableName, table) + SCHEMA_V2_TABLE_NAME;
    }

    public String render(String rawTableName, TableDefinition table) {
        return new ClassRendererV2(packageName, namespace, rawTableName, table).render();
    }

    public static Class<?> getColumnTypeClass(NamedColumnDescription col) {
        return col.getValue().getValueType().getTypeClass();
    }

    public static MethodSpec.Builder addParametersFromRowComponents(
            MethodSpec.Builder methodFactory,
            TableMetadata tableMetadata) {
        for (NameComponentDescription rowPart : getRowComponents(tableMetadata)) {
            methodFactory.addParameter(
                    rowPart.getType().getTypeClass(),
                    rowPart.getComponentName());
        }
        return methodFactory;
    }

    public static String getArgumentsFromRowComponents(TableMetadata tableMetadata) {
        List<String> args = getRowComponents(tableMetadata).stream()
                .map(NameComponentDescription:: getComponentName)
                .collect(Collectors.toList());

        return String.join(", ", args);
    }

    public static List<NameComponentDescription> getRowComponents(TableMetadata tableMetadata) {
        NameMetadataDescription rowMetadata = tableMetadata.getRowMetadata();
        return rowMetadata.getRowParts();
    }
}
