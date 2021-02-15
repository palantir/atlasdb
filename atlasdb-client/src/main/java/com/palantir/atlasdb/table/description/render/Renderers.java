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

import com.palantir.atlasdb.table.description.IndexMetadata;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.squareup.javapoet.MethodSpec;
import java.util.List;
import java.util.stream.Collectors;

public final class Renderers {
    private Renderers() {
        // cannot instantiate
    }

    public static String CamelCase(String string) {
        return camelCase(string, true);
    }

    public static String camelCase(String string) {
        return camelCase(string, false);
    }

    private static String camelCase(String string, boolean lastWasUnderscore) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            if (ch != '_') {
                if (lastWasUnderscore) {
                    sb.append(Character.toUpperCase(ch));
                } else {
                    sb.append(ch);
                }
            }
            lastWasUnderscore = ch == '_';
        }
        return sb.toString();
    }

    static String lower_case(String string) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            if (Character.isUpperCase(ch)) {
                sb.append('_');
            }
            sb.append(Character.toLowerCase(ch));
        }
        return sb.toString();
    }

    static String UPPER_CASE(String string) {
        return lower_case(string).toUpperCase();
    }

    static String getClassTableName(String rawTableName, TableDefinition table) {
        if (table.getGenericTableName() != null) {
            return table.getGenericTableName();
        } else if (table.getJavaTableName() != null) {
            return table.getJavaTableName();
        } else {
            return Renderers.CamelCase(rawTableName);
        }
    }

    static String getIndexTableName(IndexMetadata index) {
        if (index.getJavaIndexName() == null) {
            return Renderers.CamelCase(index.getIndexName());
        } else {
            return index.getJavaIndexName();
        }
    }

    public static Class<?> getColumnClassForGenericTypeParameter(NamedColumnDescription col) {
        return col.getValue().getJavaObjectTypeClass();
    }

    public static Class<?> getColumnClass(NamedColumnDescription col) {
        return col.getValue().getJavaTypeClass();
    }

    public static MethodSpec.Builder addParametersFromRowComponents(
            MethodSpec.Builder methodFactory, TableMetadata tableMetadata) {
        for (NameComponentDescription rowPart : getRowComponents(tableMetadata)) {
            methodFactory.addParameter(rowPart.getType().getJavaClass(), rowPart.getComponentName());
        }
        return methodFactory;
    }

    public static String getArgumentsFromRowComponents(TableMetadata tableMetadata) {
        List<String> args = getRowComponents(tableMetadata).stream()
                .map(NameComponentDescription::getComponentName)
                .collect(Collectors.toList());

        return String.join(", ", args);
    }

    public static List<NameComponentDescription> getRowComponents(TableMetadata tableMetadata) {
        NameMetadataDescription rowMetadata = tableMetadata.getRowMetadata();
        List<NameComponentDescription> rowParts = rowMetadata.getRowParts();
        return rowMetadata.numberOfComponentsHashed() == 0 ? rowParts : rowParts.subList(1, rowParts.size());
    }
}
