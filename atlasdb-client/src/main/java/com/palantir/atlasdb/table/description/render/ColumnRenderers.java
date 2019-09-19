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

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.util.SortedSet;

public final class ColumnRenderers {
    private ColumnRenderers() {
        // cannot instantiate
    }

    static String varName(NamedColumnDescription col) {
        return Renderers.camelCase(col.getLongName());
    }

    @SuppressWarnings("checkstyle:MethodName")
    static String VarName(NamedColumnDescription col) {
        return Renderers.CamelCase(col.getLongName());
    }

    static String typeName(NamedColumnDescription col) {
        switch (col.getValue().getFormat()) {
            case PERSISTER:
            case PERSISTABLE:
            case PROTO:
                return col.getValue().getJavaObjectTypeName();
            case VALUE_TYPE:
                return col.getValue().getValueType().getJavaClassName();
            default:
                throw new UnsupportedOperationException("Unsupported value type: " + col.getValue().getFormat());
        }
    }

    @SuppressWarnings("checkstyle:MethodName")
    static String TypeName(NamedColumnDescription col) {
        return col.getValue().getJavaObjectTypeName();
    }

    static String long_name(NamedColumnDescription col) {
        return '"' + col.getLongName() + '"';
    }

    static String short_name(NamedColumnDescription col) {
        return '"' + col.getShortName() + '"';
    }

    static SortedSet<NamedColumnDescription> namedColumns(TableMetadata table) {
        return ImmutableSortedSet.copyOf(
                Ordering.natural().onResultOf(NamedColumnDescription::getLongName),
                table.getColumns().getNamedColumns());
    }
}
