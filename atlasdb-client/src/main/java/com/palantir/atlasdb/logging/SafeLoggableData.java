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
package com.palantir.atlasdb.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;

@Value.Immutable
public abstract class SafeLoggableData implements KeyValueServiceLogArbitrator {
    public abstract Set<TableReference> permittedTableReferences();

    @Value.Lazy
    public Set<String> permittedInternalTableReferences() {
        return permittedTableReferences().stream()
                .map(AbstractKeyValueService::internalTableName)
                .collect(Collectors.toSet());
    }

    public abstract Map<TableReference, Set<String>> permittedRowComponents();

    public abstract Map<TableReference, Set<String>> permittedColumnNames();

    public abstract Set<TableReference> prohibitedTableReferences();

    @Value.Lazy
    public Set<String> prohibitedInternalTableReferences() {
        return prohibitedTableReferences().stream()
                .map(AbstractKeyValueService::internalTableName)
                .collect(Collectors.toSet());
    }

    public abstract Map<TableReference, Set<String>> prohibitedRowComponents();

    public abstract Map<TableReference, Set<String>> prohibitedColumnNames();

    @Override
    public boolean isTableReferenceSafe(TableReference tableReference) {
        return permittedTableReferences().contains(tableReference)
                && !prohibitedTableReferences().contains(tableReference);
    }

    @Override
    public boolean isInternalTableReferenceSafe(String internalTableReference) {
        return permittedInternalTableReferences().contains(internalTableReference)
                && !prohibitedInternalTableReferences().contains(internalTableReference);
    }

    @Override
    public boolean isRowComponentNameSafe(TableReference tableReference, String rowComponentName) {
        return hasMatchingComponent(tableReference, rowComponentName, permittedRowComponents())
                && !hasMatchingComponent(tableReference, rowComponentName, prohibitedRowComponents());
    }

    @Override
    public boolean isColumnNameSafe(TableReference tableReference, String columnName) {
        return hasMatchingComponent(tableReference, columnName, permittedColumnNames())
                && !hasMatchingComponent(tableReference, columnName, prohibitedColumnNames());
    }

    @Override
    public SafeLoggableData combine(KeyValueServiceLogArbitrator other) {
        if (!(other instanceof SafeLoggableData)) {
            throw new UnsupportedOperationException("Not mergeable with arbitrators of other types. Found "
                    + other.getClass());
        }
        SafeLoggableData otherData = (SafeLoggableData) other;

        // Cannot use from(this) because merging is tricky
        return ImmutableSafeLoggableData.builder()
                .addAllPermittedTableReferences(permittedTableReferences())
                .addAllPermittedTableReferences(otherData.permittedTableReferences())
                .addAllProhibitedTableReferences(prohibitedTableReferences())
                .addAllProhibitedTableReferences(otherData.prohibitedTableReferences())
                .permittedRowComponents(mergeMaps(permittedRowComponents(), otherData.permittedRowComponents()))
                .prohibitedRowComponents(mergeMaps(prohibitedRowComponents(), otherData.prohibitedRowComponents()))
                .permittedColumnNames(mergeMaps(permittedColumnNames(), otherData.permittedColumnNames()))
                .prohibitedColumnNames(mergeMaps(prohibitedColumnNames(), otherData.prohibitedColumnNames()))
                .build();
    }

    static KeyValueServiceLogArbitrator noData() {
        return ImmutableSafeLoggableData.builder().build();
    }

    private static Map<TableReference, Set<String>> mergeMaps(
            Map<TableReference, Set<String>> first,
            Map<TableReference, Set<String>> second) {
        Map<TableReference, Set<String>> result = new HashMap<>(first);
        second.forEach((key, value) -> result.merge(key, value, Sets::union));
        return result;
    }

    private static boolean hasMatchingComponent(TableReference tableReference, String rowComponentName,
            Map<TableReference, Set<String>> tableReferenceSetMap) {
        return tableReferenceSetMap.containsKey(tableReference)
                && tableReferenceSetMap.get(tableReference).contains(rowComponentName);
    }
}
