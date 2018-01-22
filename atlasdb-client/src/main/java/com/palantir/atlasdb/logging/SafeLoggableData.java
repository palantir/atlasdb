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

package com.palantir.atlasdb.logging;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

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

    public static SafeLoggableData fromTableMetadata(Map<TableReference, byte[]> tableRefToMetadata) {
        return SafeLoggableDataUtils.fromTableMetadata(tableRefToMetadata);
    }

    @Override
    public boolean isTableReferenceSafe(TableReference tableReference) {
        return permittedTableReferences().contains(tableReference);
    }

    @Override
    public boolean isInternalTableReferenceSafe(String internalTableReference) {
        return permittedInternalTableReferences().contains(internalTableReference);
    }

    @Override
    public boolean isRowComponentNameSafe(TableReference tableReference, String rowComponentName) {
        return permittedRowComponents().containsKey(tableReference)
                && permittedRowComponents().get(tableReference).contains(rowComponentName);
    }

    @Override
    public boolean isColumnNameSafe(TableReference tableReference, String columnName) {
        return permittedColumnNames().containsKey(tableReference)
                && permittedColumnNames().get(tableReference).contains(columnName);
    }
}
