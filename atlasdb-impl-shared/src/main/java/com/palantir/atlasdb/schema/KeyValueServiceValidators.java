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
package com.palantir.atlasdb.schema;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Set;
import java.util.stream.Collectors;

public final class KeyValueServiceValidators {
    private KeyValueServiceValidators() {
        // utility
    }

    /**
     * Returns tables that need to be validated.
     * Generally speaking, this excludes tables that are modified outside of the transaction protocol
     * (e.g. timestamp, transaction), and tables which are not required to be equal in the to- and from- KVSes
     * (e.g. the sweep priority table).
     *
     * Clearly, the tables to be validated are a subset of that to be migrated.
     */
    public static Set<TableReference> getValidatableTableNames(
            KeyValueService kvs,
            Set<TableReference> unmigratableTables) {
        Set<TableReference> tableNames = KeyValueServiceMigratorUtils
                .getMigratableTableNames(kvs, unmigratableTables, null);
        return removeSweepTableReferences(tableNames);
    }

    private static Set<TableReference> removeSweepTableReferences(Set<TableReference> tableNames) {
        return tableNames.stream()
                .filter(tableReference -> !isSweepTableReference(tableReference))
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static boolean isSweepTableReference(TableReference tableReference) {
        return tableReference.getNamespace().equals(SweepSchema.INSTANCE.getNamespace());
    }
}
