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

package com.palantir.atlasdb.sweep.queue.clear;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Set;

public interface TableClearer {
    void deleteAllRowsInTables(Set<TableReference> tables);

    default void deleteAllRowsInTables(TableReference... tables) {
        deleteAllRowsInTables(ImmutableSet.copyOf(tables));
    }

    void truncateTables(Set<TableReference> tables);

    default void truncateTables(TableReference... tables) {
        truncateTables(ImmutableSet.copyOf(tables));
    }

    void dropTables(Set<TableReference> tables);

    default void dropTables(TableReference... tables) {
        dropTables(ImmutableSet.copyOf(tables));
    }
}
