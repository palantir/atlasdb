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
package com.palantir.atlasdb.keyvalue;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Map;
import java.util.Set;

public interface TableMappingService {
    /**
     * Add a new table to the table mapping service. The table must be visible to all calls made after this
     * method returns.
     *
     * @param tableRef full table name of the table to be added
     * @return short table name that tableRef is mapped to
     */
    @Idempotent
    TableReference addTable(TableReference tableRef);

    /**
     * Removes a table form the table mapping service. The deletion must be visible to all calls made after this
     * method returns
     *
     * @param tableRef full table name of the table to be removed
     */
    @Idempotent
    void removeTable(TableReference tableRef);

    default void removeTables(Set<TableReference> tableRefs) {
        tableRefs.forEach(this::removeTable);
    }

    /**
     * Returns the short table name tableRef is mapped to.
     *
     * @param tableRef full table of the table
     * @throws TableMappingNotFoundException if no mapping is found
     */
    TableReference getMappedTableName(TableReference tableRef) throws TableMappingNotFoundException;

    /**
     * Returns a map where each key in tableMap ,i.e., full table name, is replaced by the short table name it is
     * mapped to in this table mapping service.
     *
     * It is not guaranteed that the result is based on a consistent snapshot of the table mapping service if any add
     * or remove methods are invoked during execution of this method.
     *
     * @param tableMap input map
     * @throws TableMappingNotFoundException if no mapping is found for any key in tableMap
     */
    <T> Map<TableReference, T> mapToShortTableNames(Map<TableReference, T> tableMap)
            throws TableMappingNotFoundException;

    /**
     * Given a set of short table names, returns a map of each short table name to its corresponding long table name.
     *
     * @param tableNames short table names to find the inverse mapping for
     */
    Map<TableReference, TableReference> generateMapToFullTableNames(Set<TableReference> tableNames);
}
