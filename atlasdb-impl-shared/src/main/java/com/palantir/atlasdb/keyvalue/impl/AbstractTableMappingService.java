/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.Validate;

import com.google.common.collect.BiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schemas;

public abstract class AbstractTableMappingService implements TableMappingService {

    protected final AtomicReference<BiMap<TableReference, TableReference>> tableMap = new AtomicReference<>();

    protected abstract BiMap<TableReference, TableReference> readTableMap();

    protected void updateTableMap() {
        while (true) {
            BiMap<TableReference, TableReference> oldMap = tableMap.get();
            BiMap<TableReference, TableReference> newMap = readTableMap();
            if (tableMap.compareAndSet(oldMap, newMap)) {
                return;
            }
        }
    }

    @Override
    public TableReference getMappedTableName(TableReference tableRef) throws TableMappingNotFoundException {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef;
        }
        if (tableMap.get().containsKey(tableRef)) {
            TableReference shortName = tableMap.get().get(tableRef);
            validateShortName(tableRef, shortName);
            return tableMap.get().get(tableRef);
        } else {
            updateTableMap();
            TableReference shortTableName = tableMap.get().get(tableRef);
            if (shortTableName != null) {
                return shortTableName;
            }
            throw new TableMappingNotFoundException("Unable to resolve full name for table reference " + tableRef);
        }
    }

    protected void validateShortName(TableReference tableRef, TableReference shortName) {
        Validate.isTrue(Schemas.isTableNameValid(shortName.getQualifiedName()),
                "Table mapper has an invalid table name for table reference %s: %s", tableRef, shortName);
    }

    private TableReference getFullTableName(TableReference shortTableName) {
        if (tableMap.get().containsValue(shortTableName)) {
            return tableMap.get().inverse().get(shortTableName);
        } else {
            updateTableMap();
            Validate.isTrue(tableMap.get().containsValue(shortTableName),
                    "Unable to resolve full name for table %s", shortTableName);
            return tableMap.get().inverse().get(shortTableName);
        }
    }

    @Override
    public <T> Map<TableReference, T> mapToShortTableNames(Map<TableReference, T> toMap)
            throws TableMappingNotFoundException {
        Map<TableReference, T> newMap = Maps.newHashMap();
        for (Entry<TableReference, T> e : toMap.entrySet()) {
            newMap.put(getMappedTableName(e.getKey()), e.getValue());
        }
        return newMap;
    }

    private final ConcurrentHashMap<TableReference, Boolean> unmappedTables = new ConcurrentHashMap<>();

    @Override
    public Map<TableReference, TableReference> generateMapToFullTableNames(Set<TableReference> tableRefs) {
        Map<TableReference, TableReference> shortNameToFullTableName = Maps.newHashMapWithExpectedSize(
                tableRefs.size());
        Set<TableReference> tablesToReload = Sets.newHashSet();
        for (TableReference inputName : tableRefs) {
            if (inputName.isFullyQualifiedName()) {
                shortNameToFullTableName.put(inputName, inputName);
            } else if (tableMap.get().containsValue(inputName)) {
                shortNameToFullTableName.put(inputName, getFullTableName(inputName));
            } else if (unmappedTables.containsKey(inputName)) {
                shortNameToFullTableName.put(inputName, inputName);
            } else {
                tablesToReload.add(inputName);
            }
        }
        if (!tablesToReload.isEmpty()) {
            updateTableMap();
            for (TableReference tableRef : Sets.difference(tablesToReload, tableMap.get().values())) {
                unmappedTables.put(tableRef, true);
                shortNameToFullTableName.put(tableRef, tableRef);
            }
            for (TableReference tableRef : Sets.intersection(tablesToReload, tableMap.get().values())) {
                shortNameToFullTableName.put(tableRef, getFullTableName(tableRef));
            }
        }
        return shortNameToFullTableName;
    }

}
