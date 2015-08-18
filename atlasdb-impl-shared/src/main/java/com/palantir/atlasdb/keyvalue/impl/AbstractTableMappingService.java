/**
 * Copyright 2015 Palantir Technologies
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

import org.apache.commons.lang.Validate;

import com.google.common.collect.BiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.schema.TableReference;
import com.palantir.atlasdb.table.description.Schemas;

public abstract class AbstractTableMappingService implements TableMappingService {

    private static final String PERIOD = ".";

    protected final AtomicReference<BiMap<TableReference, String>> tableMap = new AtomicReference<BiMap<TableReference, String>>();

    protected abstract BiMap<TableReference, String> readTableMap();

    protected void updateTableMap() {
        while(true) {
            BiMap<TableReference, String> oldMap = tableMap.get();
            BiMap<TableReference, String> newMap = readTableMap();
            if (tableMap.compareAndSet(oldMap, newMap)) {
                return;
            }
        }
    }

    @Override
    public String getShortTableName(TableReference tableRef) {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef.getTablename();
        }
        if (tableMap.get().containsKey(tableRef)) {
            String shortName = tableMap.get().get(tableRef);
            validateShortName(tableRef, shortName);
            return tableMap.get().get(tableRef);
        } else {
            updateTableMap();
            Validate.isTrue(tableMap.get().containsKey(tableRef), "Unable to resolve full name for table reference " + tableRef);
            return tableMap.get().get(tableRef);
        }
    }

    protected void validateShortName(TableReference tableRef, String shortName) {
        Validate.isTrue(Schemas.validateTableName(shortName), "Table mapper has an invalid table name for table reference " + tableRef + ": " + shortName);
    }

    private TableReference getFullTableName(String shortTableName) {
        if (tableMap.get().containsValue(shortTableName)) {
            return tableMap.get().inverse().get(shortTableName);
        } else {
            updateTableMap();
            Validate.isTrue(tableMap.get().containsValue(shortTableName), "Unable to resolve full name for table " + shortTableName);
            return tableMap.get().inverse().get(shortTableName);
        }
    }

    @Override
    public <T> Map<String, T> mapToShortTableNames(Map<TableReference, T> toMap) {
        Map<String, T> newMap = Maps.newHashMap();
        for (Entry<TableReference, T> e : toMap.entrySet()) {
            newMap.put(getShortTableName(e.getKey()), e.getValue());
        }
        return newMap;
    }

    private final ConcurrentHashMap<String, Boolean> unmappedTables = new ConcurrentHashMap<String, Boolean>();

    @Override
    public Set<TableReference> mapToFullTableNames(Set<String> tableNames) {
        Set<TableReference> newSet = Sets.newHashSet();
        Set<String> tablesToReload = Sets.newHashSet();
        for (String name : tableNames) {
            if (name.contains(PERIOD)) {
                newSet.add(createTableReferenceFromNamespacedName(name));
            } else if (tableMap.get().containsValue(name)) {
                newSet.add(getFullTableName(name));
            } else if (unmappedTables.containsKey(name)) {
                newSet.add(TableReference.createWithEmptyNamespace(name));
            } else {
                tablesToReload.add(name);
            }
        }
        if (!tablesToReload.isEmpty()) {
            updateTableMap();
            for (String tableName : Sets.difference(tablesToReload, tableMap.get().values())) {
                unmappedTables.put(tableName, true);
                newSet.add(TableReference.createWithEmptyNamespace(tableName));
            }
            for (String tableName : Sets.intersection(tablesToReload, tableMap.get().values())) {
                newSet.add(getFullTableName(tableName));
            }
        }
        return newSet;
    }

    private static TableReference createTableReferenceFromNamespacedName(String name) {
        return TableReference.createFromFullyQualifiedName(name);
    }

}
