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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.TableReference;

public class StaticTableMappingService implements TableMappingService {

    public static TableMappingService create() {
        StaticTableMappingService ret = new StaticTableMappingService();
        return ret;
    }

    private StaticTableMappingService() {
        //
    }

    @Override
    public Set<TableReference> mapToFullTableNames(Set<String> tableNames) {
        Set<TableReference> newMap = Sets.newHashSet();
        for (String table : tableNames) {
            newMap.add(getTableReference(table));
        }
        return newMap;
    }

    @Override
    public String addTable(TableReference tableRef) {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef.getTablename();
        }
        return tableRef.getNamespace().getName() + "." + tableRef.getTablename();
    }

    @Override
    public void removeTable(TableReference tableRef) {
        // We don't need to do any work here because we don't store a mapping.
    }

    @Override
    public String getShortTableName(TableReference tableRef) {
        return addTable(tableRef);
    }

    @Override
    public <T> Map<String, T> mapToShortTableNames(Map<TableReference, T> toMap) {
        Map<String, T> newMap = Maps.newHashMap();
        for (Entry<TableReference, T> e : toMap.entrySet()) {
            newMap.put(getShortTableName(e.getKey()), e.getValue());
        }
        return newMap;
    }

    private static final String PERIOD = ".";

    @Override
    public TableReference getTableReference(String tableName) {
        if (tableName.contains(PERIOD)) {
            return TableReference.createFromFullyQualifiedName(tableName);
        }
        if (AtlasDbConstants.hiddenTables.contains(tableName)) {
            return TableReference.createWithEmptyNamespace(tableName);
        }
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, tableName);
    }
}
