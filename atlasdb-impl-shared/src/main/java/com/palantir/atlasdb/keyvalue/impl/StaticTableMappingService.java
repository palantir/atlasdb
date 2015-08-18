package com.palantir.atlasdb.keyvalue.impl;

import java.util.List;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.TableReference;

public class StaticTableMappingService extends AbstractTableMappingService {
    private final KeyValueService kv;

    public static TableMappingService create(KeyValueService kv) {
        StaticTableMappingService ret = new StaticTableMappingService(kv);
        ret.updateTableMap();
        return ret;
    }

    private StaticTableMappingService(KeyValueService kv) {
        this.kv = kv;
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
    protected BiMap<TableReference, String> readTableMap() {
        Set<String> tables = kv.getAllTableNames();
        BiMap<TableReference, String> ret = HashBiMap.create();
        for (String table : tables) {
            ret.put(getTableReference(table), table);
        }

        return ret;
    }

    @Override
    protected void validateShortName(TableReference tableRef, String shortName) {
        // any name is ok for the static mapper
    }

    public static TableReference getTableReference(String tableName) {
        if (tableName.contains(".")) {
            List<String> split = Splitter.on('.').limit(2).splitToList(tableName);
            return TableReference.create(Namespace.create(split.get(0)), split.get(1));
        }
        if (AtlasDbConstants.hiddenTables.contains(tableName)) {
            return TableReference.createWithEmptyNamespace(tableName);
        }
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, tableName);
    }

}
