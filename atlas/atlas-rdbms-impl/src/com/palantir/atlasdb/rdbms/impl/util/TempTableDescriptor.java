package com.palantir.atlasdb.rdbms.impl.util;

import java.util.Map;

public class TempTableDescriptor {

    private final String tableName;
    private final Map<String, String> columns;

    public TempTableDescriptor(String tableName, Map<String, String> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "TempTableDescriptor [tableName=" + tableName + ", columns="
                + columns + "]";
    }
}
