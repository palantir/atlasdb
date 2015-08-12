package com.palantir.atlasdb.keyvalue.rdbms.utils;

public final class MetaTable {
    public static final String META_TABLE_NAME = "_table_meta";
    public static final class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String METADATA = "metadata";
    }
    private MetaTable() {}
}