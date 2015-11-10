package com.palantir.atlasdb.keyvalue.rocksdb.impl;

public interface RocksDbMXBean {
    String[] listTableNames();
    void forceCompaction();
    void forceCompaction(String tableName);
    String getProperty(String property);
    String getProperty(String tableName, String property);
}
