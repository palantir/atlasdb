package com.palantir.atlasdb.keyvalue.dbkvs.impl;

public interface DbDdlTable {
    void create(byte[] tableMetadta);
    void drop();
    void truncate();
    void checkDatabaseVersion();
    void compactInternally();
}
