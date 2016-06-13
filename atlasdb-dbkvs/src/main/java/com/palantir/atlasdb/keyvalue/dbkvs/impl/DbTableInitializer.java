package com.palantir.atlasdb.keyvalue.dbkvs.impl;

public interface DbTableInitializer {
    void createUtilityTables();
    void createMetadataTable(String metadataTableName);
}
