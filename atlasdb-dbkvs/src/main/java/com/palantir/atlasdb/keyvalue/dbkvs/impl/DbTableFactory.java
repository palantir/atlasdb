package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.io.Closeable;

import com.palantir.atlasdb.AtlasSystemPropertyManager;
import com.palantir.db.oracle.OracleShim;

public interface DbTableFactory extends Closeable {
    DbMetadataTable createMetadata(String tableName, ConnectionSupplier conns);
    DbDdlTable createDdl(String tableName, ConnectionSupplier conns, AtlasSystemPropertyManager systemProperties);
    DbReadTable createRead(String tableName, ConnectionSupplier conns, OracleShim oracleShim);
    DbWriteTable createWrite(String tableName, ConnectionSupplier conns);
    @Override
    void close();
}
