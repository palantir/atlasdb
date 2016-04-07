package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class SimpleDbMetadataTable implements DbMetadataTable {
    protected final String tableName;
    protected final ConnectionSupplier conns;

    public SimpleDbMetadataTable(String tableName,
                                 ConnectionSupplier conns) {
        this.tableName = tableName;
        this.conns = conns;
    }

    @Override
    public boolean exists() {
        return conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM pt_metropolis_table_meta WHERE table_name = ?",
                tableName);
    }

    @Override
    @SuppressWarnings("deprecation")
    public byte[] getMetadata() {
        AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                "SELECT value FROM pt_metropolis_table_meta WHERE table_name = ?",
                tableName);
        if (results.size() < 1) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        } else {
            return MoreObjects.firstNonNull(
                    Iterables.getOnlyElement(results.rows()).getBytes("value"),
                    PtBytes.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void putMetadata(byte[] metadata) {
        Preconditions.checkArgument(exists(), "Table %s does not exist.", tableName);
        conns.get().updateUnregisteredQuery(
                "UPDATE pt_metropolis_table_meta SET value = ? WHERE table_name = ?",
                metadata,
                tableName);
    }
}
