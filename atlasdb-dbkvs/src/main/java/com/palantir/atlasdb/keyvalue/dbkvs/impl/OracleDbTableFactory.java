package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.palantir.atlasdb.AtlasSystemPropertyManager;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleRawQueryFactory;
import com.palantir.common.base.Throwables;
import com.palantir.db.oracle.OracleShim;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class OracleDbTableFactory implements DbTableFactory {
    private final Cache<String, TableSize> tableSizeByTableName = CacheBuilder.newBuilder().build();
    private final Supplier<Long> overflowIds;
    private final OverflowMigrationState migrationState;

    public OracleDbTableFactory(Supplier<Long> overflowIds,
                                OverflowMigrationState migrationState) {
        this.overflowIds = overflowIds;
        this.migrationState = migrationState;
    }

    @Override
    public DbMetadataTable createMetadata(String tableName, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableName, conns);
    }

    @Override
    public DbDdlTable createDdl(String tableName, ConnectionSupplier conns, AtlasSystemPropertyManager systemProperties) {
        return new OracleDdlTable(tableName, conns, migrationState, systemProperties);
    }

    @Override
    public DbReadTable createRead(String tableName, ConnectionSupplier conns, OracleShim oracleShim) {
        TableSize tableSize = getTableSize(conns, tableName);
        DbQueryFactory queryFactory;
        switch (tableSize) {
        case OVERFLOW:
            queryFactory = new OracleOverflowQueryFactory(tableName, migrationState, oracleShim);
            break;
        case RAW:
            queryFactory = new OracleRawQueryFactory(tableName, oracleShim);
            break;
        default:
            throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
        return new UnbatchedDbReadTable(conns, queryFactory);
    }

    @Override
    public DbWriteTable createWrite(String tableName, ConnectionSupplier conns) {
        TableSize tableSize = getTableSize(conns, tableName);
        switch (tableSize) {
        case OVERFLOW:
            return new OracleOverflowWriteTable(tableName, conns, overflowIds, migrationState);
        case RAW:
            return new SimpleDbWriteTable(tableName, conns);
        default:
            throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
    }

    private TableSize getTableSize(final ConnectionSupplier conns, final String tableName) {
        try {
            return tableSizeByTableName.get(tableName, new Callable<TableSize>() {
                @Override
                public TableSize call() {
                    AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                            "SELECT table_size FROM pt_metropolis_table_meta WHERE table_name = ?",
                            tableName);
                    Preconditions.checkArgument(
                            !results.rows().isEmpty(),
                            "table %s not found",
                            tableName);
                    return TableSize.byId(results.get(0).getInteger("table_size"));
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
