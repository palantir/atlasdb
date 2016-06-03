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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleRawQueryFactory;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class OracleDbTableFactory implements DbTableFactory {
    private final Cache<String, TableSize> tableSizeByTableName = CacheBuilder.newBuilder().build();
    private final OracleKeyValueServiceConfig config;

    public OracleDbTableFactory(OracleKeyValueServiceConfig config) {
        this.config = config;
    }

    @Override
    public DbMetadataTable createMetadata(String tableName, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableName, conns, config);
    }

    @Override
    public DbDdlTable createDdl(String tableName, ConnectionSupplier conns) {
        return new OracleDdlTable(tableName, conns, config);
    }

    @Override
    public DbReadTable createRead(String tableName, ConnectionSupplier conns) {
        TableSize tableSize = getTableSize(conns, tableName);
        DbQueryFactory queryFactory;
        switch (tableSize) {
        case OVERFLOW:
            queryFactory = new OracleOverflowQueryFactory(tableName, config);
            break;
        case RAW:
            queryFactory = new OracleRawQueryFactory(tableName, config);
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
            return new OracleOverflowWriteTable(tableName, conns, config);
        case RAW:
            return new SimpleDbWriteTable(tableName, conns, config);
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
                            "SELECT table_size FROM " + AtlasDbConstants.METADATA_TABLE.getQualifiedName() + " WHERE table_name = ?",
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
    public DBType getDbType() {
        return DBType.ORACLE;
    }

    @Override
    public void close() {
        // do nothing
    }
}
