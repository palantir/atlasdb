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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleTableInitializer;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class OracleDbTableFactory implements DbTableFactory {
    private final Cache<String, TableType> tableSizeByTableName = CacheBuilder.newBuilder().build();
    private final OracleDdlConfig config;

    public OracleDbTableFactory(OracleDdlConfig config) {
        this.config = config;
    }

    @Override
    public DbMetadataTable createMetadata(TableReference tableRef, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableRef, conns, config);
    }

    @Override
    public DbDdlTable createDdl(TableReference tableName, ConnectionSupplier conns) {
        return new OracleDdlTable(tableName, conns, config);
    }

    @Override
    public DbTableInitializer createInitializer(ConnectionSupplier conns) {
        return new OracleTableInitializer(conns, config);
    }

    @Override
    public DbReadTable createRead(TableReference tableRef, ConnectionSupplier conns) {
        TableType tableType = getTableType(conns, tableRef);
        return tableType.getReadTable(tableRef, conns, config);
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        TableType tableType = getTableType(conns, tableRef);
        return tableType.getWriteTable(tableRef, conns, config);
    }

    private TableType getTableType(final ConnectionSupplier conns, final TableReference tableRef) {
        try {
            return tableSizeByTableName.get(tableRef.getQualifiedName(), new Callable<TableType>() {
                @Override
                public TableType call() {
                    AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                            String.format("SELECT table_size FROM %s WHERE table_name = ?", config.metadataTableName()),
                            tableRef.getQualifiedName());
                    Preconditions.checkArgument(
                            !results.rows().isEmpty(),
                            "table %s not found",
                            tableRef.getQualifiedName());
                    return TableType.byId(results.get(0).getInteger("table_size"));
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
