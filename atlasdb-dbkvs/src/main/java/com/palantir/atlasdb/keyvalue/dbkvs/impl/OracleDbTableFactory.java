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
import com.palantir.atlasdb.keyvalue.dbkvs.AbstractTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleRawQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleTableInitializer;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class OracleDbTableFactory extends AbstractTableFactory {
    private final Cache<TableReference, TableSize> tableSizeByTableRef = CacheBuilder.newBuilder().build();
    private final OracleDdlConfig config;

    public OracleDbTableFactory(OracleDdlConfig config) {
        super(config);
        this.config = config;
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
        TableSize tableSize = getTableSize(conns, tableRef);
        DbQueryFactory queryFactory;
        switch (tableSize) {
            case OVERFLOW:
                queryFactory = new OracleOverflowQueryFactory(DbKvs.internalTableName(tableRef), config);
                break;
            case RAW:
                queryFactory = new OracleRawQueryFactory(DbKvs.internalTableName(tableRef), config);
                break;
            default:
                throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
        return new BatchedDbReadTable(conns, queryFactory, exec, config);
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        TableSize tableSize = getTableSize(conns, tableRef);
        switch (tableSize) {
            case OVERFLOW:
                return OracleOverflowWriteTable.create(DbKvs.internalTableName(tableRef), conns, config);
            case RAW:
                return new SimpleDbWriteTable(DbKvs.internalTableName(tableRef), conns, config);
            default:
                throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
    }

    private TableSize getTableSize(final ConnectionSupplier conns, final TableReference tableRef) {
        try {
            return tableSizeByTableRef.get(tableRef, new Callable<TableSize>() {
                @Override
                public TableSize call() {
                    AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                            String.format(
                                    "SELECT table_size FROM %s WHERE table_name = ?",
                                    config.metadataTable().getQualifiedName()),
                            tableRef.getQualifiedName());
                    Preconditions.checkArgument(
                            !results.rows().isEmpty(),
                            "table %s not found",
                            tableRef.getQualifiedName());
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
}
