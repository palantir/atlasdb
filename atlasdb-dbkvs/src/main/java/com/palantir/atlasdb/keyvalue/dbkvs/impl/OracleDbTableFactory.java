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

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleRawQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleTableInitializer;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class OracleDbTableFactory implements DbTableFactory {
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
        TableSize tableSize = TableSizeCache.getTableSize(conns, tableRef, config.metadataTable());
        DbQueryFactory queryFactory;
        String shortTableName = getInternalTableName(conns, prefixedTableName(tableRef));
        switch (tableSize) {
            case OVERFLOW:
                String overflowTableName = getInternalTableName(conns, prefixedOverflowTableName(tableRef));
                queryFactory = new OracleOverflowQueryFactory(shortTableName, config, overflowTableName);
                break;
            case RAW:
                queryFactory = new OracleRawQueryFactory(shortTableName, config);
                break;
            default:
                throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
        return new UnbatchedDbReadTable(conns, queryFactory);
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        TableSize tableSize = TableSizeCache.getTableSize(conns, tableRef, config.metadataTable());
        switch (tableSize) {
            case OVERFLOW:
                return OracleOverflowWriteTable.create(tableRef, conns, config);
            case RAW:
                return new SimpleDbWriteTable(tableRef, conns, config);
            default:
                throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
    }

    private String getInternalTableName(final ConnectionSupplier conns, String tableName) {
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery(
                String.format(
                        "SELECT short_table_name FROM %s WHERE table_name = ?",
                        AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE),
                tableName);
        return result.get(0).getString("short_table_name");
    }

    private String prefixedTableName(TableReference tableRef) {
        return config.tablePrefix() + DbKvs.internalTableName(tableRef);
    }

    private String prefixedOverflowTableName(TableReference tableRef) {
        return config.overflowTablePrefix() + DbKvs.internalTableName(tableRef);
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
