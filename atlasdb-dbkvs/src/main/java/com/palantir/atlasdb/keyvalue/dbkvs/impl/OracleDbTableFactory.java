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

import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.TableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleRawQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleTableInitializer;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleWriteTable;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.nexus.db.DBType;

public class OracleDbTableFactory implements DbTableFactory {
    private final OracleDdlConfig config;
    private TableNameGetter tableNameGetter;
    private TableValueStyleCache valueStyleCache;

    public OracleDbTableFactory(OracleDdlConfig config) {
        this.config = config;
        tableNameGetter = new OracleTableNameGetter(config);
        valueStyleCache = new TableValueStyleCache();
    }

    @Override
    public DbMetadataTable createMetadata(TableReference tableRef, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableRef, conns, config);
    }

    @Override
    public DbDdlTable createDdl(TableReference tableRef, ConnectionSupplier conns) {
        return OracleDdlTable.create(tableRef, conns, config, tableNameGetter, valueStyleCache);
    }

    @Override
    public DbTableInitializer createInitializer(ConnectionSupplier conns) {
        return new OracleTableInitializer(conns, config);
    }

    @Override
    public DbReadTable createRead(TableReference tableRef, ConnectionSupplier connectionSupplier) {
        TableValueStyle tableValueStyle =
                valueStyleCache.getTableType(connectionSupplier, tableRef, config.metadataTable());
        String shortTableName = getTableName(connectionSupplier, tableRef);
        DbQueryFactory queryFactory;
        switch (tableValueStyle) {
            case OVERFLOW:
                String shortOverflowTableName = getOverflowTableName(connectionSupplier, tableRef);
                queryFactory = new OracleOverflowQueryFactory(config, shortTableName, shortOverflowTableName);
                break;
            case RAW:
                queryFactory = new OracleRawQueryFactory(shortTableName, config);
                break;
            default:
                throw new EnumConstantNotPresentException(TableValueStyle.class, tableValueStyle.name());
        }
        return new UnbatchedDbReadTable(connectionSupplier, queryFactory);
    }

    private String getTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        try {
            return tableNameGetter.getInternalShortTableName(connectionSupplier, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        try {
            return tableNameGetter.getInternalShortOverflowTableName(connectionSupplier, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        TableValueStyle tableValueStyle = valueStyleCache.getTableType(conns, tableRef, config.metadataTable());
        switch (tableValueStyle) {
            case OVERFLOW:
                return OracleOverflowWriteTable.create(config, conns, tableNameGetter, tableRef);
            case RAW:
                return new OracleWriteTable(config, conns, tableNameGetter, tableRef);
            default:
                throw new EnumConstantNotPresentException(TableValueStyle.class, tableValueStyle.name());
        }
    }

    @Override
    public DBType getDbType() {
        return DBType.ORACLE;
    }

    @Override
    public TableNameGetter getTableNameGetter() {
        return tableNameGetter;
    }

    @Override
    public void close() {
        // do nothing
    }
}
