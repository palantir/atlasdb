/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleTableInitializer;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleWriteTable;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.nexus.db.DBType;
import java.util.concurrent.ExecutorService;

public class OracleDbTableFactory implements DbTableFactory {
    private final OracleDdlConfig config;
    private final OracleTableNameGetter oracleTableNameGetter;
    private final OraclePrefixedTableNames oraclePrefixedTableNames;
    private final TableValueStyleCache valueStyleCache;
    private final ExecutorService compactionTimeoutExecutor;

    public OracleDbTableFactory(OracleDdlConfig config,
            OracleTableNameGetter oracleTableNameGetter,
            OraclePrefixedTableNames oraclePrefixedTableNames,
            TableValueStyleCache valueStyleCache,
            ExecutorService executorService) {
        this.config = config;
        this.oracleTableNameGetter = oracleTableNameGetter;
        this.oraclePrefixedTableNames = oraclePrefixedTableNames;
        this.valueStyleCache = valueStyleCache;
        this.compactionTimeoutExecutor = executorService;
    }

    @Override
    public DbMetadataTable createMetadata(TableReference tableRef, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableRef, conns, config);
    }

    @Override
    public DbDdlTable createDdl(TableReference tableRef, ConnectionSupplier conns) {
        return OracleDdlTable.create(tableRef, conns, config, oracleTableNameGetter, valueStyleCache,
                compactionTimeoutExecutor);
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
        DbQueryFactory queryFactory = new OracleQueryFactory(
                config, shortTableName, tableValueStyle == TableValueStyle.OVERFLOW);
        return new DbReadTable(connectionSupplier, queryFactory);
    }

    private String getTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        try {
            return oracleTableNameGetter.getInternalShortTableName(connectionSupplier, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        TableValueStyle tableValueStyle = valueStyleCache.getTableType(conns, tableRef, config.metadataTable());
        switch (tableValueStyle) {
            case OVERFLOW:
                return OracleOverflowWriteTable.create(
                        config, conns, oracleTableNameGetter, oraclePrefixedTableNames, tableRef);
            case RAW:
                return new OracleWriteTable(config, conns, oraclePrefixedTableNames, tableRef);
            default:
                throw new EnumConstantNotPresentException(TableValueStyle.class, tableValueStyle.name());
        }
    }

    @Override
    public DBType getDbType() {
        return DBType.ORACLE;
    }

    @Override
    public PrefixedTableNames getPrefixedTableNames() {
        return oraclePrefixedTableNames;
    }

    @Override
    public void close() {
        // do nothing
    }
}
