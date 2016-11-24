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
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleRawQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleTableInitializer;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.nexus.db.DBType;

public class OracleDbTableFactory implements DbTableFactory {
    private final OracleDdlConfig config;

    public OracleDbTableFactory(OracleDdlConfig config) {
        this.config = config;
    }

    @Override
    public DbKvsGetRanges createGetRanges(DbKvs dbKvs, TableReference tableRef, ConnectionSupplier conns) {
        return new DbKvsGetRanges(dbKvs, tableRef, conns, config, getDbType());
    }

    @Override
    public DbMetadataTable createMetadata(TableReference tableRef, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableRef, conns, config);
    }

    @Override
    public DbDdlTable createDdl(TableReference tableRef, ConnectionSupplier conns) {
        return OracleDdlTable.create(tableRef, conns, config);
    }

    @Override
    public DbTableInitializer createInitializer(ConnectionSupplier conns) {
        return new OracleTableInitializer(conns, config);
    }

    @Override
    public DbReadTable createRead(TableReference tableRef, ConnectionSupplier conns) {
        OracleTableNameGetter oracleTableNameGetter = new OracleTableNameGetter(config, conns, tableRef);
        TableSize tableSize = TableSizeCache.getTableSize(conns, tableRef, config.metadataTable());
        DbQueryFactory queryFactory;
        String shortTableName = getTableName(oracleTableNameGetter);
        switch (tableSize) {
            case OVERFLOW:
                String shortOverflowTableName = getOverflowTableName(oracleTableNameGetter);
                queryFactory = new OracleOverflowQueryFactory(config, shortTableName, shortOverflowTableName);
                break;
            case RAW:
                queryFactory = new OracleRawQueryFactory(shortTableName, config);
                break;
            default:
                throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
        }
        return new UnbatchedDbReadTable(conns, queryFactory);
    }

    private String getTableName(OracleTableNameGetter oracleTableNameGetter) {
        try {
            return oracleTableNameGetter.getInternalShortTableName();
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getOverflowTableName(OracleTableNameGetter oracleTableNameGetter) {
        try {
            return oracleTableNameGetter.getInternalShortOverflowTableName();
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        TableSize tableSize = TableSizeCache.getTableSize(conns, tableRef, config.metadataTable());
        switch (tableSize) {
            case OVERFLOW:
                return OracleOverflowWriteTable.create(config, conns, tableRef);
            case RAW:
                return new SimpleDbWriteTable(config, conns, tableRef);
            default:
                throw new EnumConstantNotPresentException(TableSize.class, tableSize.name());
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
