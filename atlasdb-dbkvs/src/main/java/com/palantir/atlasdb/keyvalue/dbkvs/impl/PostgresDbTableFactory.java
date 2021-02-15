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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresPrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresTableInitializer;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresWriteTable;
import com.palantir.nexus.db.DBType;

public class PostgresDbTableFactory implements DbTableFactory {

    private final PostgresDdlConfig config;
    private final PostgresPrefixedTableNames prefixedTableNames;

    public PostgresDbTableFactory(PostgresDdlConfig config, PostgresPrefixedTableNames prefixedTableNames) {
        this.config = config;
        this.prefixedTableNames = prefixedTableNames;
    }

    @Override
    public DbMetadataTable createMetadata(TableReference tableRef, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableRef, conns, config);
    }

    @Override
    public DbDdlTable createDdl(TableReference tableName, ConnectionSupplier conns) {
        return new PostgresDdlTable(tableName, conns, config);
    }

    @Override
    public DbTableInitializer createInitializer(ConnectionSupplier conns) {
        return new PostgresTableInitializer(conns);
    }

    @Override
    public DbReadTable createRead(TableReference tableRef, ConnectionSupplier conns) {
        return new DbReadTable(conns, new PostgresQueryFactory(DbKvs.internalTableName(tableRef), config));
    }

    @Override
    public DbWriteTable createWrite(TableReference tableRef, ConnectionSupplier conns) {
        return new PostgresWriteTable(config, conns, tableRef, prefixedTableNames);
    }

    @Override
    public DBType getDbType() {
        return DBType.POSTGRESQL;
    }

    @Override
    public PrefixedTableNames getPrefixedTableNames() {
        return prefixedTableNames;
    }

    @Override
    public void close() {}
}
