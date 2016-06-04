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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.keyvalue.dbkvs.PostgresKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresQueryFactory;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.nexus.db.DBType;

public class PostgresDbTableFactory implements DbTableFactory {

    private final PostgresKeyValueServiceConfig config;
    private final ExecutorService exec;

    public PostgresDbTableFactory(PostgresKeyValueServiceConfig config) {
        this.config = config;
        int poolSize = config.shared().poolSize();
        this.exec = newFixedThreadPool(poolSize);
    }

    private static ThreadPoolExecutor newFixedThreadPool(int maxPoolSize) {
        ThreadPoolExecutor pool = PTExecutors.newThreadPoolExecutor(maxPoolSize, maxPoolSize,
                15L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("Atlas postgres reader", true /* daemon */));

        pool.allowCoreThreadTimeOut(false);
        return pool;
    }

    @Override
    public DbMetadataTable createMetadata(String tableName, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableName, conns, config);
    }

    @Override
    public DbDdlTable createDdl(String tableName, ConnectionSupplier conns) {
        return new PostgresDdlTable(tableName, conns, config);
    }

    @Override
    public DbReadTable createRead(String tableName, ConnectionSupplier conns) {
        return new BatchedDbReadTable(conns, new PostgresQueryFactory(tableName, config), exec, config.shared());
    }

    @Override
    public DbWriteTable createWrite(String tableName, ConnectionSupplier conns) {
        return new SimpleDbWriteTable(tableName, conns, config);
    }

    @Override
    public DBType getDbType() {
        return DBType.POSTGRESQL;
    }

    @Override
    public void close() {
        exec.shutdown();
    }
}
