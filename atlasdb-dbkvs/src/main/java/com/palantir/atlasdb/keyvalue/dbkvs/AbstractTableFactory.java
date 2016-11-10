/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbMetadataTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SimpleDbMetadataTable;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

public abstract class AbstractTableFactory implements DbTableFactory {
    protected final ExecutorService exec;
    protected final DdlConfig ddlConfig;

    protected AbstractTableFactory(DdlConfig ddlConfig) {
        this.ddlConfig = ddlConfig;
        this.exec = newFixedThreadPool(ddlConfig.poolSize(), "Atlas " + ddlConfig.type() + " reader");
    }

    protected static ThreadPoolExecutor newFixedThreadPool(int maxPoolSize, String threadNamePrefix) {
        ThreadPoolExecutor pool = PTExecutors.newThreadPoolExecutor(maxPoolSize, maxPoolSize,
                15L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory(threadNamePrefix, true /* daemon */));

        pool.allowCoreThreadTimeOut(false);
        return pool;
    }

    @Override
    public DbMetadataTable createMetadata(TableReference tableRef, ConnectionSupplier conns) {
        return new SimpleDbMetadataTable(tableRef, conns, ddlConfig);
    }

    @Override
    public void close() {
        exec.shutdown();
    }
}
