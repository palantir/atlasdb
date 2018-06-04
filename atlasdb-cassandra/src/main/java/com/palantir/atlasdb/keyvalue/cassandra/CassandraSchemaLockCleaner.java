/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

public final class CassandraSchemaLockCleaner {
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaLockCleaner.class);

    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;
    private final SchemaMutationLockTables lockTables;
    private final TracingQueryRunner queryRunner;
    private final CassandraTableDropper cassandraTableDropper;

    public static CassandraSchemaLockCleaner create(CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            SchemaMutationLockTables lockTables,
            TracingQueryRunner queryRunner) {
        CassandraTableDropper cassandraTableDropper = getCassandraTableDropper(config, clientPool, queryRunner);

        return new CassandraSchemaLockCleaner(config, clientPool, lockTables, queryRunner,
                cassandraTableDropper);
    }

    private CassandraSchemaLockCleaner(CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            SchemaMutationLockTables lockTables,
            TracingQueryRunner queryRunner,
            CassandraTableDropper cassandraTableDropper) {
        this.config = config;
        this.clientPool = clientPool;
        this.lockTables = lockTables;
        this.queryRunner = queryRunner;
        this.cassandraTableDropper = cassandraTableDropper;
    }

    private static CassandraTableDropper getCassandraTableDropper(
            CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            TracingQueryRunner tracingQueryRunner) {
        WrappingQueryRunner wrappingQueryRunner = new WrappingQueryRunner(tracingQueryRunner);
        ExecutorService executorService = PTExecutors.newFixedThreadPool(config.poolSize(),
                new NamedThreadFactory("Atlas CleanCassLocksState", false));
        TaskRunner taskRunner = new TaskRunner(executorService);
        CellLoader cellLoader = new CellLoader(config, clientPool, wrappingQueryRunner, taskRunner);

        CellValuePutter cellValuePutter = new CellValuePutter(
                config,
                clientPool,
                taskRunner,
                wrappingQueryRunner,
                ConsistencyLevel.QUORUM,
                System::currentTimeMillis); // CassandraTableDropper also uses wall clock time

        return new CassandraTableDropper(config,
                clientPool,
                cellLoader,
                cellValuePutter,
                wrappingQueryRunner,
                ConsistencyLevel.ALL);
    }

    public void cleanLocksState() throws TException {
        Set<TableReference> tables = lockTables.getAllLockTables();
        Optional<TableReference> tableToKeep = tables.stream().findFirst();
        if (!tableToKeep.isPresent()) {
            log.info("No lock tables to clean up.");
            return;
        }
        TableReference remainingLockTable = tableToKeep.get();
        tables.remove(remainingLockTable);
        if (tables.size() > 0) {
            cassandraTableDropper.dropTables(tables);
            LoggingArgs.SafeAndUnsafeTableReferences safeAndUnsafe = LoggingArgs.tableRefs(tables);
            log.info("Dropped tables {} and {}", safeAndUnsafe.safeTableRefs(), safeAndUnsafe.unsafeTableRefs());
        }

        getSchemaMutationLock(remainingLockTable).cleanLockState();
        log.info("Reset the schema mutation lock in table [{}]",
                LoggingArgs.tableRef(remainingLockTable));
    }

    private SchemaMutationLock getSchemaMutationLock(TableReference remainingLockTable) {
        HeartbeatService heartbeatService = new HeartbeatService(
                clientPool,
                queryRunner,
                HeartbeatService.DEFAULT_HEARTBEAT_TIME_PERIOD_MILLIS,
                remainingLockTable,
                ConsistencyLevel.QUORUM);
        return new SchemaMutationLock(true,
                config,
                clientPool,
                queryRunner,
                ConsistencyLevel.QUORUM,
                () -> remainingLockTable,
                heartbeatService,
                SchemaMutationLock.DEFAULT_DEAD_HEARTBEAT_TIMEOUT_THRESHOLD_MILLIS);
    }
}
