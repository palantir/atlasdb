/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cli.command;

import java.util.concurrent.ExecutorService;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraSchemaLockCleaner;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTableDropper;
import com.palantir.atlasdb.keyvalue.cassandra.CellLoader;
import com.palantir.atlasdb.keyvalue.cassandra.CellValuePutter;
import com.palantir.atlasdb.keyvalue.cassandra.HeartbeatService;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLock;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLockTables;
import com.palantir.atlasdb.keyvalue.cassandra.TaskRunner;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.cassandra.WrappingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

import io.airlift.airline.Command;

@Command(name = "clean-cass-locks-state", description = "Clean up and get the schema mutation "
        + "locks for the CassandraKVS into a good state")
public class CleanCassLocksStateCommand extends AbstractCommand {
    private static final Logger log = LoggerFactory.getLogger(CleanCassLocksStateCommand.class);
    private static final OutputPrinter printer = new OutputPrinter(log);

    @Override
    public Integer call() throws Exception {
        Preconditions.checkState(isOffline(), "This CLI can only be run offline");

        CassandraKeyValueServiceConfig config = getCassandraKvsConfig();

        CassandraClientPool clientPool = CassandraClientPoolImpl.create(config);
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, config);

        TracingQueryRunner tracingQueryRunner = new TracingQueryRunner(log, new TracingPrefsConfig());

        SchemaMutationLock schemaMutationLock = getSchemaMutationLock(config, clientPool, lockTables,
                tracingQueryRunner);

        CassandraTableDropper cassandraTableDropper = getCassandraTableDropper(config, clientPool, tracingQueryRunner,
                "Atlas CleanCassLocksState");

        new CassandraSchemaLockCleaner(lockTables, schemaMutationLock, cassandraTableDropper).cleanLocksState();
        printer.info("Schema mutation lock cli completed successfully.");
        return 0;
    }

    private CassandraTableDropper getCassandraTableDropper(
            CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            TracingQueryRunner tracingQueryRunner,
            String purpose) {
        WrappingQueryRunner wrappingQueryRunner = new WrappingQueryRunner(tracingQueryRunner);
        ExecutorService executorService = PTExecutors.newFixedThreadPool(config.poolSize(),
                new NamedThreadFactory(purpose, false));
        TaskRunner taskRunner = new TaskRunner(executorService);
        CellLoader cellLoader = new CellLoader(config, clientPool, wrappingQueryRunner, taskRunner);

        CellValuePutter cellValuePutter = new CellValuePutter(config, clientPool, taskRunner,
                wrappingQueryRunner, ConsistencyLevel.QUORUM);

        return new CassandraTableDropper(config,
                clientPool,
                cellLoader,
                cellValuePutter,
                wrappingQueryRunner,
                ConsistencyLevel.ALL);
    }

    private SchemaMutationLock getSchemaMutationLock(CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool, SchemaMutationLockTables lockTables,
            TracingQueryRunner tracingQueryRunner) {
        HeartbeatService heartbeatService = new HeartbeatService(
                clientPool,
                tracingQueryRunner,
                HeartbeatService.DEFAULT_HEARTBEAT_TIME_PERIOD_MILLIS,
                getLockTable(lockTables),
                ConsistencyLevel.QUORUM);
        return new SchemaMutationLock(true,
                config,
                clientPool,
                tracingQueryRunner,
                ConsistencyLevel.QUORUM,
                // TODO we should pass in the real value... but we don't know it yet :'(
                () -> getLockTable(lockTables),
                heartbeatService,
                SchemaMutationLock.DEFAULT_DEAD_HEARTBEAT_TIMEOUT_THRESHOLD_MILLIS);
    }

    private CassandraKeyValueServiceConfig getCassandraKvsConfig() {
        KeyValueServiceConfig kvsConfig = getAtlasDbConfig().keyValueService();
        if (!kvsConfig.type().equals(CassandraKeyValueServiceConfig.TYPE)) {
            throw new IllegalStateException(
                    String.format("KeyValueService must be of type %s, but yours is %s",
                            CassandraKeyValueServiceConfig.TYPE, kvsConfig.type()));
        }
        return (CassandraKeyValueServiceConfig) kvsConfig;
    }

    private TableReference getLockTable(SchemaMutationLockTables lockTables) {
        try {
            return lockTables.getAllLockTables().stream().findAny().orElseThrow(
                    () -> new IllegalStateException("Couldn't find a lock table!"));
        } catch (TException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
