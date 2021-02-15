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
package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;
import com.palantir.nexus.db.pool.RetriableTransactions.TransactionResult;
import com.palantir.nexus.db.pool.RetriableWriteTransaction;
import com.palantir.timestamp.AutoDelegate_TimestampBoundStore;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.OptionalLong;
import javax.annotation.concurrent.GuardedBy;

// TODO(hsaraogi): switch to using ptdatabase sql running, which more gracefully supports multiple db types.
public class InDbTimestampBoundStore implements TimestampBoundStore {
    private final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_TimestampBoundStore {
        @Override
        public TimestampBoundStore delegate() {
            checkInitialized();
            return InDbTimestampBoundStore.this;
        }

        @Override
        protected void tryInitialize() {
            InDbTimestampBoundStore.this.init();
        }

        @Override
        protected String getInitializingClassName() {
            return "InDbTimestampBoundStore";
        }
    }

    private static final String EMPTY_TABLE_PREFIX = "";

    private final ConnectionManager connManager;
    private final PhysicalBoundStoreStrategy physicalBoundStoreStrategy;
    private final InitializingWrapper wrapper = new InitializingWrapper();

    @GuardedBy("this") // lazy init to avoid db connections in constructors
    private DBType dbType;

    @GuardedBy("this")
    private Long currentLimit = null;

    /**
     * Use only if you have already initialized the timestamp table. This exists for legacy support.
     */
    public InDbTimestampBoundStore(ConnectionManager connManager, TableReference timestampTable) {
        this(connManager, new LegacyPhysicalBoundStoreStrategy(timestampTable, EMPTY_TABLE_PREFIX));
    }

    public static TimestampBoundStore create(ConnectionManager connManager, TableReference timestampTable) {
        return InDbTimestampBoundStore.create(connManager, timestampTable, EMPTY_TABLE_PREFIX);
    }

    public static TimestampBoundStore create(
            ConnectionManager connManager, TableReference timestampTable, boolean initializeAsync) {
        return InDbTimestampBoundStore.create(connManager, timestampTable, EMPTY_TABLE_PREFIX, initializeAsync);
    }

    public static TimestampBoundStore create(
            ConnectionManager connManager, TableReference timestampTable, String tablePrefixString) {
        return createWithStrategy(
                connManager,
                new LegacyPhysicalBoundStoreStrategy(timestampTable, tablePrefixString),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static TimestampBoundStore create(
            ConnectionManager connManager,
            TableReference timestampTable,
            String tablePrefixString,
            boolean initializeAsync) {
        return createWithStrategy(
                connManager, new LegacyPhysicalBoundStoreStrategy(timestampTable, tablePrefixString), initializeAsync);
    }

    public static TimestampBoundStore createForMultiSeries(
            ConnectionManager connManager, TableReference timestampTable, TimestampSeries series) {
        return createWithStrategy(
                connManager,
                new MultiSequencePhysicalBoundStoreStrategy(timestampTable, series),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static TimestampBoundStore createForMultiSeries(
            ConnectionManager connManager,
            TableReference timestampTable,
            TimestampSeries series,
            boolean initializeAsync) {
        return createWithStrategy(
                connManager, new MultiSequencePhysicalBoundStoreStrategy(timestampTable, series), initializeAsync);
    }

    private static TimestampBoundStore createWithStrategy(
            ConnectionManager connManager, PhysicalBoundStoreStrategy strategy, boolean initializeAsync) {
        InDbTimestampBoundStore store = new InDbTimestampBoundStore(connManager, strategy);
        store.wrapper.initialize(initializeAsync);
        return store.wrapper.isInitialized() ? store : store.wrapper;
    }

    private InDbTimestampBoundStore(
            ConnectionManager connManager, PhysicalBoundStoreStrategy physicalBoundStoreStrategy) {
        this.connManager = Preconditions.checkNotNull(connManager, "connectionManager is required");
        this.physicalBoundStoreStrategy = physicalBoundStoreStrategy;
    }

    private void init() {
        try (Connection conn = connManager.getConnection()) {
            physicalBoundStoreStrategy.createTimestampTable(conn, this::getDbType);
        } catch (SQLException error) {
            throw PalantirSqlException.create(error);
        }
    }

    private interface Operation {
        long run(Connection connection, OptionalLong oldLimit) throws SQLException;
    }

    @GuardedBy("this")
    private long runOperation(final Operation operation) {
        TransactionResult<Long> result = RetriableTransactions.run(connManager, new RetriableWriteTransaction<Long>() {
            @GuardedBy("InDbTimestampBoundStore.this")
            @Override
            public Long run(Connection connection) throws SQLException {
                OptionalLong oldLimit = physicalBoundStoreStrategy.readLimit(connection);
                if (currentLimit != null) {
                    if (oldLimit.isPresent()) {
                        if (oldLimit.getAsLong() != currentLimit) {
                            // mismatch
                            throw new MultipleRunningTimestampServiceError(
                                    "Timestamp limit changed underneath us (limit in memory: " + currentLimit
                                            + ", limit in DB: " + oldLimit + "). This may indicate that "
                                            + "another timestamp service is running against this database!  "
                                            + "This may indicate that your services are not properly configured "
                                            + "to run in an HA configuration, or to have a CLI run against them.");
                        }
                    } else {
                        // disappearance
                        throw new SafeIllegalStateException("Unable to retrieve a timestamp when expected. "
                                + "This service is in a dangerous state and should be taken down "
                                + "until a new safe timestamp value can be established in the KVS. "
                                + "Please contact support.");
                    }
                } else {
                    // first read, no check to be done
                }
                return operation.run(connection, oldLimit);
            }
        });
        switch (result.getStatus()) {
            case SUCCESSFUL:
                currentLimit = result.getResultValue();
                return currentLimit;
            case UNKNOWN:
            case FAILED:
                if (result.getStatus() == RetriableTransactions.TransactionStatus.UNKNOWN) {
                    // Since the DB's state is unknown, the in-memory state can't be confirmed, so get rid of it.
                    currentLimit = null;
                }

                Throwable error = result.getError();
                if (error instanceof SQLException) {
                    throw PalantirSqlException.create((SQLException) error);
                }
                throw Throwables.rewrapAndThrowUncheckedException(error);
            default:
                throw new IllegalStateException("Unrecognized transaction status " + result.getStatus());
        }
    }

    @Override
    public synchronized long getUpperLimit() {
        return runOperation((connection, oldLimit) -> {
            if (oldLimit.isPresent()) {
                return oldLimit.getAsLong();
            }

            final long startVal = 10000;
            physicalBoundStoreStrategy.createLimit(connection, startVal);
            return startVal;
        });
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        runOperation((connection, oldLimit) -> {
            if (oldLimit.isPresent()) {
                physicalBoundStoreStrategy.writeLimit(connection, limit);
            } else {
                physicalBoundStoreStrategy.createLimit(connection, limit);
            }

            return limit;
        });
    }

    @GuardedBy("this")
    private DBType getDbType(Connection connection) {
        if (dbType == null) {
            dbType = ConnectionDbTypes.getDbType(connection);
        }
        return dbType;
    }
}
