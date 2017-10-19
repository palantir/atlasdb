/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleStandardEditionShrinkConfiguration;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.nexus.db.sql.SqlConnection;

public class OracleShrinkExecutor {
    private static final Logger log = LoggerFactory.getLogger(OracleShrinkExecutor.class);

    private final SqlConnectionSupplier connectionPool;
    private final ExecutorService executorService;
    private final OracleTableNameGetter oracleTableNameGetter;
    private final OracleStandardEditionShrinkConfiguration shrinkConfig;

    private static Future<Boolean> previousShrinkFuture;

    public OracleShrinkExecutor(
            SqlConnectionSupplier connectionPool,
            ExecutorService executorService,
            OracleTableNameGetter oracleTableNameGetter,
            OracleStandardEditionShrinkConfiguration shrinkConfig) {
        this.connectionPool = connectionPool;
        this.executorService = executorService;
        this.oracleTableNameGetter = oracleTableNameGetter;
        this.shrinkConfig = shrinkConfig;
    }

    public void shrinkAsync(TableReference tableRef) {
        final long secondsToWaitBeforeShrink = getSecondsToWaitBeforeShrink();
        previousShrinkFuture = executorService.submit(
                () -> shrinkCompactFollowedByShrink(tableRef, secondsToWaitBeforeShrink));

    }

    private long getSecondsToWaitBeforeShrink() {
        return shouldWaitLonger()
                ? shrinkConfig.shrinkPauseSeconds() + shrinkConfig.shrinkPauseOnFailureSeconds()
                : shrinkConfig.shrinkPauseSeconds();
    }

    private boolean shouldWaitLonger() {
        if (previousShrinkFuture != null) {
            try {
                if (previousShrinkFailed()) {
                    return true;
                }
            } catch (InterruptedException | ExecutionException e) {
                return true;
            }
        }
        return false;
    }

    private boolean previousShrinkFailed() throws InterruptedException, ExecutionException {
        return !previousShrinkFuture.get();
    }

    private boolean shrinkCompactFollowedByShrink(TableReference tableRef, long secondsToWaitBeforeShrink) {

        try {
            TimeUnit.SECONDS.sleep(secondsToWaitBeforeShrink);
        } catch (InterruptedException e) {
            log.warn("Skipping Shrink for table: {} because the thread was interrupted.", LoggingArgs.tableRef("tableToShrink", tableRef));
            return false;
        }

        Stopwatch timer = Stopwatch.createStarted();
        ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
        try {
            Stopwatch shrinkAndCompactTimer = Stopwatch.createStarted();
            getConnectionWithIncreasedTimeout(conns).executeUnregisteredQuery(
                    "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                            + " SHRINK SPACE COMPACT");
            log.info("Call to SHRINK SPACE COMPACT on table {} took {} ms.",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("time taken", shrinkAndCompactTimer.elapsed(TimeUnit.MILLISECONDS)));

            Stopwatch shrinkTimer = Stopwatch.createStarted();
            getConnectionWithIncreasedTimeout(conns).executeUnregisteredQuery(
                    "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                            + " SHRINK SPACE");
            log.info("Call to SHRINK SPACE on table {} took {} ms."
                            + " This implies that locks on the entire table were held for this period.",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("time taken", shrinkTimer.elapsed(TimeUnit.MILLISECONDS)));
            return true;
        } catch (PalantirSqlException e) {
            log.error("Tried to clean up {} bloat after a sweep operation via Oracle Shrink, but failed."
                    + " If you are running against Enterprise Edition, you can set enableOracleEnterpriseFeatures"
                    + " to true in the configuration to start running Oracle EE move online operations."
                    + " Otherwise, good practice would be to do occasional offline manual maintenance of rebuilding"
                    + " IOT tables to compensate for bloat. You can contact Palantir Support if you'd"
                    + " like more information. Underlying error was: {}",
                    LoggingArgs.tableRef("tableToShrink", tableRef),
                    UnsafeArg.of("exception message", e.getMessage()));
            return false;
        } catch (TableMappingNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            //closing so that other operations cannot grab the increased timeout connection.
            conns.close();
            log.info("Call to KVS.compactInternally on table {} took {} ms.",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("time taken", timer.elapsed(TimeUnit.MILLISECONDS)));
        }
    }

    private SqlConnection getConnectionWithIncreasedTimeout(ConnectionSupplier conns) {
        SqlConnection sqlConnection = conns.get();
        try {
            int originalNetworkTimeout = sqlConnection.getUnderlyingConnection().getNetworkTimeout();
            int newNetworkTimeout = shrinkConfig.shrinkConnectionTimeoutMillis();
            sqlConnection.getUnderlyingConnection().setNetworkTimeout(Executors.newSingleThreadExecutor(),
                    newNetworkTimeout);
            log.info("Increased sql socket read timeout from {} to {}",
                    SafeArg.of("originalNetworkTimeout", originalNetworkTimeout),
                    SafeArg.of("newNetworkTimeout", newNetworkTimeout));
            return sqlConnection;
        } catch (SQLException e) {
            log.warn("Failed to increase socket read timeout for the connection. Encountered an exception:", e);
            return sqlConnection;
        }
    }
}
