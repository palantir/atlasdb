/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueServiceManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.RateLimitedLogger;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.LongSupplier;

public final class DefaultDeleteExecutor implements DeleteExecutor {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultDeleteExecutor.class);
    private static final RateLimitedLogger deleteExecutorRateLimitedLogger = new RateLimitedLogger(log, 1.0);
    private static final String ERROR_MSG =
            "This isn't a bug but it should be infrequent if all nodes of your KV service are"
                    + " running. Delete has stronger consistency semantics than read/write and must talk to all nodes"
                    + " instead of just talking to a quorum of nodes. "
                    + "Failed to delete keys for table: {} from an uncommitted transaction; "
                    + " sweep should eventually clean these values.";

    private final TransactionKeyValueServiceManager keyValueService;

    private final LongSupplier freshTimestampSupplier;
    private final ExecutorService executorService;

    public DefaultDeleteExecutor(
            TransactionKeyValueServiceManager keyValueService,
            LongSupplier freshTimestampSupplier,
            ExecutorService executorService) {
        this.keyValueService = keyValueService;
        this.freshTimestampSupplier = freshTimestampSupplier;
        this.executorService = executorService;
    }

    @Override
    public void scheduleForDeletion(TableReference tableRef, Map<Cell, Long> keysToDelete) {
        if (keysToDelete.isEmpty()) {
            return;
        }
        try {
            executorService.execute(() -> {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "For table: {} we are deleting values of an uncommitted transaction: {}",
                                LoggingArgs.tableRef(tableRef),
                                UnsafeArg.of("keysToDelete", keysToDelete));
                    }
                    // TODO(jakubk): We probably want a disruptor to better batch this, but we should check metrics.
                    TransactionKeyValueService session =
                            keyValueService.getTransactionKeyValueService(freshTimestampSupplier);
                    session.delete(tableRef, Multimaps.forMap(keysToDelete));
                    // TODO(jakubk): Probably want to throw something better here, have special logging? Or retry?
                    Preconditions.checkState(
                            session.isValid(freshTimestampSupplier.getAsLong()), "Session not longer valid");
                } catch (RuntimeException e) {
                    if (log.isDebugEnabled()) {
                        log.warn(
                                ERROR_MSG + " The keys that failed to be deleted during rollback were {}",
                                LoggingArgs.tableRef(tableRef),
                                UnsafeArg.of("keysToDelete", keysToDelete),
                                e);
                    } else {
                        log.warn(ERROR_MSG, LoggingArgs.tableRef(tableRef), e);
                    }
                }
            });
        } catch (RejectedExecutionException rejected) {
            deleteExecutorRateLimitedLogger.log(logger -> logger.info(
                    "Could not delete keys {} for table {}, because the delete executor's queue was full."
                            + " Sweep should eventually clean these values.",
                    SafeArg.of("numKeysToDelete", keysToDelete.size()),
                    LoggingArgs.tableRef(tableRef),
                    rejected));
        }
    }

    @Override
    public void close() {
        MoreExecutors.shutdownAndAwaitTermination(executorService, Duration.ofSeconds(10));
    }
}
