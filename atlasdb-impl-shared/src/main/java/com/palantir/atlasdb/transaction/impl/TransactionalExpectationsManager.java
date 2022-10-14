/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.transaction.api.OpenTransaction;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.NotImplementedException;

public final class TransactionalExpectationsManager implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionalExpectationsManager.class);

    private final AtomicBoolean updateIsScheduled = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService;
    private final MetricsManager metricsManager;
    private final Map<OpenTransaction, Stopwatch> transactionClock = new ConcurrentHashMap<>();

    public TransactionalExpectationsManager(ScheduledExecutorService executorService, MetricsManager metricsManager) {
        this.executorService = executorService;
        this.metricsManager = metricsManager;
    }

    public void scheduleMetricsUpdate(long delayMillis) {
        Preconditions.checkArgument(
                delayMillis > 0, "Transactional expectations manager scheduler delay must be strictly positive.");
        if (!updateIsScheduled.compareAndExchange(false, true)) {
            executorService.scheduleWithFixedDelay(this::run, delayMillis, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void registerTransactionStart(OpenTransaction transaction) {
        transactionClock.putIfAbsent(transaction, Stopwatch.createStarted());
    }

    private void registerTransactionCompletion(OpenTransaction transaction) {
        transactionClock.remove(transaction);
    }

    private void run() {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        updateIsScheduled.set(true);
        executorService.shutdown();
    }
}
