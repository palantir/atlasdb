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
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.transaction.api.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.api.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.ExpectationsStatistics;
import com.palantir.atlasdb.transaction.api.ImmutableExpectationsStatistics;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// class is getting too big perhaps? should I separate concerns?
// class is doing:
//   metrics registration
//   scheduling metrics update
//   transaction start/end registration
public final class ExpectationsManager implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(ExpectationsManager.class);

    private final ScheduledExecutorService executorService;
    private final ExpectationsMetricsTracker tracker;
    private final Map<ExpectationsAwareTransaction, Stopwatch> transactionClock = new ConcurrentHashMap<>();
    private final AtomicBoolean updateIsScheduled = new AtomicBoolean(false);

    public ExpectationsManager(ScheduledExecutorService executorService, MetricsManager metricsManager) {
        this.executorService = executorService;
        this.tracker = new ExpectationsMetricsTracker(metricsManager);
    }

    public void scheduleMetricsUpdate(long delayMillis) {
        Preconditions.checkArgument(
                delayMillis > 0, "Transactional expectations manager scheduler delay must be strictly positive.");
        if (!updateIsScheduled.compareAndExchange(false, true)) {
            executorService.scheduleWithFixedDelay(this::run, delayMillis, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    void registerTransaction(ExpectationsAwareTransaction transaction) {
        transactionClock.putIfAbsent(transaction, Stopwatch.createStarted());
    }

    /*
     * Stop tracking a given transaction.
     */
    void unregisterTransaction(ExpectationsAwareTransaction transaction) {
        transactionClock.remove(transaction);
    }

    /*
     * Cleans up state and calls expectations callbacks for successful/aborted transaction.
     */
    void markConcludedTransaction(ExpectationsAwareTransaction transaction) {
        // empty maps are placeholders for now
        ExpectationsStatistics stats = ImmutableExpectationsStatistics.builder()
                .transactionAgeMillis(Optional.ofNullable(transactionClock.get(transaction))
                        .map(Stopwatch::elapsed)
                        .map(Duration::toMillis))
                .bytesRead(transaction.getBytesReadByTable())
                .maximumBytesReadInOneKvsCall(ImmutableMap.of())
                .kvsReadCallCount(ImmutableMap.of())
                .build();
        transaction.runExpectationsCallbacks(stats);
        transactionClock.remove(transaction);
    }

    // worst offender tracking for each violation will change how this is impl
    private void run() {
        KeyedStream.stream(transactionClock).forEach(this::run);
    }

    private void run(ExpectationsAwareTransaction transaction, Stopwatch stopwatch) {
        ExpectationsConfig currentConfig = transaction.expectationsConfig();

        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // todo aalouane: modify this to reflect terminology used in metrics/alert for easier grep-ing through logs
        if (elapsed > currentConfig.transactionAgeMillisLimit()) {
            // too many dot accesses?
            tracker.transactionsTooOld.increment();
            log.warn(
                    "Transaction is running for longer than expected",
                    SafeArg.of("transactionAgeMillis", elapsed),
                    SafeArg.of("maxTransactionAgeMillis", currentConfig.transactionAgeMillisLimit()),
                    SafeArg.of("transactionName", currentConfig.transactionName()));
        }

        long bytesRead = transaction.getBytesRead();

        if (bytesRead > currentConfig.bytesReadLimit()) {
            tracker.transactionsReadingTooMuch.increment();
            log.warn(
                    "Transaction is reading more than expected",
                    SafeArg.of("bytesRead", bytesRead),
                    SafeArg.of("bytesReadLimit", currentConfig.bytesReadLimit()),
                    SafeArg.of("transactionName", currentConfig.transactionName()));
        }
    }

    // todo aalouane: see where this could be actually ran (seen some code doing autocloseable magic somewhere)
    @Override
    public void close() {
        updateIsScheduled.set(true);
        executorService.shutdown();
    }

    // not sure if this merits its own file. it's literally just two long adders
    // mimicking MetricsForStrategy but not sure if that is up to current standards
    private static final class ExpectationsMetricsTracker {
        private final ExpectationsMetrics metrics;
        private final AccumulatingValueMetric transactionsTooOld;
        private final AccumulatingValueMetric transactionsReadingTooMuch;

        private ExpectationsMetricsTracker(MetricsManager manager) {
            this.transactionsTooOld = new AccumulatingValueMetric();
            this.transactionsReadingTooMuch = new AccumulatingValueMetric();
            this.metrics = ExpectationsMetrics.of(manager.getTaggedRegistry());
            registerMetrics();
        }

        private void registerMetrics() {
            metrics.transactionsTooOld(transactionsTooOld);
            metrics.transactionsReadingTooMuch(transactionsReadingTooMuch);
        }
    }
}
