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

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.TransactionLockAcquisitionTimeoutException;
import com.palantir.atlasdb.transaction.knowledge.KnownAbortedTransactions;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.AsyncTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class CommitTimestampLoader {
    private static final SafeLogger log = SafeLoggerFactory.get(CommitTimestampLoader.class);
    private static final SafeLogger perfLogger = SafeLoggerFactory.get("dualschema.perf");
    private final TimestampCache timestampCache;
    private final Optional<LockToken> immutableTimestampLock;
    private final Supplier<Long> startTimestampSupplier;
    private final Supplier<TransactionConfig> transactionConfig;
    private final MetricsManager metricsManager;
    private final TimelockService timelockService;
    private final long immutableTimestamp;
    private final Supplier<Long> lastSeenCommitTs;

    private final KnownAbortedTransactions abortedTransactionsCache;

    public CommitTimestampLoader(
            TimestampCache timestampCache,
            Optional<LockToken> immutableTimestampLock,
            Supplier<Long> startTimestampSupplier,
            Supplier<TransactionConfig> transactionConfig,
            MetricsManager metricsManager,
            TimelockService timelockService,
            long immutableTimestamp,
            TransactionKnowledgeComponents knowledge) {
        this.timestampCache = timestampCache;
        this.immutableTimestampLock = immutableTimestampLock;
        this.startTimestampSupplier = startTimestampSupplier;
        this.transactionConfig = transactionConfig;
        this.metricsManager = metricsManager;
        this.timelockService = timelockService;
        this.immutableTimestamp = immutableTimestamp;
        this.lastSeenCommitTs = knowledge.getLastSeenCommitSupplier();
        this.abortedTransactionsCache = knowledge.aborted();
    }

    /**
     * Returns a map from start timestamp to commit timestamp. If a start timestamp wasn't committed, then it will be
     * missing from the map. This method will block until the transactions for these start timestamps are complete.
     */
    ListenableFuture<Map<Long, Long>> getCommitTimestamps(
            @Nullable TableReference tableRef,
            Iterable<Long> startTimestamps,
            boolean shouldWaitForCommitterToComplete,
            AsyncTransactionService asyncTransactionService) {
        if (Iterables.isEmpty(startTimestamps)) {
            return Futures.immediateFuture(ImmutableMap.of());
        }

        Set<Long> pendingGets = new HashSet<>();
        Map<Long, Long> result = new HashMap<>();

        for (Long startTs : startTimestamps) {
            Long commitTs = timestampCache.getCommitTimestampIfPresent(startTs);
            if (commitTs == null) {
                pendingGets.add(startTs);
            } else {
                result.put(startTs, commitTs);
            }
        }

        if (pendingGets.isEmpty()) {
            return Futures.immediateFuture(result);
        }

        // Before we do the reads, we need to make sure the committer is done writing.
        if (shouldWaitForCommitterToComplete) {
            waitForCommitterToComplete(tableRef, startTimestamps);
        }

        return Futures.transform(
                loadCommitTimestamps(asyncTransactionService, pendingGets),
                rawResults -> {
                    Map<Long, Long> loadedCommitTs = cacheKnownLoadedValuesAndValidate(rawResults);
                    result.putAll(loadedCommitTs);
                    return result;
                },
                MoreExecutors.directExecutor());
    }

    // We do not cache unknown transactions as they are already being cached at a lower level.
    private Map<Long, Long> cacheKnownLoadedValuesAndValidate(Map<Long, TransactionStatus> rawResults) {
        Map<Long, Long> results = new HashMap<>();
        boolean shouldValidate = false;

        // The method is written this way to avoid multiple scans on the result set as it is on a hot path.
        for (Map.Entry<Long, TransactionStatus> entry : rawResults.entrySet()) {
            long start = entry.getKey();
            TransactionStatus commitStatus = entry.getValue();

            if (commitStatus.equals(TransactionStatuses.inProgress())) continue;

            long commitTs = TransactionStatusUtils.getCommitTsFromStatus(
                    start, commitStatus, abortedTransactionsCache::isKnownAborted);
            if (commitStatus.equals(TransactionStatuses.unknown())) {
                shouldValidate = true;
            } else {
                timestampCache.putAlreadyCommittedTransaction(start, commitTs);
            }
            results.put(start, commitTs);
        }

        if (shouldValidate) {
            throwIfTransactionsTableSweptBeyondReadOnlyTxn();
        }

        return results;
    }

    /**
     * We will block here until the passed transactions have released their lock. This means that the committing
     * transaction is either complete or it has failed, and we are allowed to roll it back.
     */
    private void waitForCommitToComplete(Iterable<Long> startTimestamps) {
        Set<LockDescriptor> lockDescriptors = new HashSet<>();
        for (long start : startTimestamps) {
            if (start < immutableTimestamp) {
                // We don't need to block in this case because this transaction is already complete
                continue;
            }
            lockDescriptors.add(AtlasRowLockDescriptor.of(
                    TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                    TransactionConstants.getValueForTimestamp(start)));
        }

        if (lockDescriptors.isEmpty()) {
            return;
        }

        waitFor(lockDescriptors);
    }

    private void waitFor(Set<LockDescriptor> lockDescriptors) {
        TransactionConfig currentTransactionConfig = transactionConfig.get();

        // TODO(fdesouza): Revert this once PDS-95791 is resolved.
        long lockAcquireTimeoutMillis = currentTransactionConfig.getLockAcquireTimeoutMillis();
        WaitForLocksRequest request = WaitForLocksRequest.of(lockDescriptors, lockAcquireTimeoutMillis);
        WaitForLocksResponse response = timelockService.waitForLocks(request);
        if (!response.wasSuccessful()) {
            log.error(
                    "Timed out waiting for commits to complete. Timeout was {} ms. First ten locks were {}.",
                    SafeArg.of("requestId", request.getRequestId()),
                    SafeArg.of("acquireTimeoutMs", lockAcquireTimeoutMillis),
                    SafeArg.of("numberOfDescriptors", lockDescriptors.size()),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out waiting for commits to complete.");
        }
    }

    private void waitForCommitterToComplete(@Nullable TableReference tableRef, Iterable<Long> startTimestamps) {
        Timer.Context timer = getTimer("waitForCommitTsMillis").time();
        waitForCommitToComplete(startTimestamps);
        long waitForCommitTsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());

        if (tableRef != null) {
            perfLogger.debug(
                    "Waited to get commit timestamps when reading from a known table.",
                    SafeArg.of("commitTsMillis", waitForCommitTsMillis),
                    LoggingArgs.tableRef(tableRef));
        } else {
            perfLogger.debug("Waited to get commit timestamps.", SafeArg.of("commitTsMillis", waitForCommitTsMillis));
        }
    }

    private Timer getTimer(String name) {
        return metricsManager.registerOrGetTimer(CommitTimestampLoader.class, name);
    }

    private static ListenableFuture<Map<Long, TransactionStatus>> loadCommitTimestamps(
            AsyncTransactionService asyncTransactionService, Set<Long> startTimestamps) {
        // distinguish between a single timestamp and a batch, for more granular metrics
        if (startTimestamps.size() == 1) {
            Long singleTs = startTimestamps.iterator().next();
            return Futures.transform(
                    asyncTransactionService.getAsyncV2(singleTs),
                    commitTsOrNull ->
                            commitTsOrNull == null ? ImmutableMap.of() : ImmutableMap.of(singleTs, commitTsOrNull),
                    MoreExecutors.directExecutor());
        } else {
            return asyncTransactionService.getAsyncV2(startTimestamps);
        }
    }

    private void throwIfTransactionsTableSweptBeyondReadOnlyTxn() {
        long startTs = startTimestampSupplier.get();
        // The schema version of current transaction does not matter. If the current transaction does not hold
        // immutableTs lock, and we were previously on schema 4 for a range of transactions, we cannot know the state
        // of those writes consistently if sweep has progressed.

        if (immutableTimestampLock.isEmpty()) {
            Preconditions.checkState(
                    lastSeenCommitTs.get() < startTs,
                    "Transactions table has been swept beyond current start timestamp, therefore, we cannot"
                            + " consistently values accessible to this transactions. This can happen if the transaction"
                            + " has been alive for more than an hour and is expected to be transient.");
        }
    }
}
