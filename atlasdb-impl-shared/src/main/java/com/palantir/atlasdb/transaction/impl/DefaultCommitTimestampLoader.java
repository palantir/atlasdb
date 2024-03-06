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
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.api.TransactionLockAcquisitionTimeoutException;
import com.palantir.atlasdb.transaction.knowledge.KnownAbandonedTransactions;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.AsyncTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

public final class DefaultCommitTimestampLoader implements CommitTimestampLoader {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultCommitTimestampLoader.class);
    private static final SafeLogger perfLogger = SafeLoggerFactory.get("dualschema.perf");
    private final TimestampCache timestampCache;
    private final Optional<LockToken> immutableTimestampLock;
    private final Supplier<Long> startTimestampSupplier;
    private final Supplier<TransactionConfig> transactionConfig;
    private final MetricsManager metricsManager;
    private final TimelockService timelockService;
    private final long immutableTimestamp;
    private final Supplier<Long> lastSeenCommitTsSupplier;

    private final KnownAbandonedTransactions abortedTransactionsCache;

    public DefaultCommitTimestampLoader(
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
        this.lastSeenCommitTsSupplier = knowledge.lastSeenCommitSupplier();
        this.abortedTransactionsCache = knowledge.abandoned();
    }

    /**
     * Returns a map from start timestamp to commit timestamp. If a start timestamp wasn't committed, then it will be
     * missing from the map. This method will block until the transactions for these start timestamps are complete.
     */
    @Override
    public ListenableFuture<LongLongMap> getCommitTimestamps(
            @Nullable TableReference tableRef,
            LongIterable startTimestamps,
            boolean shouldWaitForCommitterToComplete,
            AsyncTransactionService asyncTransactionService) {
        if (startTimestamps.isEmpty()) {
            return Futures.immediateFuture(LongLongMaps.immutable.of());
        }

        MutableLongSet pendingGets = LongSets.mutable.of();
        MutableLongLongMap result = new LongLongHashMap();
        startTimestamps.each(startTs -> {
            Long commitTs = timestampCache.getCommitTimestampIfPresent(startTs);
            if (commitTs == null) {
                pendingGets.add(startTs);
            } else {
                result.put(startTs, commitTs);
            }
        });

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
                    LongLongMap loadedCommitTs = cacheKnownLoadedValuesAndValidate(rawResults);
                    result.putAll(loadedCommitTs);
                    return result;
                },
                MoreExecutors.directExecutor());
    }

    // We do not cache unknown transactions as they are already being cached at a lower level.
    private LongLongMap cacheKnownLoadedValuesAndValidate(Map<Long, TransactionStatus> rawResults) {
        MutableLongLongMap results = LongLongMaps.mutable.empty();
        boolean shouldValidate = false;

        // The method is written this way to avoid multiple scans on the result set as it is on a hot path.
        for (Map.Entry<Long, TransactionStatus> entry : rawResults.entrySet()) {
            long start = entry.getKey();
            TransactionStatus commitStatus = entry.getValue();

            if (commitStatus.equals(TransactionStatus.inProgress())) {
                continue;
            }

            long commitTs = TransactionStatusUtils.getCommitTsFromStatus(
                    start, commitStatus, abortedTransactionsCache::isKnownAbandoned);
            if (commitStatus.equals(TransactionStatus.unknown())) {
                shouldValidate = true;
            } else {
                timestampCache.putAlreadyCommittedTransaction(start, commitTs);
            }
            results.put(start, commitTs);
        }

        if (shouldValidate) {
            throwIfTransactionsTableSweptBeyondReadOnlyTxn();
        }

        return results.asUnmodifiable();
    }

    /**
     * We will block here until the passed transactions have released their lock. This means that the committing
     * transaction is either complete or it has failed, and we are allowed to roll it back.
     */
    private void waitForCommitToComplete(LongIterable startTimestamps) {
        ImmutableSet<LockDescriptor> lockDescriptors = startTimestamps
                // We only need to block and wait for transactions on or after immutable timestamp
                .select(start -> start >= immutableTimestamp)
                .collect(start -> AtlasRowLockDescriptor.of(
                        TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                        TransactionConstants.getValueForTimestamp(start)))
                .toImmutableSet();

        if (lockDescriptors.isEmpty()) {
            return;
        }

        waitFor(lockDescriptors.castToSet());
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

    private void waitForCommitterToComplete(@Nullable TableReference tableRef, LongIterable startTimestamps) {
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
        return metricsManager.registerOrGetTimer(DefaultCommitTimestampLoader.class, name);
    }

    private static ListenableFuture<Map<Long, TransactionStatus>> loadCommitTimestamps(
            AsyncTransactionService asyncTransactionService, LongSet startTimestamps) {
        // distinguish between a single timestamp and a batch, for more granular metrics
        if (startTimestamps.size() == 1) {
            long singleTs = startTimestamps.longIterator().next();
            return Futures.transform(
                    asyncTransactionService.getAsyncV2(singleTs),
                    commitState -> ImmutableMap.of(singleTs, commitState),
                    MoreExecutors.directExecutor());
        } else {
            return asyncTransactionService.getAsyncV2(startTimestamps.collect(Long::valueOf));
        }
    }

    private void throwIfTransactionsTableSweptBeyondReadOnlyTxn() {
        long startTs = startTimestampSupplier.get();
        // The schema version of current transaction does not matter. If the current transaction does not hold
        // immutableTs lock, and we were previously on schema 4 for a range of transactions, we cannot know the state
        // of those writes consistently if sweep has progressed.

        if (immutableTimestampLock.isEmpty()) {
            Preconditions.checkState(
                    lastSeenCommitTsSupplier.get() < startTs,
                    "Sweep has swept some entries with a commit TS after us, and now we cannot know the commit TS for"
                            + " a timestamp that has been TTSd, but it is greater than our start timestamp. This can "
                            + "happen if the transaction has been alive for more than an hour and is expected to be "
                            + "transient.");
        }
    }
}
