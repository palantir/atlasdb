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

package com.palantir.atlasdb.transaction.knowledge;

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Aborted transactions timestamps are not expected to be contiguous. The caching for aborted transactions is bucketed
 * i.e. each bucket is a range of timestamps we cache the set of aborted timestamps in that bucket.
 * A bucket becomes immutable when lastKnownConcludedTs exceeds the upper bound for that bucket.
 * At any point in time, there will be exactly one bucket which will be mutable.
 *
 * This class provides a caching mechanism for the mutable bucket. The queries are expected to be highly concurrent and
 * are batched to reduce request volume to remote server. Since the queries are batched with exactly one consumer,
 * this class has been designed for serial execution to improve performance.
 *
 * Queries to this class must only be made for a transaction timestamp that is known to have been concluded.
 * */
public final class AbortedTransactionSoftCache implements AutoCloseable {
    public enum TransactionSoftCacheStatus {
        PENDING_LOAD_FROM_RELIABLE,
        IS_ABORTED,
        IS_NOT_ABORTED;
    }

    private final DisruptorAutobatcher<Long, TransactionSoftCacheStatus> autobatcher;

    private volatile PatchyCache patchyCache = null;
    private final FutileTimestampStore futileTimestampStore;
    private final KnownConcludedTransactions knownConcludedTransactions;

    public AbortedTransactionSoftCache(
            FutileTimestampStore futileTimestampStore, KnownConcludedTransactions knownConcludedTransactions) {
        this.futileTimestampStore = futileTimestampStore;
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.autobatcher = Autobatchers.coalescing(this::processBatch)
                .safeLoggablePurpose("get-transaction-soft-cache-status")
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .build();
    }

    public TransactionSoftCacheStatus getSoftCacheTransactionStatus(long startTimestamp) {
        return AtlasFutures.getUnchecked(autobatcher.apply(startTimestamp));
    }

    private Map<Long, TransactionSoftCacheStatus> processBatch(Set<Long> request) {
        Optional<PatchyCache> maybeSnapshot = getSnapshot();

        long latestTsSeenSoFar = getLatestTsSeenSoFar(request, maybeSnapshot);
        long latestBucketSeenSoFar = AbortedTimestampUtils.getBucket(latestTsSeenSoFar);

        PatchyCache refreshedPatchyCache = refreshPatchyCache(maybeSnapshot, latestTsSeenSoFar, latestBucketSeenSoFar);

        return KeyedStream.of(request)
                .map(startTimestamp -> {
                    long requestedBucket = AbortedTimestampUtils.getBucket(startTimestamp);

                    if (requestedBucket < latestBucketSeenSoFar) {
                        return TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE;
                    } else {
                        return getStatus(startTimestamp, refreshedPatchyCache.abortedTransactions);
                    }
                })
                .collectToMap();
    }

    private long getLatestTsSeenSoFar(Set<Long> batch, Optional<PatchyCache> maybeSnapshot) {
        long latestTsSeenSoFar = batch.stream().max(Comparator.naturalOrder()).orElse(0L);

        if (maybeSnapshot.isPresent()) {
            latestTsSeenSoFar = Math.max(latestTsSeenSoFar, maybeSnapshot.get().lastKnownConcludedTimestamp);
        }

        long latestBucket = AbortedTimestampUtils.getBucket(latestTsSeenSoFar);
        // The purpose of this call is to refresh the knownConcluded store for current bucket if it is not up-to-date.
        // Do not remove this line without considering perf implications.
        knownConcludedTransactions.isKnownConcluded(
                AbortedTimestampUtils.getMaxTsInCurrentBucket(latestBucket),
                KnownConcludedTransactions.Consistency.REMOTE_READ);

        latestTsSeenSoFar =
                Math.max(latestTsSeenSoFar, knownConcludedTransactions.lastLocallyKnownConcludedTimestamp());
        return latestTsSeenSoFar;
    }

    private PatchyCache refreshPatchyCache(
            Optional<PatchyCache> maybeSnapshot, long latestTsSeenSoFar, long latestBucketSeenSoFar) {
        PatchyCache refreshedPatchyCache;

        if (maybeSnapshot.isEmpty() || maybeSnapshot.get().bucket < latestBucketSeenSoFar) {
            refreshedPatchyCache = loadPatchyBucket(latestTsSeenSoFar, latestBucketSeenSoFar);
        } else {
            // need to load a range of timestamps
            refreshedPatchyCache = extendPatchIfNeeded(maybeSnapshot.get(), latestTsSeenSoFar);
        }

        patchyCache = refreshedPatchyCache;
        return refreshedPatchyCache;
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    private PatchyCache extendPatchIfNeeded(PatchyCache snapshot, long latestTs) {
        long currentLastKnownConcluded = snapshot.lastKnownConcludedTimestamp;

        // It is possible that cache was updated by the time we reach here and refresh is not required any more.
        if (latestTs <= currentLastKnownConcluded) {
            return snapshot;
        }

        Set<Long> newAbortedTransactions =
                futileTimestampStore.getAbortedTransactionsInRange(currentLastKnownConcluded, latestTs);
        snapshot.extend(latestTs, newAbortedTransactions);
        return snapshot;
    }

    private PatchyCache loadPatchyBucket(long latestTsSeenSoFar, long latestBucketSeenSoFar) {
        long maxTsInCurrentBucket = AbortedTimestampUtils.getMaxTsInCurrentBucket(latestBucketSeenSoFar);

        Set<Long> futileTimestamps = futileTimestampStore.getAbortedTransactionsInRange(
                AbortedTimestampUtils.getMinTsInBucket(latestBucketSeenSoFar), maxTsInCurrentBucket);

        return new PatchyCache(Math.min(latestTsSeenSoFar, maxTsInCurrentBucket), futileTimestamps);
    }

    private Optional<PatchyCache> getSnapshot() {
        return Optional.ofNullable(patchyCache);
    }

    private TransactionSoftCacheStatus getStatus(long startTimestamp, Set<Long> abortedTransactions) {
        return abortedTransactions.contains(startTimestamp)
                ? TransactionSoftCacheStatus.IS_ABORTED
                : TransactionSoftCacheStatus.IS_NOT_ABORTED;
    }

    /**
     * Maintains the cache for the mutable bucket. The cache is incomplete and can reliably serve query in
     * range [minTsForBucket(bucket), lastKnownConcludedTs].
     * For performance, we maintain a mutable instance of PatchyCache that can be extended if we have a query beyond
     * lastKnownConcludedTs.
     * */
    static class PatchyCache {
        private final long bucket;
        private final Set<Long> abortedTransactions;
        private long lastKnownConcludedTimestamp;

        PatchyCache(long lastKnownConcludedTimestamp, Set<Long> abortedTransactions) {
            Set<Long> mutableAbortedTimestamps = ConcurrentHashMap.newKeySet();
            mutableAbortedTimestamps.addAll(abortedTransactions);

            this.lastKnownConcludedTimestamp = lastKnownConcludedTimestamp;
            this.abortedTransactions = mutableAbortedTimestamps;
            this.bucket = AbortedTimestampUtils.getBucket(lastKnownConcludedTimestamp);
        }

        public void extend(long latestConcluded, Set<Long> newAbortedTransactions) {
            Preconditions.checkState(
                    AbortedTimestampUtils.getBucket(latestConcluded) == bucket,
                    "Can only extend within the same bucket.");
            abortedTransactions.addAll(newAbortedTransactions);
            lastKnownConcludedTimestamp = Math.max(lastKnownConcludedTimestamp, latestConcluded);
        }
    }
}
