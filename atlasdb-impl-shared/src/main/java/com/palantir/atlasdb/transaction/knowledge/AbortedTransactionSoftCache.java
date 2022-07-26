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
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public final class AbortedTransactionSoftCache implements AutoCloseable {
    public enum TransactionSoftCacheStatus {
        PENDING_LOAD_FROM_RELIABLE,
        IS_ABORTED,
        IS_NOT_ABORTED;
    }

    private final DisruptorAutobatcher<Long, TransactionSoftCacheStatus> autobatcher;

    private final AtomicReference<PatchyCache> patchyCacheRef;
    private final FutileTimestampStore futileTimestampStore;
    private final KnownConcludedTransactions knownConcludedTransactions;

    public AbortedTransactionSoftCache(
            FutileTimestampStore futileTimestampStore, KnownConcludedTransactions knownConcludedTransactions) {
        this.futileTimestampStore = futileTimestampStore;
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.patchyCacheRef = new AtomicReference<>();
        this.autobatcher = Autobatchers.independent(this::consumer)
                .safeLoggablePurpose("get-transaction-soft-cache-status")
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .build();
    }

    public TransactionSoftCacheStatus getSoftCacheTransactionStatus(long startTimestamp) {
        return AtlasFutures.getUnchecked(autobatcher.apply(startTimestamp));
    }

    private void consumer(List<BatchElement<Long, TransactionSoftCacheStatus>> batch) {
        Optional<PatchyCache> maybeSnapshot = getSnapshot();

        long latestTsSeenSoFar = getLatestTsSeenSoFar(batch, maybeSnapshot);
        long latestBucketSeenSoFar = Utils.getBucket(latestTsSeenSoFar);

        PatchyCache refreshedPatchyCache = refreshPatchyCache(maybeSnapshot, latestTsSeenSoFar, latestBucketSeenSoFar);

        for (BatchElement<Long, TransactionSoftCacheStatus> elem : batch) {
            long startTimestamp = elem.argument();
            long requestedBucket = Utils.getBucket(startTimestamp);

            if (requestedBucket < latestBucketSeenSoFar) {
                elem.result().set(TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);
            } else {
                elem.result().set(getStatus(startTimestamp, refreshedPatchyCache.abortedTransactions));
            }
        }
    }

    private long getLatestTsSeenSoFar(
            List<BatchElement<Long, TransactionSoftCacheStatus>> batch, Optional<PatchyCache> maybeSnapshot) {
        long latestTsSeenSoFar =
                batch.stream().mapToLong(BatchElement::argument).max().orElse(0L);

        if (maybeSnapshot.isPresent()) {
            latestTsSeenSoFar = Math.max(latestTsSeenSoFar, maybeSnapshot.get().lastKnownConcludedTimestamp);
        }

        long latestBucket = Utils.getBucket(latestTsSeenSoFar);
        // The purpose of this call is to refresh the knownConcluded store for current bucket if it is not up-to-date.
        // Do not remove this line without considering perf implications.
        knownConcludedTransactions.isKnownConcluded(latestBucket, KnownConcludedTransactions.Consistency.REMOTE_READ);

        latestTsSeenSoFar = Math.max(latestTsSeenSoFar, knownConcludedTransactions.lastKnownConcludedTimestamp());
        return latestTsSeenSoFar;
    }

    private PatchyCache refreshPatchyCache(
            Optional<PatchyCache> maybeSnapshot, long latestTsSeenSoFar, long latestBucketSeenSoFar) {
        PatchyCache refreshedPatchyCache;

        if (maybeSnapshot.isEmpty() || maybeSnapshot.get().bucket < latestBucketSeenSoFar) {
            refreshedPatchyCache = loadPatchyBucket(latestTsSeenSoFar, latestBucketSeenSoFar);
        } else {
            // need to load a range of timestamps
            refreshedPatchyCache = extendPatch(maybeSnapshot.get(), latestTsSeenSoFar);
        }

        tryUpdate(refreshedPatchyCache);
        return refreshedPatchyCache;
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    private PatchyCache extendPatch(PatchyCache snapshot, long latestTs) {
        long currentLastKnownConcluded = snapshot.lastKnownConcludedTimestamp;

        // It is possible that cache was update by the time we reach here and refresh is not required any more.
        if (latestTs <= currentLastKnownConcluded) {
            return snapshot;
        }

        Set<Long> newAbortedTransactions =
                futileTimestampStore.getAbortedTransactionsInRange(currentLastKnownConcluded, latestTs);
        snapshot.extend(latestTs, newAbortedTransactions);
        return snapshot;
    }

    private PatchyCache loadPatchyBucket(long latestTsSeenSoFar, long latestBucketSeenSoFar) {
        long maxTsInCurrentBucket = Utils.getMaxTsInCurrentBucket(latestBucketSeenSoFar);

        Set<Long> futileTimestamps = futileTimestampStore.getAbortedTransactionsInRange(
                Utils.getMinTsInBucket(latestBucketSeenSoFar), Utils.getMaxTsInCurrentBucket(latestBucketSeenSoFar));

        return new PatchyCache(Math.min(latestTsSeenSoFar, maxTsInCurrentBucket), futileTimestamps);
    }

    private Optional<PatchyCache> getSnapshot() {
        return Optional.ofNullable(patchyCacheRef.get());
    }

    private void tryUpdate(PatchyCache update) {
        patchyCacheRef.getAndAccumulate(update, AbortedTransactionSoftCache::getLatest);
    }

    private TransactionSoftCacheStatus getStatus(long startTimestamp, Set<Long> abortedTransactions) {
        return abortedTransactions.contains(startTimestamp)
                ? TransactionSoftCacheStatus.IS_ABORTED
                : TransactionSoftCacheStatus.IS_NOT_ABORTED;
    }

    static PatchyCache getLatest(PatchyCache current, PatchyCache update) {
        if (current == null) {
            return update;
        }

        if (current == update) {
            return current;
        }

        if (current.bucket == update.bucket) {
            return current.lastKnownConcludedTimestamp >= update.lastKnownConcludedTimestamp ? current : update;
        }

        return current.bucket > update.bucket ? current : update;
    }

    static class PatchyCache {
        private final long bucket;
        private final Set<Long> abortedTransactions;
        private long lastKnownConcludedTimestamp;

        PatchyCache(long lastKnownConcludedTimestamp, Set<Long> abortedTransactions) {
            Set<Long> mutableAbortedTimestamps = ConcurrentHashMap.newKeySet();
            mutableAbortedTimestamps.addAll(abortedTransactions);

            this.lastKnownConcludedTimestamp = lastKnownConcludedTimestamp;
            this.abortedTransactions = mutableAbortedTimestamps;
            this.bucket = Utils.getBucket(lastKnownConcludedTimestamp);
        }

        public void extend(long latestConcluded, Set<Long> newAbortedTransactions) {
            Preconditions.checkState(
                    Utils.getBucket(latestConcluded) == bucket, "Can only extend within the same bucket.");
            abortedTransactions.addAll(newAbortedTransactions);
            lastKnownConcludedTimestamp = Math.max(lastKnownConcludedTimestamp, latestConcluded);
        }
    }
}
