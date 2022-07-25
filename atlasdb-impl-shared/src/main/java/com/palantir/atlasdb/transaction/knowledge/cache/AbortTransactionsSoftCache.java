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

package com.palantir.atlasdb.transaction.knowledge.cache;

import static com.palantir.atlasdb.transaction.knowledge.Utils.getMaxTsInCurrentBucket;

import com.google.common.collect.Range;
import com.palantir.atlasdb.transaction.knowledge.FutileTimestampStore;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.knowledge.Utils;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.immutables.value.Value;

public final class AbortTransactionsSoftCache {
    public enum TransactionSoftCacheStatus {
        PENDING_LOAD_FROM_RELIABLE,
        IS_ABORTED,
        IS_NOT_ABORTED
    }

    private final AtomicReference<PatchyCache> patchyCache;
    private final FutileTimestampStore futileTimestampStore;
    private final KnownConcludedTransactions knownConcludedTransactions;

    public AbortTransactionsSoftCache(
            FutileTimestampStore futileTimestampStore, KnownConcludedTransactions knownConcludedTransactions) {
        this.futileTimestampStore = futileTimestampStore;
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.patchyCache = new AtomicReference<>();
    }

    public TransactionSoftCacheStatus getSoftCacheTransactionStatus(long startTimestamp) {
        PatchyCache snapshot = getSnapshot();
        long bucketForStartTs = Utils.getBucket(startTimestamp);

        PatchyCache refreshedPatchyCache;

        if (bucketForStartTs < snapshot.bucket()) {
            // startTs is in an immutable bucket and thus, can be reliably loaded
            return TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE;
        } else if (bucketForStartTs == snapshot.bucket()) {
            if (startTimestamp <= snapshot.lastKnownConcludedTimestamp()) {
                return getStatus(startTimestamp, snapshot.abortedTransactions());
            } else {
                // need to load a range of timestamps
                refreshedPatchyCache = extendPatch(snapshot, startTimestamp);
            }
        } else {
            // patchyCache contains data of now reliable bucket and must be refreshed
            refreshedPatchyCache = loadPatchyBucket(bucketForStartTs);
        }

        tryUpdate(refreshedPatchyCache);

        // It does not matter if the soft cache ref is updated or not. The `refreshedPatchyCache` contains the correct
        // view to answer the query for startTimestamp.
        return getStatus(startTimestamp, refreshedPatchyCache.abortedTransactions());
    }

    private PatchyCache extendPatch(PatchyCache snapshot, long startTimestamp) {
        Set<Long> newAbortedTransactions = futileTimestampStore.getAbortedTransactionsInRange(
                snapshot.lastKnownConcludedTimestamp(), startTimestamp);

        Set<Long> abortedTransactionsSoFar = snapshot.abortedTransactions();
        abortedTransactionsSoFar.addAll(newAbortedTransactions);
        return ImmutablePatchyCache.of(startTimestamp, abortedTransactionsSoFar);
    }

    private PatchyCache loadPatchyBucket(long bucketForStartTs) {
        long maxTsInCurrentBucket = getMaxTsInCurrentBucket(bucketForStartTs);

        // The purpose of this call is to refresh the knownConcluded store for current bucket if it is not up-to-date.
        // Do not remove this line without considering perf implications.
        knownConcludedTransactions.isKnownConcluded(
                maxTsInCurrentBucket, KnownConcludedTransactions.Consistency.REMOTE_READ);

        long lastKnownConcludedTimestamp = knownConcludedTransactions.lastKnownConcludedTimestamp();

        Range<Long> rangeForBucket = Utils.getInclusiveRangeForBucket(bucketForStartTs);
        Set<Long> futileTimestamps = futileTimestampStore.getAbortedTransactionsInRange(
                rangeForBucket.lowerEndpoint(), rangeForBucket.upperEndpoint());

        return ImmutablePatchyCache.of(Math.min(lastKnownConcludedTimestamp, maxTsInCurrentBucket), futileTimestamps);
    }

    private PatchyCache getSnapshot() {
        return patchyCache.get();
    }

    private PatchyCache tryUpdate(PatchyCache update) {
        return patchyCache.getAndAccumulate(update, PatchyCache::getLatest);
    }

    private TransactionSoftCacheStatus getStatus(long startTimestamp, Set<Long> abortedTransactions) {
        return abortedTransactions.contains(startTimestamp)
                ? TransactionSoftCacheStatus.IS_ABORTED
                : TransactionSoftCacheStatus.IS_NOT_ABORTED;
    }

    @Value.Immutable
    interface PatchyCache {
        @Value.Parameter
        long lastKnownConcludedTimestamp();

        // This will be a mutable ConcurrentHashSet that supports append only operations
        @Value.Parameter
        Set<Long> abortedTransactions();

        @Value.Derived
        default long bucket() {
            return Utils.getBucket(lastKnownConcludedTimestamp());
        }

        @Value.Derived
        default long bucketUpperLimitInclusive() {
            return Utils.getMaxTsInCurrentBucket(bucket());
        }

        static PatchyCache of(long lastKnownConcludedTimestamp, Set<Long> abortedTransactions) {
            Set<Long> mutableAbortedTimestamps = ConcurrentHashMap.newKeySet();
            mutableAbortedTimestamps.addAll(abortedTransactions);
            return ImmutablePatchyCache.of(lastKnownConcludedTimestamp, mutableAbortedTimestamps);
        }

        static PatchyCache getLatest(PatchyCache current, PatchyCache update) {
            if (current == null) {
                return update;
            }

            if (current.bucket() == update.bucket()) {
                return current.lastKnownConcludedTimestamp() >= update.lastKnownConcludedTimestamp() ? current : update;
            }

            return current.bucket() > update.bucket() ? current : update;
        }
    }
}
