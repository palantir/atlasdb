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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions.Consistency;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.checkerframework.checker.index.qual.NonNegative;
import org.immutables.value.Value;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final AbortedTimestampStore abortedTimestampStore;
    private final Cache<Long, Set<Long>> reliableCache;
    private final AtomicReference<SoftCache> softCacheRef;

    public DefaultKnownAbortedTransactions(
            KnownConcludedTransactions knownConcludedTransactions, AbortedTimestampStore abortedTimestampStore) {
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.abortedTimestampStore = abortedTimestampStore;
        this.reliableCache = cache();
        this.softCacheRef = new AtomicReference<>();
    }

    @Override
    public boolean isKnownAborted(long startTimestamp) {
        long bucketForTimestamp = getBucket(startTimestamp);

        // taking snapshot of the soft cache - this makes it easier to reason about rest of the logic in this class
        SoftCache cache = softCacheRef.get();

        boolean canBeReliablyCached = bucketCanBeReliablyCached(bucketForTimestamp);
        if (canBeReliablyCached) {
            Set<Long> cachedAbortedTimestamps = getCachedAbortedTimestampsInBucket(bucketForTimestamp);
            return cachedAbortedTimestamps.contains(startTimestamp);
        }

        Set<Long> softCachedAbortedTransactions = mayReloadAndGetSoftCache(startTimestamp, cache);
        return softCachedAbortedTransactions.contains(startTimestamp);
    }

    // Bucket rollover logic
    private Set<Long> mayReloadAndGetSoftCache(long startTimestamp, SoftCache cache) {
        long lastKnownConcludedCached = cache.lastKnownConcludedTimestamp();

        // Flushing of data from soft cache will only happen when a bucket is reliable
        // since the snapshot was taken before doing the bucket cache reliability check, the soft cache should
        // contain data of the non-reliable bucket. If then, the lastKnownConcludedCached in the funny bucket is
        // greater that startTimestamp do not actually need to load from remote.
        if (lastKnownConcludedCached > startTimestamp) {
            return cache.abortedTransactions();
        }

        long bucketForStartTs = getBucket(startTimestamp);
        // this will be the highest timestamp seen so far, no matter what bucket startTs points to.
        long newLastKnownConcluded = knownConcludedTransactions.lastKnownConcludedTimestamp();

        // we check once again if this bucket can be reliably cached
        if (bucketForStartTs < getBucket(newLastKnownConcluded)) {
            return getCachedAbortedTimestampsInBucket(bucketForStartTs);
        }

        Set<Long> abortedTransactions;

        // different buckets but startTs is the new bucket
        // getBucket(lastKnownConcludedCached) can be cached then
        if (getBucket(lastKnownConcludedCached) < bucketForStartTs) {
            // should load the entire newest bucket and discard the soft cache (since the soft cache
            // might not be up-to-date)
            abortedTransactions = abortedTimestampStore.getBucket(bucketForStartTs);

            // we are limiting newLastKnownConcluded due to the api above
            newLastKnownConcluded = Math.min(newLastKnownConcluded, getMaxTsInCurrentBucket(bucketForStartTs));
        } else {
            // equal bucket but startTs is greater
            // can to range get and append
            Set<Long> newAbortedTransactions = abortedTimestampStore.getAbortedTransactionsInRange(
                    lastKnownConcludedCached, newLastKnownConcluded);

            abortedTransactions = ImmutableSet.<Long>builder()
                    .addAll(cache.abortedTransactions())
                    .addAll(newAbortedTransactions)
                    .build();
        }

        tryUpdateSoftCache(ImmutableSoftCache.of(newLastKnownConcluded, abortedTransactions));

        return abortedTransactions;
    }

    private void tryUpdateSoftCache(SoftCache newSoftCacheValue) {
        // we try to update the cache iff the already cached last known concluded is less that of newSoftCacheValue
        SoftCache currentCache = softCacheRef.get();

        if (currentCache.lastKnownConcludedTimestamp() > newSoftCacheValue.lastKnownConcludedTimestamp()) {
            return;
        }

        softCacheRef.compareAndSet(currentCache, newSoftCacheValue);
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        abortedTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    private Set<Long> getCachedAbortedTimestampsInBucket(long bucket) {
        return reliableCache.get(bucket, this::getAbortedTransactionsRemote);
    }

    private boolean bucketCanBeReliablyCached(long bucket) {
        long maxTsInCurrentBucket = getMaxTsInCurrentBucket(bucket);
        return knownConcludedTransactions.isKnownConcluded(maxTsInCurrentBucket, Consistency.REMOTE_READ);
    }

    private long getMaxTsInCurrentBucket(long bucket) {
        return ((bucket + 1) * AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE) - 1;
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        return abortedTimestampStore.getBucket(bucket);
    }

    private long getBucket(long startTimestamp) {
        return startTimestamp / AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE;
    }

    private static Cache<Long, Set<Long>> cache() {
        return Caffeine.newBuilder()
                .maximumWeight(100_000)
                .weigher(new AbortedTransactionBucketWeigher())
                .build();
    }

    private static final class AbortedTransactionBucketWeigher implements Weigher<Long, Set<Long>> {
        @Override
        public @NonNegative int weigh(Long key, Set<Long> value) {
            return value.size();
        }
    }

    @Value.Immutable
    interface SoftCache {
        @Value.Parameter
        long lastKnownConcludedTimestamp();

        @Value.Parameter
        Set<Long> abortedTransactions();
    }
}
