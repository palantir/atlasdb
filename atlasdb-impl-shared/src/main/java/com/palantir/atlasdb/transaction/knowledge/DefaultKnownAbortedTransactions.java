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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions.Consistency;
import java.util.Set;
import java.util.stream.Collectors;
import org.checkerframework.checker.index.qual.NonNegative;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final AbortedTimestampStore abortedTimestampStore;
    private final Cache<Long, Set<Long>> cache;

    public DefaultKnownAbortedTransactions(
            KnownConcludedTransactions knownConcludedTransactions, AbortedTimestampStore abortedTimestampStore) {
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.abortedTimestampStore = abortedTimestampStore;
        this.cache = Caffeine.newBuilder()
                .maximumWeight(100_000)
                .weigher(EntryWeigher.INSTANCE)
                .build();
    }

    /**
     * This method should only be called for concluded transactions.
     * */
    @Override
    public boolean isKnownAborted(long startTimestamp, Consistency consistency) {
        long bucketForTimestamp = getBucket(startTimestamp);
        Set<Long> cachedAbortedTimestamps = getCachedAbortedTimestampsInBucket(bucketForTimestamp);
        if (cachedAbortedTimestamps.contains(startTimestamp)) {
            return true;
        }

        // Try remote fetch if current aborted ts bucket is not immutable.
        if (isBucketMutable(bucketForTimestamp, consistency)) {
            return getAbortedTransactionsRemote(bucketForTimestamp).contains(startTimestamp);
        }

        return false;
    }

    private boolean isBucketMutable(long bucketForTimestamp, Consistency consistency) {
        // using approximation here
        long maxTsInCurrentBucket = ((bucketForTimestamp + 1) * AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE) - 1;
        return !knownConcludedTransactions.isKnownConcluded(maxTsInCurrentBucket, consistency);
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        invalidateBuckets(abortedTimestamps);
        abortedTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    private void invalidateBuckets(Set<Long> abortedTimestamps) {
        Set<Long> invalidatedBuckets =
                abortedTimestamps.stream().map(this::getBucket).collect(Collectors.toSet());
        cache.invalidateAll(invalidatedBuckets);
    }

    private Set<Long> getCachedAbortedTimestampsInBucket(long bucket) {
        return cache.get(bucket, this::getAbortedTransactionsRemote);
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        Set<Long> abortedTs = abortedTimestampStore.getBucket(bucket);
        cache.put(bucket, abortedTs);
        return abortedTs;
    }

    private long getBucket(long startTimestamp) {
        return startTimestamp / AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE;
    }

    enum EntryWeigher implements Weigher<Long, Set<Long>> {
        INSTANCE;

        @Override
        public @NonNegative int weigh(Long key, Set<Long> value) {
            return value.size();
        }
    }
}
