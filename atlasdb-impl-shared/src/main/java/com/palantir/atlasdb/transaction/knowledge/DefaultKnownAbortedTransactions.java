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
import com.github.benmanes.caffeine.cache.Expiry;
import com.palantir.atlasdb.AtlasDbConstants;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final KnownConcludedTransactionsStore concludedTransactionsStore;
    private final AbortedTimestampStore abortedTimestampStore;
    private final Cache<Long, Set<Long>> cache;

    public DefaultKnownAbortedTransactions(
            KnownConcludedTransactionsStore concludedTransactionsStore,
            AbortedTimestampStore abortedTimestampStore) {
        this.concludedTransactionsStore = concludedTransactionsStore;
        this.abortedTimestampStore = abortedTimestampStore;
        this.cache = Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfter(new Expiry<Long, Set<Long>>() {
                @Override
                public long expireAfterCreate(Long key, Set<Long> value, long currentTime) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long expireAfterUpdate(
                        Long key, Set<Long> value, long currentTime, long currentDuration) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long expireAfterRead(
                        Long key, Set<Long> value, long currentTime, long currentDuration) {
                    // todo(snanda): expiration should be weighted depending on the size of bucket
                    return Duration.ofSeconds(value.size()).toNanos();
                }
            })
            .build();
    }

    /**
     * This method should only be called for concluded transactions.
     * */
    @Override
    public boolean isKnownAborted(long startTimestamp) {
        long bucketForTimestamp = getBucket(startTimestamp);
        // todo(snanda): Only cache buckets that are older than the latest one. Though, this is another read on every
        //  call.
        long latestBucket = getBucket(concludedTransactionsStore.lastKnownConcludedTransaction());
        Set<Long> abortedTimestampsInBucket = getAbortedTimestampsInBucket(bucketForTimestamp, latestBucket);
        return abortedTimestampsInBucket.contains(startTimestamp);
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        invalidateBuckets(abortedTimestamps);
        abortedTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    private void invalidateBuckets(Set<Long> abortedTimestamps) {
        Set<Long> invalidatedBuckets = abortedTimestamps.stream().map(this::getBucket).collect(Collectors.toSet());
        cache.invalidateAll(invalidatedBuckets);
    }

    private Set<Long> getAbortedTimestampsInBucket(long bucket, long latestBucket) {
        if (bucket < latestBucket) {
            return cache.get(bucket, this::getAbortedTransactionsRemote);
        }
        return getAbortedTransactionsRemote(bucket);
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        return abortedTimestampStore.getBucket(bucket);
    }

    private long getBucket(long startTimestamp) {
        return startTimestamp / AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE;
    }
}
