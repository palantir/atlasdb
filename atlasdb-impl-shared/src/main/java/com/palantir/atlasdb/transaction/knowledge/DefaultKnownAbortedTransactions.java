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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.palantir.atlasdb.transaction.knowledge.cache.AbortTransactionsSoftCache;
import com.palantir.atlasdb.transaction.knowledge.cache.AbortTransactionsSoftCache.TransactionSoftCacheStatus;
import java.util.Set;
import org.checkerframework.checker.index.qual.NonNegative;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final FutileTimestampStore futileTimestampStore;
    /**
     * This cache is only meant for timestamp ranges (inclusive) which are known to be concluded.
     */
    private final Cache<Long, Set<Long>> reliableCache;

    private final AbortTransactionsSoftCache softCache;

    @VisibleForTesting
    DefaultKnownAbortedTransactions(FutileTimestampStore futileTimestampStore, AbortTransactionsSoftCache softCache) {
        this.futileTimestampStore = futileTimestampStore;
        this.reliableCache = Caffeine.newBuilder()
                .maximumWeight(100_000)
                .weigher(new AbortedTransactionBucketWeigher())
                .build();
        this.softCache = softCache;
    }

    public static DefaultKnownAbortedTransactions create(
            KnownConcludedTransactions knownConcludedTransactions, FutileTimestampStore futileTimestampStore) {
        AbortTransactionsSoftCache softCache = new AbortTransactionsSoftCache(futileTimestampStore, knownConcludedTransactions);
        return new DefaultKnownAbortedTransactions(futileTimestampStore, softCache);
    }

    @Override
    public boolean isKnownAborted(long startTimestamp) {
        long bucketForTimestamp = Utils.getBucket(startTimestamp);
        TransactionSoftCacheStatus softCacheTransactionStatus = softCache.getSoftCacheTransactionStatus(startTimestamp);

        switch (softCacheTransactionStatus) {
            case IS_ABORTED:
                return true;
            case IS_NOT_ABORTED:
                return false;
            case PENDING_LOAD_FROM_RELIABLE:
                return getCachedAbortedTimestampsInBucket(bucketForTimestamp).contains(startTimestamp);
            default:
                throw new IllegalStateException("Unrecognized transaction status returned from soft cache.");
        }
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        futileTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    private Set<Long> getCachedAbortedTimestampsInBucket(long bucket) {
        return reliableCache.get(bucket, this::getAbortedTransactionsRemote);
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        Range<Long> timestampRangeClosed = Utils.getInclusiveRangeForBucket(bucket);
        return futileTimestampStore.getAbortedTransactionsInRange(
                timestampRangeClosed.lowerEndpoint(), timestampRangeClosed.upperEndpoint());
    }

    private static final class AbortedTransactionBucketWeigher implements Weigher<Long, Set<Long>> {
        @Override
        public @NonNegative int weigh(Long key, Set<Long> value) {
            return value.size();
        }
    }
}
