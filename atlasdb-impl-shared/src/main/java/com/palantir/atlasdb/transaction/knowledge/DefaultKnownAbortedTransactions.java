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
import com.palantir.atlasdb.transaction.knowledge.cache.AbortTransactionsSoftCache;
import com.palantir.atlasdb.transaction.knowledge.cache.AbortTransactionsSoftCache.TransactionSoftCacheStatus;
import java.util.Set;
import org.checkerframework.checker.index.qual.NonNegative;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final FutileTimestampStore futileTimestampStore;
    private final Cache<Long, Set<Long>> reliableCache;
    private final AbortTransactionsSoftCache softCache;

    public DefaultKnownAbortedTransactions(
            KnownConcludedTransactions knownConcludedTransactions, FutileTimestampStore futileTimestampStore) {
        this.futileTimestampStore = futileTimestampStore;
        this.reliableCache = cache();
        this.softCache = new AbortTransactionsSoftCache(futileTimestampStore, knownConcludedTransactions);
    }

    @Override
    public boolean isKnownAborted(long startTimestamp) {
        long bucketForTimestamp = Utils.getBucket(startTimestamp);
        TransactionSoftCacheStatus softCacheTransactionStatus = softCache.getSoftCacheTransactionStatus(startTimestamp);

        if (softCacheTransactionStatus.equals(TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE)) {
            Set<Long> cachedAbortedTimestamps = getCachedAbortedTimestampsInBucket(bucketForTimestamp);
            return cachedAbortedTimestamps.contains(startTimestamp);
        }

        return softCacheTransactionStatus.equals(TransactionSoftCacheStatus.IS_ABORTED);
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        futileTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    private Set<Long> getCachedAbortedTimestampsInBucket(long bucket) {
        return reliableCache.get(bucket, this::getAbortedTransactionsRemote);
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        return futileTimestampStore.getFutileTimestampsForBucket(bucket);
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
}
