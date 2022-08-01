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
import com.palantir.atlasdb.transaction.knowledge.AbortedTransactionSoftCache.TransactionSoftCacheStatus;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Set;
import org.checkerframework.checker.index.qual.NonNegative;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final FutileTimestampStore futileTimestampStore;

    @VisibleForTesting
    static final int MAXIMUM_CACHE_WEIGHT = 100_000;
    /**
     * This cache is only meant for timestamp ranges (inclusive) which are known to be concluded.
     */
    private final Cache<Long, Set<Long>> reliableCache;

    private final AbortedTransactionSoftCache softCache;
    private final AbortedTransctionsCacheMetrics metrics;

    @VisibleForTesting
    DefaultKnownAbortedTransactions(
            FutileTimestampStore futileTimestampStore,
            AbortedTransactionSoftCache softCache,
            TaggedMetricRegistry registry) {
        this.futileTimestampStore = futileTimestampStore;
        this.softCache = softCache;
        this.metrics = AbortedTransctionsCacheMetrics.of(registry);
        this.reliableCache = Caffeine.newBuilder()
                .maximumWeight(MAXIMUM_CACHE_WEIGHT)
                .weigher(new AbortedTransactionBucketWeigher())
                .evictionListener((k, v, cause) -> {
                    if (cause.wasEvicted()) {
                        metrics.reliableBucketEvictions().mark();
                    }
                })
                .build();
    }

    public static DefaultKnownAbortedTransactions create(
            KnownConcludedTransactions knownConcludedTransactions,
            FutileTimestampStore futileTimestampStore,
            TaggedMetricRegistry registry) {
        AbortedTransactionSoftCache softCache =
                new AbortedTransactionSoftCache(futileTimestampStore, knownConcludedTransactions);
        return new DefaultKnownAbortedTransactions(futileTimestampStore, softCache, registry);
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
                throw new SafeIllegalStateException(
                        "Unrecognized transaction status returned from soft cache.",
                        SafeArg.of("status", softCacheTransactionStatus));
        }
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        futileTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    @VisibleForTesting
    void cleanup() {
        reliableCache.cleanUp();
    }

    private Set<Long> getCachedAbortedTimestampsInBucket(long bucket) {
        return reliableCache.get(bucket, this::getAbortedTransactionsRemote);
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        metrics.abortedTxnCacheMiss().mark();
        return futileTimestampStore.getAbortedTransactionsInRange(
                Utils.getMinTsInBucket(bucket), Utils.getMaxTsInCurrentBucket(bucket));
    }

    private static final class AbortedTransactionBucketWeigher implements Weigher<Long, Set<Long>> {
        @Override
        public @NonNegative int weigh(Long key, Set<Long> value) {
            return value.size();
        }
    }
}
