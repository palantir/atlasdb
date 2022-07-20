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
import java.util.Comparator;
import java.util.Set;
import org.checkerframework.checker.index.qual.NonNegative;

public class DefaultKnownAbortedTransactions implements KnownAbortedTransactions {
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final AbortedTimestampStore abortedTimestampStore;
    private final Cache<Long, Set<Long>> reliableCache;
    private final Cache<Long, Set<Long>> softCache;

    public DefaultKnownAbortedTransactions(
            KnownConcludedTransactions knownConcludedTransactions, AbortedTimestampStore abortedTimestampStore) {
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.abortedTimestampStore = abortedTimestampStore;
        this.reliableCache = Caffeine.newBuilder()
                .maximumWeight(100_000)
                .weigher(EntryWeigher.INSTANCE)
                .build();
        this.softCache = Caffeine.newBuilder()
                .maximumWeight(100_000)
                .weigher(EntryWeigher.INSTANCE)
                .build();
    }

    @Override
    public boolean isKnownAborted(long startTimestamp) {
        long bucketForTimestamp = getBucket(startTimestamp);
        boolean canBeReliablyCached = bucketCanBeReliablyCached(bucketForTimestamp);
        Set<Long> cachedAbortedTimestamps = getCachedAbortedTimestampsInBucket(bucketForTimestamp, canBeReliablyCached);

        if (cachedAbortedTimestamps.contains(startTimestamp)) {
            return true;
        }

        if (!canBeReliablyCached && shouldReloadBucket(startTimestamp, cachedAbortedTimestamps)) {
            Set<Long> abortedTransactionsRemote = getAbortedTransactionsRemote(bucketForTimestamp);
            softCache.put(bucketForTimestamp, abortedTransactionsRemote);
            return abortedTransactionsRemote.contains(startTimestamp);
        }

        return false;
    }

    private boolean shouldReloadBucket(long startTimestamp, Set<Long> cachedAbortedTimestamps) {
        long greatestCachedAbortedTs =
                cachedAbortedTimestamps.stream().max(Comparator.naturalOrder()).orElse(0L);
        return greatestCachedAbortedTs < startTimestamp;
    }

    @Override
    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {
        abortedTimestampStore.addAbortedTimestamps(abortedTimestamps);
    }

    private Set<Long> getCachedAbortedTimestampsInBucket(long bucket, boolean canBeReliablyCached) {
        if (canBeReliablyCached) {
            softCache.invalidate(bucket);
            reliableCache.get(bucket, this::getAbortedTransactionsRemote);
        }
        return softCache.get(bucket, this::getAbortedTransactionsRemote);
    }

    private boolean bucketCanBeReliablyCached(long bucket) {
        long maxTsInCurrentBucket = ((bucket + 1) * AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE) - 1;
        return !knownConcludedTransactions.isKnownConcluded(maxTsInCurrentBucket, Consistency.REMOTE_READ);
    }

    private Set<Long> getAbortedTransactionsRemote(long bucket) {
        return abortedTimestampStore.getBucket(bucket);
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
