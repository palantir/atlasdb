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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.knowledge.AbandonedTransactionSoftCache.TransactionSoftCacheStatus;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;

public final class KnownAbortedTransactionsImplTest {
    private final AbandonedTimestampStore abandonedTimestampStore = mock(AbandonedTimestampStore.class);
    private final AbandonedTransactionSoftCache softCache = mock(AbandonedTransactionSoftCache.class);
    private final KnownAbortedTransactionsImpl knownAbortedTransactions = new KnownAbortedTransactionsImpl(
            abandonedTimestampStore,
            softCache,
            new DefaultTaggedMetricRegistry(),
            KnownAbortedTransactionsImpl.MAXIMUM_CACHE_WEIGHT);

    @Test
    public void testIsKnownAbortedReturnsTrueIfAbortedInSoftCache() {
        long abortedTimestamp = 27L;
        when(softCache.getSoftCacheTransactionStatus(abortedTimestamp))
                .thenReturn(TransactionSoftCacheStatus.IS_ABORTED);

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isTrue();
        verifyNoMoreInteractions(abandonedTimestampStore);
    }

    @Test
    public void testIsKnownAbortedReturnsFalseIfNotAbortedInSoftCache() {
        long abortedTimestamp = 27L;
        when(softCache.getSoftCacheTransactionStatus(abortedTimestamp))
                .thenReturn(TransactionSoftCacheStatus.IS_NOT_ABORTED);

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isFalse();
        verifyNoMoreInteractions(abandonedTimestampStore);
    }

    @Test
    public void testIsKnownAbortedLoadsFromReliableCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong()))
                .thenReturn(AbandonedTransactionSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long abortedTimestamp = 27L;
        Bucket bucket = Bucket.forTimestamp(abortedTimestamp);
        Set<Long> abortedTimestamps = ImmutableSet.of(abortedTimestamp);

        when(abandonedTimestampStore.getAbandonedTimestampsInRange(anyLong(), anyLong()))
                .thenReturn(abortedTimestamps);

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isTrue();
        verify(abandonedTimestampStore)
                .getAbandonedTimestampsInRange(bucket.getMinTsInBucket(), bucket.getMaxTsInCurrentBucket());

        // a second call will load state from the cache
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp + 1))
                .isFalse();
        verifyNoMoreInteractions(abandonedTimestampStore);
    }

    @Test
    public void testIsKnownAbortedLoadsFromRemoteIfBucketNotInReliableCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong()))
                .thenReturn(AbandonedTransactionSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long abortedTimestampBucket1 = 27L;
        Bucket bucket1 = Bucket.forTimestamp(abortedTimestampBucket1);

        long abortedTimestampBucket2 = AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE + 27L;
        Bucket bucket2 = Bucket.forTimestamp(abortedTimestampBucket2);

        when(abandonedTimestampStore.getAbandonedTimestampsInRange(anyLong(), anyLong()))
                .thenReturn(ImmutableSet.of(abortedTimestampBucket1))
                .thenReturn(ImmutableSet.of(abortedTimestampBucket2));

        // First call for bucket1 loads from remote
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket1))
                .isTrue();
        verify(abandonedTimestampStore)
                .getAbandonedTimestampsInRange(bucket1.getMinTsInBucket(), bucket1.getMaxTsInCurrentBucket());

        // First call for bucket2 loads from remote
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket2))
                .isTrue();
        verify(abandonedTimestampStore)
                .getAbandonedTimestampsInRange(bucket2.getMinTsInBucket(), bucket2.getMaxTsInCurrentBucket());

        // a second call will load state from the cache
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket1 + 1))
                .isFalse();
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket2 + 1))
                .isFalse();
        verifyNoMoreInteractions(abandonedTimestampStore);
    }

    @Test
    public void testReliableCacheEvictsIfWeightLimitReached() {
        when(softCache.getSoftCacheTransactionStatus(anyLong()))
                .thenReturn(AbandonedTransactionSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long numAbortedTimestampsInBucket = Math.min(
                AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE, KnownAbortedTransactionsImpl.MAXIMUM_CACHE_WEIGHT);
        when(abandonedTimestampStore.getAbandonedTimestampsInRange(anyLong(), anyLong()))
                .thenAnswer(invocation -> {
                    long start = invocation.getArgument(0);
                    return LongStream.range(start, start + numAbortedTimestampsInBucket)
                            .boxed()
                            .collect(Collectors.toSet());
                });

        Bucket bucket = Bucket.ofIndex(1);
        Range<Long> rangeForBucket = Range.closed(bucket.getMinTsInBucket(), bucket.getMaxTsInCurrentBucket());

        // First query for bucket 1 goes to the store
        knownAbortedTransactions.isKnownAborted(rangeForBucket.lowerEndpoint());
        verify(abandonedTimestampStore)
                .getAbandonedTimestampsInRange(rangeForBucket.lowerEndpoint(), rangeForBucket.upperEndpoint());

        // Subsequent queries for bucket 1 are resolved from cache
        knownAbortedTransactions.isKnownAborted(rangeForBucket.lowerEndpoint());
        verifyNoMoreInteractions(abandonedTimestampStore);

        Bucket bucket2 = Bucket.ofIndex(2);
        // caching a second bucket will cross the threshold weight of cache, marking first bucket for eviction
        knownAbortedTransactions.isKnownAborted(bucket2.getMinTsInBucket());

        knownAbortedTransactions.cleanup();

        // Now the query for bucket 1 will go to the futile store due to cache eviction
        knownAbortedTransactions.isKnownAborted(rangeForBucket.lowerEndpoint());
        verify(abandonedTimestampStore, times(2))
                .getAbandonedTimestampsInRange(rangeForBucket.lowerEndpoint(), rangeForBucket.upperEndpoint());
    }

    @Test
    public void testAddAbortedTransactionsDelegatesToFutileStore() {
        ImmutableSet<Long> abortedTimestamps = ImmutableSet.of(25L, 49L);
        knownAbortedTransactions.addAbortedTimestamps(abortedTimestamps);
        abortedTimestamps.forEach(ts -> verify(abandonedTimestampStore).markAbandoned(ts));
    }
}
