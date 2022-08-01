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
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.knowledge.AbortedTransactionSoftCache.TransactionSoftCacheStatus;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;

public final class DefaultKnownAbortedTransactionsTest {
    private final FutileTimestampStore futileTimestampStore = mock(FutileTimestampStore.class);
    private final AbortedTransactionSoftCache softCache = mock(AbortedTransactionSoftCache.class);
    private final DefaultKnownAbortedTransactions knownAbortedTransactions =
            new DefaultKnownAbortedTransactions(futileTimestampStore, softCache, new DefaultTaggedMetricRegistry());

    @Test
    public void testIsKnownAbortedReturnsTrueIfAbortedInSoftCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong())).thenReturn(TransactionSoftCacheStatus.IS_ABORTED);
        long abortedTimestamp = 27L;

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isTrue();
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void testIsKnownAbortedReturnsFalseIfNotAbortedInSoftCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong())).thenReturn(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        long abortedTimestamp = 27L;

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isFalse();
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void testIsKnownAbortedLoadsFromReliableCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong()))
                .thenReturn(AbortedTransactionSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long abortedTimestamp = 27L;
        long bucket = Utils.getBucket(abortedTimestamp);
        Set<Long> abortedTimestamps = ImmutableSet.of(abortedTimestamp);

        when(futileTimestampStore.getAbortedTransactionsInRange(anyLong(), anyLong()))
                .thenReturn(abortedTimestamps);

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isTrue();
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(Utils.getMinTsInBucket(bucket), Utils.getMaxTsInCurrentBucket(bucket));

        // a second call will load state from the cache
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp + 1))
                .isFalse();
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void testIsKnownAbortedLoadsFromRemoteIfBucketNotInReliableCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong()))
                .thenReturn(AbortedTransactionSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long abortedTimestampBucket1 = 27L;
        long bucket1 = Utils.getBucket(abortedTimestampBucket1);

        long abortedTimestampBucket2 = AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE + 27L;
        long bucket2 = Utils.getBucket(abortedTimestampBucket2);

        when(futileTimestampStore.getAbortedTransactionsInRange(anyLong(), anyLong()))
                .thenReturn(ImmutableSet.of(abortedTimestampBucket1))
                .thenReturn(ImmutableSet.of(abortedTimestampBucket2));

        // First call for bucket1 loads from remote
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket1))
                .isTrue();
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(Utils.getMinTsInBucket(bucket1), Utils.getMaxTsInCurrentBucket(bucket1));

        // First call for bucket2 loads from remote
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket2))
                .isTrue();
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(Utils.getMinTsInBucket(bucket2), Utils.getMaxTsInCurrentBucket(bucket2));

        // a second call will load state from the cache
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket1 + 1))
                .isFalse();
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestampBucket2 + 1))
                .isFalse();
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void testReliableCacheEvictsIfWeightLimitReached() {
        when(softCache.getSoftCacheTransactionStatus(anyLong()))
                .thenReturn(AbortedTransactionSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long numAbortedTimestampsInBucket = Math.min(
                AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE, DefaultKnownAbortedTransactions.MAXIMUM_CACHE_WEIGHT);
        when(futileTimestampStore.getAbortedTransactionsInRange(anyLong(), anyLong()))
                .thenAnswer(invocation -> {
                    long start = invocation.getArgument(0);
                    return LongStream.range(start, start + numAbortedTimestampsInBucket)
                            .boxed()
                            .collect(Collectors.toSet());
                });

        long bucket = 1;
        Range<Long> rangeForBucket =
                Range.closed(Utils.getMinTsInBucket(bucket), Utils.getMaxTsInCurrentBucket(bucket));

        // First query for bucket 1 goes to the store
        knownAbortedTransactions.isKnownAborted(rangeForBucket.lowerEndpoint());
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(rangeForBucket.lowerEndpoint(), rangeForBucket.upperEndpoint());

        // Subsequent queries for bucket 1 are resolved from cache
        knownAbortedTransactions.isKnownAborted(rangeForBucket.lowerEndpoint());
        verifyNoMoreInteractions(futileTimestampStore);

        long bucket2 = 2;
        // caching a second bucket will cross the threshold weight of cache, marking first bucket for eviction
        knownAbortedTransactions.isKnownAborted(Utils.getMinTsInBucket(bucket2));

        knownAbortedTransactions.cleanup();

        // Now the query for bucket 1 will go to the futile store due to cache eviction
        knownAbortedTransactions.isKnownAborted(rangeForBucket.lowerEndpoint());
        verify(futileTimestampStore, times(2))
                .getAbortedTransactionsInRange(rangeForBucket.lowerEndpoint(), rangeForBucket.upperEndpoint());
    }

    @Test
    public void testAddAbortedTransactionsDelegatesToFutileStore() {
        ImmutableSet<Long> abortedTimestamps = ImmutableSet.of(25L, 49L);
        knownAbortedTransactions.addAbortedTimestamps(abortedTimestamps);

        verify(futileTimestampStore).addAbortedTimestamps(abortedTimestamps);
    }
}
