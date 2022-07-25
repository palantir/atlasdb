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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.knowledge.AbortTransactionsSoftCache.TransactionSoftCacheStatus;
import org.junit.Before;
import org.junit.Test;

public class AbortTransactionsSoftCacheTest {
    private final FutileTimestampStore futileTimestampStore = mock(FutileTimestampStore.class);
    private final KnownConcludedTransactions knownConcludedTransactions = mock(KnownConcludedTransactions.class);
    private final AbortTransactionsSoftCache abortTransactionsSoftCache =
            new AbortTransactionsSoftCache(futileTimestampStore, knownConcludedTransactions);

    @Before
    public void before() {
        when(knownConcludedTransactions.isKnownConcluded(anyLong(), any())).thenReturn(true);
        // defaulting to no aborted transactions.
        when(futileTimestampStore.getAbortedTransactionsInRange(anyLong(), anyLong()))
                .thenReturn(ImmutableSet.of());
    }

    @Test
    public void initializesCacheLazily() {
        long timestamp = 25L;
        long bucket = Utils.getBucket(timestamp);
        long maxTsInCurrentBucket = Utils.getMaxTsInCurrentBucket(bucket);

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(maxTsInCurrentBucket);

        AbortTransactionsSoftCache abortTransactionsSoftCache =
                new AbortTransactionsSoftCache(futileTimestampStore, knownConcludedTransactions);
        // no remote calls upon init
        verify(futileTimestampStore, times(0)).getAbortedTransactionsInRange(anyLong(), anyLong());

        // init happens only on query
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(timestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(anyLong(), anyLong());
    }

    @Test
    public void servesRequestFromCacheIfAlreadyLoaded() {
        long timestamp = 25L;
        long maxTsInCurrentBucket = Utils.getMaxTsInCurrentBucket(Utils.getBucket(timestamp));

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(maxTsInCurrentBucket);

        // first query will init the cache for bucket
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(timestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(anyLong(), anyLong());

        // relies on soft cache to server request
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(timestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void extendsCacheIfBucketInCacheIsIncomplete() {
        long firstQueryTimestamp = 25L;
        long firstIterLastConcluded = firstQueryTimestamp + 1;
        long secondQueryTimestamp = firstQueryTimestamp + 2;

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(firstIterLastConcluded);

        // first query will init the cache for bucket until firstQueryTimestamp + 1
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(firstQueryTimestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(anyLong(), anyLong());

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(secondQueryTimestamp);
        // second query will extend the cache
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(secondQueryTimestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(firstIterLastConcluded, secondQueryTimestamp);
    }

    @Test
    public void extendsCacheToMaxPossibleIfBucketInCacheIsIncomplete() {
        long firstQueryTimestamp = 25L;
        long firstIterLastConcluded = firstQueryTimestamp + 1;
        long secondQueryTimestamp = firstQueryTimestamp + 2;

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(firstIterLastConcluded);

        // first query will init the cache for bucket until firstQueryTimestamp + 1
        abortTransactionsSoftCache.getSoftCacheTransactionStatus(firstQueryTimestamp);

        long maxConcluded = Utils.getMaxTsInCurrentBucket(Utils.getBucket(firstQueryTimestamp));
        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(maxConcluded);

        // second query will extend the cache
        abortTransactionsSoftCache.getSoftCacheTransactionStatus(secondQueryTimestamp);
        verify(futileTimestampStore).getAbortedTransactionsInRange(firstIterLastConcluded, maxConcluded);
    }

    @Test
    public void loadsLatestBucketIfBucketInCacheIsOld() {
        long bucket1 = 0;
        long bucket2 = 1;
        long tsInBucket1 = getTsInBucket(bucket1);
        long tsInBucket2 = getTsInBucket(bucket2);

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(tsInBucket1);

        // first query will init for bucket 1
        abortTransactionsSoftCache.getSoftCacheTransactionStatus(tsInBucket1);
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(Utils.getMinTsInBucket(bucket1), Utils.getMaxTsInCurrentBucket(bucket1));

        when(knownConcludedTransactions.lastKnownConcludedTimestamp())
                .thenReturn(Utils.getMaxTsInCurrentBucket(bucket1));

        // second query will load the cache for new bucket
        abortTransactionsSoftCache.getSoftCacheTransactionStatus(tsInBucket2);
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(Utils.getMinTsInBucket(bucket2), Utils.getMaxTsInCurrentBucket(bucket2));

        // subsequent requests can be served by cache
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(tsInBucket2))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void statusLoadFromReliableIfBucketIsOld() {
        long tsInBucket1 = 25L;
        long tsInBucket2 = AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE + 25L;

        when(knownConcludedTransactions.lastKnownConcludedTimestamp()).thenReturn(tsInBucket2);

        // cache second bucket
        abortTransactionsSoftCache.getSoftCacheTransactionStatus(tsInBucket2);
        assertThat(abortTransactionsSoftCache.getSoftCacheTransactionStatus(tsInBucket1))
                .isEqualTo(TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);
    }

    private long getTsInBucket(long bucket) {
        return AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE * bucket + 25L;
    }
}
