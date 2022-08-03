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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.knowledge.AbortedTransactionSoftCache.TransactionSoftCacheStatus;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions.Consistency;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.junit.Before;
import org.junit.Test;

public class AbortedTransactionSoftCacheTest {
    private final FutileTimestampStore futileTimestampStore = mock(FutileTimestampStore.class);
    private final KnownConcludedTransactions knownConcludedTransactions = mock(KnownConcludedTransactions.class);
    private final AbortedTransactionSoftCache abortedTransactionSoftCache =
            new AbortedTransactionSoftCache(futileTimestampStore, knownConcludedTransactions);

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
        Bucket bucket = Bucket.forTimestamp(timestamp);
        long maxTsInCurrentBucket = bucket.getMaxTsInCurrentBucket();

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(maxTsInCurrentBucket);

        AbortedTransactionSoftCache abortedTransactionSoftCache =
                new AbortedTransactionSoftCache(futileTimestampStore, knownConcludedTransactions);
        // no remote calls upon init
        verify(futileTimestampStore, times(0)).getAbortedTransactionsInRange(anyLong(), anyLong());

        // init happens only on query
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(timestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(bucket.getMinTsInBucket(), maxTsInCurrentBucket);
    }

    @Test
    public void callsKnownConcludedStoreWithRemoteConsistency() {
        long firstQueryTimestamp = 25L;
        long firstIterLastConcluded = firstQueryTimestamp + 1;
        Bucket bucket = Bucket.forTimestamp(firstQueryTimestamp);

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(firstIterLastConcluded);
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(firstQueryTimestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(knownConcludedTransactions)
                .isKnownConcluded(bucket.getMaxTsInCurrentBucket(), KnownConcludedTransactions.Consistency.REMOTE_READ);
    }

    @Test
    public void throwsForRequestWithInconcludedTransaction() {
        long firstQueryTimestamp = 25L;

        when(knownConcludedTransactions.isKnownConcluded(eq(firstQueryTimestamp), eq(Consistency.LOCAL_READ)))
                .thenReturn(false);
        assertThatThrownBy(() -> abortedTransactionSoftCache.getSoftCacheTransactionStatus(firstQueryTimestamp))
                .isInstanceOf(SafeIllegalStateException.class);
    }

    @Test
    public void servesRequestFromCacheIfAlreadyLoaded() {
        long timestamp = 25L;
        Bucket bucket = Bucket.forTimestamp(timestamp);
        long maxTsInCurrentBucket = bucket.getMaxTsInCurrentBucket();

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(maxTsInCurrentBucket);

        // first query will init the cache for bucket
        when(futileTimestampStore.getAbortedTransactionsInRange(anyLong(), anyLong()))
                .thenReturn(ImmutableSet.of(timestamp));
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(timestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(bucket.getMinTsInBucket(), maxTsInCurrentBucket);

        // relies on soft cache to server request
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(timestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_ABORTED);
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void extendsCacheIfBucketInCacheIsIncomplete() {
        long firstQueryTimestamp = 25L;
        long firstIterLastConcluded = firstQueryTimestamp + 1;
        long secondQueryTimestamp = firstQueryTimestamp + 2;

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(firstIterLastConcluded);

        // first query will init the cache for bucket until firstQueryTimestamp + 1
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(firstQueryTimestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(anyLong(), anyLong());

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(secondQueryTimestamp);
        // second query will extend the cache
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(secondQueryTimestamp))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verify(futileTimestampStore).getAbortedTransactionsInRange(firstIterLastConcluded, secondQueryTimestamp);
    }

    @Test
    public void extendsCacheToMaxPossibleIfBucketInCacheIsIncomplete() {
        long firstQueryTimestamp = 25L;
        long firstIterLastConcluded = firstQueryTimestamp + 1;
        long secondQueryTimestamp = firstQueryTimestamp + 2;

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(firstIterLastConcluded);

        // first query will init the cache for bucket until firstQueryTimestamp + 1
        abortedTransactionSoftCache.getSoftCacheTransactionStatus(firstQueryTimestamp);
        verify(futileTimestampStore).getAbortedTransactionsInRange(anyLong(), anyLong());

        long maxConcluded = Bucket.forTimestamp(firstQueryTimestamp).getMaxTsInCurrentBucket();

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(maxConcluded);

        // second query will extend the cache
        abortedTransactionSoftCache.getSoftCacheTransactionStatus(secondQueryTimestamp);
        verify(futileTimestampStore).getAbortedTransactionsInRange(firstIterLastConcluded, maxConcluded);

        // queries until maxConcluded will be served from cache
        abortedTransactionSoftCache.getSoftCacheTransactionStatus(maxConcluded);
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void loadsLatestBucketIfBucketInCacheIsOld() {
        Bucket bucket1 = Bucket.ofIndex(0);
        Bucket bucket2 = Bucket.ofIndex(1);
        long tsInBucket1 = getNotSoRandomTsInBucket(bucket1);
        long tsInBucket2 = getNotSoRandomTsInBucket(bucket2);

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(tsInBucket1);

        // first query will init for bucket 1
        abortedTransactionSoftCache.getSoftCacheTransactionStatus(tsInBucket1);
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(bucket1.getMinTsInBucket(), bucket1.getMaxTsInCurrentBucket());

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp())
                .thenReturn(bucket1.getMaxTsInCurrentBucket());

        // second query will load the cache for new bucket
        abortedTransactionSoftCache.getSoftCacheTransactionStatus(tsInBucket2);
        verify(futileTimestampStore)
                .getAbortedTransactionsInRange(bucket2.getMinTsInBucket(), bucket2.getMaxTsInCurrentBucket());

        // subsequent requests can be served by cache
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(tsInBucket2))
                .isEqualTo(TransactionSoftCacheStatus.IS_NOT_ABORTED);
        verifyNoMoreInteractions(futileTimestampStore);
    }

    @Test
    public void statusLoadFromReliableIfBucketIsOld() {
        long tsInBucket1 = 25L;
        long tsInBucket2 = AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE + 25L;

        when(knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()).thenReturn(tsInBucket2);

        // cache second bucket
        abortedTransactionSoftCache.getSoftCacheTransactionStatus(tsInBucket2);
        assertThat(abortedTransactionSoftCache.getSoftCacheTransactionStatus(tsInBucket1))
                .isEqualTo(TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);
    }

    private long getNotSoRandomTsInBucket(Bucket bucket) {
        return AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE * bucket.value() + 25L;
    }
}
