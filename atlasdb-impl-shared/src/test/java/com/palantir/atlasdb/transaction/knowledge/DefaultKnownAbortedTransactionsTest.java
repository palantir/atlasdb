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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.transaction.knowledge.cache.AbortTransactionsSoftCache;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public final class DefaultKnownAbortedTransactionsTest {
    private final FutileTimestampStore futileTimestampStore = mock(FutileTimestampStore.class);
    private final AbortTransactionsSoftCache softCache = mock(AbortTransactionsSoftCache.class);
    private final KnownAbortedTransactions knownAbortedTransactions = new DefaultKnownAbortedTransactions(futileTimestampStore, softCache);

    @Test
    public void testIsKnownAbortedLoadsFromReliableCache() {
        when(softCache.getSoftCacheTransactionStatus(anyLong())).thenReturn(AbortTransactionsSoftCache.TransactionSoftCacheStatus.PENDING_LOAD_FROM_RELIABLE);

        long abortedTimestamp = 27L;
        long bucket = Utils.getBucket(abortedTimestamp);
        Set<Long> abortedTimestamps = ImmutableSet.of(abortedTimestamp);

        when(futileTimestampStore.getAbortedTransactionsInRange(any(), any())).thenReturn(abortedTimestamps);

        Range<Long> rangeForBucket = Utils.getInclusiveRangeForBucket(bucket);

        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp)).isTrue();
        verify(futileTimestampStore).getAbortedTransactionsInRange(rangeForBucket.lowerEndpoint(), rangeForBucket.upperEndpoint());

        // a second call will load state from the cache
        assertThat(knownAbortedTransactions.isKnownAborted(abortedTimestamp  + 1)).isFalse();
        verifyNoMoreInteractions(futileTimestampStore);
    }
}
