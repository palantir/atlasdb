/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchStateUpdate.Success;
import java.util.UUID;
import org.junit.Test;

public class LockWatchCacheImplTest {
    private static final long TIMESTAMP = 1L;
    private static final ImmutableSet<Long> TIMESTAMPS = ImmutableSet.of(TIMESTAMP, 2L);
    private static final Success SUCCESS = LockWatchStateUpdate.success(UUID.randomUUID(), 2L, ImmutableList.of());
    private static final TransactionUpdate UPDATE_1 = ImmutableTransactionUpdate.builder()
            .startTs(1L)
            .commitTs(3L)
            .writesToken(LockToken.of(UUID.randomUUID()))
            .build();
    private static final TransactionUpdate UPDATE_2 = ImmutableTransactionUpdate.builder()
            .startTs(2L)
            .commitTs(4L)
            .writesToken(LockToken.of(UUID.randomUUID()))
            .build();
    private static final ImmutableList<TransactionUpdate> UPDATES = ImmutableList.of(UPDATE_1, UPDATE_2);

    private final LockWatchEventCache eventCache = mock(LockWatchEventCache.class);
    private final LockWatchValueCache valueCache = mock(LockWatchValueCache.class);
    private final LockWatchCache cache = new LockWatchCacheImpl(eventCache, valueCache);

    @Test
    public void startTransactionsTest() {
        cache.processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        verify(eventCache).processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        verify(valueCache).processStartTransactions(TIMESTAMPS);
    }

    @Test
    public void commitTest() {
        cache.processCommitTimestampsUpdate(UPDATES, SUCCESS);
        verify(eventCache).processGetCommitTimestampsUpdate(UPDATES, SUCCESS);
        verify(valueCache, never()).updateCacheAndRemoveTransactionState(anyLong());
        verify(valueCache, never()).removeTransactionState(anyLong());

        cache.updateCacheAndRemoveTransactionState(TIMESTAMP);
        verify(eventCache).removeTransactionStateFromCache(TIMESTAMP);
        verify(valueCache).updateCacheAndRemoveTransactionState(TIMESTAMP);
        verifyNoMoreInteractions(eventCache);
        verifyNoMoreInteractions(valueCache);
    }

    @Test
    public void removeTest() {
        cache.removeTransactionState(TIMESTAMP);
        verify(eventCache).removeTransactionStateFromCache(TIMESTAMP);
        verify(valueCache).removeTransactionState(TIMESTAMP);
    }
}
