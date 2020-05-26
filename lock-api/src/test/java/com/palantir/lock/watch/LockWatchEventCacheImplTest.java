/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchEventCacheImplTest {
    private static final IdentifiedVersion VERSION = IdentifiedVersion.of(UUID.randomUUID(), 17L);
    private static final LockWatchStateUpdate.Success SUCCESS =
            LockWatchStateUpdate.success(VERSION.id(), VERSION.version(), ImmutableList.of());
    private static final LockWatchStateUpdate.Snapshot SNAPSHOT =
            LockWatchStateUpdate.snapshot(UUID.randomUUID(), 0L, ImmutableSet.of(), ImmutableSet.of());
    private static final LockWatchStateUpdate.Failed FAILED = LockWatchStateUpdate.failed(UUID.randomUUID());
    private static final Set<Long> TIMESTAMPS = ImmutableSet.of(1L, 2L, 3L, 7L, 124L);

    @Mock
    private ClientLockWatchEventLog eventLog;

    private LockWatchEventCacheImpl eventCache;

    @Before
    public void before() {
        eventCache = LockWatchEventCacheImpl.create(eventLog);
    }

    @Test
    public void processUpdatePassesThroughToEventLog() {
        eventCache.processUpdate(SUCCESS);
        verify(eventLog).processUpdate(eq(SUCCESS), any());
    }

    @Test
    public void processStartTransactionUpdateAddsToCache() {
        when(eventLog.processUpdate(any(), any())).thenReturn(Optional.of(VERSION));
        Map<Long, IdentifiedVersion> expectedMap = new HashMap<>();
        TIMESTAMPS.forEach(timestamp -> expectedMap.put(timestamp, VERSION));

        eventCache.processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        eventCache.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION));
        verify(eventLog).processUpdate(eq(SUCCESS), any());
        verify(eventLog).getEventsForTransactions(expectedMap, Optional.of(VERSION));
    }

    @Test
    public void removeFromCachePerformsDeleteOnUpdate() {
        Map<Long, IdentifiedVersion> expectedMap = new HashMap<>();
        TIMESTAMPS.forEach(timestamp -> expectedMap.put(timestamp, VERSION));

        when(eventLog.processUpdate(any(), any())).thenReturn(Optional.of(VERSION));
        eventCache.processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        eventCache.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION));

        // now, delete
        eventCache.removeTimestampFromCache(7L);
        eventCache.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION));
        verify(eventLog, times(2)).getEventsForTransactions(expectedMap, Optional.of(VERSION));

        // now, confirm that it is gone
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);
        eventCache.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION));
        Map<Long, IdentifiedVersion> reducedMap = new HashMap<>(expectedMap);
        reducedMap.remove(7L);
        verify(eventLog).getEventsForTransactions(reducedMap, Optional.of(VERSION));
    }

    @Test
    public void timestampsRemovedOnSnapshot() {
        Map<Long, IdentifiedVersion> expectedMap = new HashMap<>();
        TIMESTAMPS.forEach(timestamp -> expectedMap.put(timestamp, VERSION));

        when(eventLog.processUpdate(SUCCESS, Optional.empty())).thenReturn(Optional.of(VERSION));
        eventCache.processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        eventCache.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION));
        verify(eventLog).getEventsForTransactions(expectedMap, Optional.of(VERSION));

        IdentifiedVersion newVersion = IdentifiedVersion.of(SNAPSHOT.logId(), SNAPSHOT.lastKnownVersion());
        when(eventLog.processUpdate(eq(SNAPSHOT), any())).thenReturn(Optional.of(newVersion));
        Set<Long> secondBatch = ImmutableSet.of(666L);
        eventCache.processStartTransactionsUpdate(secondBatch, SNAPSHOT);
        eventCache.getEventsForTransactions(secondBatch, Optional.empty());
        verify(eventLog).getEventsForTransactions(ImmutableMap.of(666L, newVersion), Optional.empty());
    }

    @Test
    public void removeFromCacheUpdatesEarliestVersion() {
        IdentifiedVersion laterVersion = IdentifiedVersion.of(SUCCESS.logId(), SUCCESS.lastKnownVersion() + 2);
        LockWatchStateUpdate.Success laterSuccess =
                LockWatchStateUpdate.success(SUCCESS.logId(), SUCCESS.lastKnownVersion() + 2, ImmutableList.of());
        when(eventLog.processUpdate(SUCCESS, Optional.empty())).thenReturn(Optional.of(VERSION));
        when(eventLog.processUpdate(laterSuccess, Optional.of(VERSION))).thenReturn(Optional.of(laterVersion));
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(1L, 2L), SUCCESS);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(3L), laterSuccess);
        eventCache.getEventsForTransactions(ImmutableSet.of(1L, 2L, 3L), Optional.empty());
        verify(eventLog).getEventsForTransactions(
                ImmutableMap.of(1L, VERSION, 2L, VERSION, 3L, laterVersion),
                Optional.empty());

        eventCache.removeTimestampFromCache(2L);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), laterSuccess);
//        verify(eventLog).getEventsForTransactions()

    }

}
