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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchEventCacheImplTest {
    private static final int MIN_EVENTS = 1;
    private static final int MAX_EVENTS = 25;

    private static final UUID INITIAL_LOG_ID = UUID.randomUUID();
    private static final UUID DIFFERENT_LOG_ID = UUID.randomUUID();
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    private static final long SEQUENCE_1 = 1;
    private static final long SEQUENCE_2 = 2;
    private static final long SEQUENCE_3 = 3L;
    private static final long SEQUENCE_4 = 4L;
    private static final long TIMESTAMP_1 = 72L;
    private static final long TIMESTAMP_2 = 97L;
    private static final long TIMESTAMP_3 = 99L;

    private static final LockDescriptor DESCRIPTOR_1 = StringLockDescriptor.of("lwelt-one");
    private static final LockDescriptor DESCRIPTOR_2 = StringLockDescriptor.of("lwelt-two");
    private static final LockWatchReference REFERENCE_1 = LockWatchReferences.entireTable("table.one");
    private static final LockWatchReference REFERENCE_2 = LockWatchReferences.entireTable("table.two");

    private static final LockWatchEvent LOCK_DESCRIPTOR_2_VERSION_2 =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_2), LOCK_TOKEN).build(SEQUENCE_2);
    private static final LockWatchEvent UNLOCK_DESCRIPTOR_1_VERSION_3 =
            UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_1)).build(SEQUENCE_3);
    private static final LockWatchEvent LOCK_DESCRIPTOR_1_VERSION_4 =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_1), LOCK_TOKEN).build(SEQUENCE_4);
    private static final LockWatchEvent SNAPSHOT_UP_TO_VERSION_4 = LockWatchCreatedEvent.builder(
                    ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_1, DESCRIPTOR_2))
            .build(SEQUENCE_4);

    private static final LockWatchStateUpdate.Snapshot SNAPSHOT = LockWatchStateUpdate.snapshot(
            INITIAL_LOG_ID, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1));
    private static final LockWatchStateUpdate.Success SUCCESS_VERSION_4 = LockWatchStateUpdate.success(
            INITIAL_LOG_ID,
            4L,
            ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_1_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4));

    private static final VersionedEventStoreState EMPTY_EVENT_STORE_STATE = ImmutableVersionedEventStoreState.builder()
            .eventMap(ImmutableSortedMap.of())
            .build();
    private static final ClientLockWatchSnapshotState SNAPSHOT_STATE_VERSION_1 =
            ImmutableClientLockWatchSnapshotState.builder()
                    .snapshotVersion(LockWatchVersion.of(INITIAL_LOG_ID, SEQUENCE_1))
                    .addLocked(DESCRIPTOR_1)
                    .addWatches(REFERENCE_1)
                    .build();

    private LockWatchEventLog eventLog;
    private LockWatchEventCache eventCache;

    @Before
    public void setUp() {
        eventLog = spy(LockWatchEventLog.create(CacheMetrics.create(MetricsManagers.createForTests()), 1, 20));
        eventCache = new LockWatchEventCacheImpl(eventLog);
    }

    @Test
    public void processStartTransactionsReturnsOnlyRelevantEventsForBatch() {
        LockWatchStateUpdate.Success firstSuccess =
                LockWatchStateUpdate.success(INITIAL_LOG_ID, SEQUENCE_2, ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2));
        LockWatchStateUpdate.Success secondSuccess = LockWatchStateUpdate.success(
                INITIAL_LOG_ID, SEQUENCE_3, ImmutableList.of(UNLOCK_DESCRIPTOR_1_VERSION_3));

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), firstSuccess);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_3), secondSuccess);

        verify(eventLog).processUpdate(SNAPSHOT);
        verify(eventLog).processUpdate(firstSuccess);
        verify(eventLog).processUpdate(secondSuccess);

        Set<Long> requestedTimestamps = ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2);
        TransactionsLockWatchUpdate update = eventCache.getUpdateForTransactions(requestedTimestamps, Optional.empty());

        assertThat(update.clearCache()).isTrue();
        assertThat(update.events())
                .containsExactly(
                        LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_1))
                                .build(SEQUENCE_1),
                        LOCK_DESCRIPTOR_2_VERSION_2);
        assertThat(update.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        TIMESTAMP_1, LockWatchVersion.of(INITIAL_LOG_ID, SEQUENCE_1),
                        TIMESTAMP_2, LockWatchVersion.of(INITIAL_LOG_ID, SEQUENCE_2)));
    }

    @Test
    public void snapshotClearsPreviousTransactionState() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT);

        LockWatchStateUpdate.Snapshot newSnapshot = LockWatchStateUpdate.snapshot(
                DIFFERENT_LOG_ID, SEQUENCE_3, ImmutableSet.of(), ImmutableSet.of(REFERENCE_2));

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2, TIMESTAMP_3), newSnapshot);

        verify(eventLog).processUpdate(SNAPSHOT);
        verify(eventLog).processUpdate(newSnapshot);

        assertThatThrownBy(() -> eventCache.getUpdateForTransactions(
                        ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3), Optional.empty()))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start timestamp missing from map");
    }
}
