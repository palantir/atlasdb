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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public final class LockWatchEventCacheImplTest {
    private static final int MIN_EVENTS = 1;
    private static final int MAX_EVENTS = 25;

    private static final UUID INITIAL_LEADER = UUID.randomUUID();
    private static final UUID DIFFERENT_LEADER = UUID.randomUUID();
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    private static final long SEQUENCE_1 = 1L;
    private static final long SEQUENCE_2 = 2L;
    private static final long SEQUENCE_3 = 3L;
    private static final long TIMESTAMP_1 = 72L;
    private static final long TIMESTAMP_2 = 97L;
    private static final long TIMESTAMP_3 = 120L;

    private static final LockWatchVersion VERSION_0 = LockWatchVersion.of(INITIAL_LEADER, 0L);
    private static final LockWatchVersion VERSION_1 = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1);
    private static final LockWatchVersion VERSION_2 = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_2);
    private static final LockWatchVersion VERSION_3 = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_3);

    private static final LockDescriptor DESCRIPTOR_1 = StringLockDescriptor.of("skeleton-key");
    private static final LockDescriptor DESCRIPTOR_2 = StringLockDescriptor.of("2spook5me");
    private static final LockWatchReference REFERENCE_1 = LockWatchReferences.entireTable("table.one");
    private static final LockWatchReference REFERENCE_2 = LockWatchReferences.entireTable("table.two");

    private static final LockWatchEvent LOCK_DESCRIPTOR_2_VERSION_2 =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_2), LOCK_TOKEN).build(SEQUENCE_2);
    private static final LockWatchEvent UNLOCK_DESCRIPTOR_1_VERSION_3 =
            UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_1)).build(SEQUENCE_3);

    private static final LockWatchStateUpdate.Snapshot SNAPSHOT_VERSION_1 = LockWatchStateUpdate.snapshot(
            INITIAL_LEADER, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1));
    private static final LockWatchStateUpdate.Success SUCCESS_VERSION_2 =
            LockWatchStateUpdate.success(INITIAL_LEADER, SEQUENCE_2, ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2));
    private static final LockWatchStateUpdate.Success SUCCESS_VERSION_3 =
            LockWatchStateUpdate.success(INITIAL_LEADER, SEQUENCE_3, ImmutableList.of(UNLOCK_DESCRIPTOR_1_VERSION_3));

    private static final CacheMetrics METRICS = CacheMetrics.create(MetricsManagers.createForTests());

    private LockWatchEventLog eventLog;
    private LockWatchEventCache eventCache;

    @Before
    public void setUp() {
        eventLog = spy(LockWatchEventLog.create(METRICS, MIN_EVENTS, MAX_EVENTS));
        eventCache = new LockWatchEventCacheImpl(eventLog);
    }

    @Test
    public void getUpdateForTransactionsReturnsOnlyRelevantEventsForBatch() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_VERSION_1);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), SUCCESS_VERSION_2);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_3), SUCCESS_VERSION_3);

        verify(eventLog).processUpdate(SNAPSHOT_VERSION_1);
        verify(eventLog).processUpdate(SUCCESS_VERSION_2);
        verify(eventLog).processUpdate(SUCCESS_VERSION_3);

        TransactionsLockWatchUpdate update =
                eventCache.getUpdateForTransactions(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), Optional.empty());

        // This event is effectively a snapshot, and is used as such by the event cache
        LockWatchEvent snapshotEventAtSequence1 = LockWatchCreatedEvent.builder(
                        ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_1))
                .build(SEQUENCE_1);

        assertThat(update.clearCache())
                .as("must clear cache due to no past user version")
                .isTrue();

        assertThat(update.events())
                .as("snapshot up to earliest sequence corresponding to a timestamp, then events up to latest known"
                        + " version")
                .containsExactly(snapshotEventAtSequence1, LOCK_DESCRIPTOR_2_VERSION_2);

        assertThat(update.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        TIMESTAMP_1, VERSION_1,
                        TIMESTAMP_2, VERSION_2));
    }

    @Test
    public void getUpdateForTransactionsCondensesSnapshotWherePossible() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_VERSION_1);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), SUCCESS_VERSION_2);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_3), SUCCESS_VERSION_3);

        verify(eventLog).processUpdate(SNAPSHOT_VERSION_1);
        verify(eventLog).processUpdate(SUCCESS_VERSION_2);
        verify(eventLog).processUpdate(SUCCESS_VERSION_3);

        TransactionsLockWatchUpdate update =
                eventCache.getUpdateForTransactions(ImmutableSet.of(TIMESTAMP_2, TIMESTAMP_3), Optional.of(VERSION_0));

        verify(eventLog)
                .getEventsBetweenVersions(VersionBounds.builder()
                        .startVersion(VERSION_0)
                        .endVersion(VERSION_3)
                        .earliestSnapshotVersion(VERSION_2.version())
                        .build());

        assertThat(update.clearCache()).as("provided version is too far behind").isTrue();

        assertThat(update.events())
                .as("events condensed up to version 2")
                .containsExactly(
                        LockWatchCreatedEvent.builder(
                                        ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_1, DESCRIPTOR_2))
                                .build(SEQUENCE_2),
                        UNLOCK_DESCRIPTOR_1_VERSION_3);

        assertThat(update.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(TIMESTAMP_2, VERSION_2, TIMESTAMP_3, VERSION_3));
    }

    @Test
    public void snapshotClearsPreviousTransactionState() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_VERSION_1);

        LockWatchStateUpdate newSnapshot = LockWatchStateUpdate.snapshot(
                DIFFERENT_LEADER, SEQUENCE_3, ImmutableSet.of(), ImmutableSet.of(REFERENCE_2));

        // New snapshot clears all state from before, and thus TIMESTAMP_1 is no longer present, and should throw
        // when attempting to retrieve information about it
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2, TIMESTAMP_3), newSnapshot);

        verify(eventLog).processUpdate(SNAPSHOT_VERSION_1);
        verify(eventLog).processUpdate(newSnapshot);

        assertThatThrownBy(() -> eventCache.getUpdateForTransactions(
                        ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3), Optional.empty()))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start timestamp missing from map");

        assertThatCode(() -> eventCache.getUpdateForTransactions(
                        ImmutableSet.of(TIMESTAMP_2, TIMESTAMP_3), Optional.empty()))
                .as("contains only timestamps from the new snapshot")
                .doesNotThrowAnyException();
    }

    @Test
    public void processStartTransactionsUpdateAssignsUpdateVersionToTimestamps() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_VERSION_1);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS_VERSION_2);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS_VERSION_3);

        assertThat(eventCache.lastKnownVersion().map(LockWatchVersion::version)).hasValue(SEQUENCE_3);

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), SUCCESS_VERSION_2);

        verify(eventLog).processUpdate(SNAPSHOT_VERSION_1);
        verify(eventLog, times(2)).processUpdate(SUCCESS_VERSION_2);
        verify(eventLog).processUpdate(SUCCESS_VERSION_3);

        assertThat(eventCache.lastKnownVersion().map(LockWatchVersion::version))
                .as("event cache does not go backwards when processing an earlier update")
                .hasValue(SEQUENCE_3);

        TransactionsLockWatchUpdate update =
                eventCache.getUpdateForTransactions(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), Optional.of(VERSION_1));

        assertThat(update.clearCache())
                .as("reasonably up-to-date version provided")
                .isFalse();
        assertThat(update.events())
                .as("only events from version 1 (exclusive) to version 2 (inclusive) required")
                .containsExactly(LOCK_DESCRIPTOR_2_VERSION_2);
        assertThat(update.startTsToSequence())
                .as("TIMESTAMP_2 should not have a version newer than it knows about")
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(TIMESTAMP_1, VERSION_1, TIMESTAMP_2, VERSION_2));
    }

    @Test
    public void processGetCommitTimestampsUpdateAssignsUpdateVersionToTimestamp() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_VERSION_1);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS_VERSION_2);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS_VERSION_3);
        eventCache.processGetCommitTimestampsUpdate(
                ImmutableSet.of(TransactionUpdate.builder()
                        .startTs(TIMESTAMP_1)
                        .commitTs(TIMESTAMP_3)
                        .writesToken(LOCK_TOKEN)
                        .build()),
                SUCCESS_VERSION_2);

        verify(eventLog).processUpdate(SNAPSHOT_VERSION_1);
        verify(eventLog, times(2)).processUpdate(SUCCESS_VERSION_2);
        verify(eventLog).processUpdate(SUCCESS_VERSION_3);

        // Retrieving a commit update should only retrieve events between the start and end of the transaction
        eventCache.getCommitUpdate(TIMESTAMP_1);
        verify(eventLog)
                .getEventsBetweenVersions(VersionBounds.builder()
                        .startVersion(LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1))
                        .endVersion(LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_2))
                        .build());

        // An event update should retrieve any events from start timestamp to latest known timestamp in the cache
        eventCache.getEventUpdate(TIMESTAMP_1);
        verify(eventLog)
                .getEventsBetweenVersions(VersionBounds.builder()
                        .startVersion(LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1))
                        .endVersion(LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_3))
                        .build());
    }

    @Test
    public void removingTransactionStateFromCacheDoesNotRetentionEventsEveryTime() {
        for (int count = 0; count < 1000; ++count) {
            eventCache.removeTransactionStateFromCache(count);
        }

        // The actual number below is somewhat arbitrary due to the gradual warm-up that rate limiters go through. The
        // main point is to confirm that retention does run sometimes, but not every time
        verify(eventLog, atLeastOnce()).retentionEvents(any());
        verify(eventLog, atMost(50)).retentionEvents(any());
    }
}
