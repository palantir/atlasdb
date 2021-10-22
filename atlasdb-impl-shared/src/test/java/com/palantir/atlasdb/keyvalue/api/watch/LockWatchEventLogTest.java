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

import com.google.common.collect.ImmutableList;
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
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;

public final class LockWatchEventLogTest {
    private static final int MIN_EVENTS = 1;
    private static final int MAX_EVENTS = 25;

    private static final UUID INITIAL_LEADER = UUID.randomUUID();
    private static final UUID DIFFERENT_LEADER = UUID.randomUUID();
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    private static final long SEQUENCE_1 = 1L;
    private static final long SEQUENCE_2 = 2L;
    private static final long SEQUENCE_3 = 3L;
    private static final long SEQUENCE_4 = 4L;

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

    /*
       Sequence 1: Snapshot: Lock DESCRIPTOR_1
       Sequence 2: Lock event: Lock DESCRIPTOR_2
       Sequence 3: Unlock event: Unlock DESCRIPTOR_1
       Sequence 4: Lock event: Lock DESCRIPTOR_1
       Net result is that at version 4, both DESCRIPTOR_1 and DESCRIPTOR_2 are locked.
    */
    private static final LockWatchEvent CREATED_UP_TO_VERSION_4 = LockWatchCreatedEvent.builder(
                    ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_1, DESCRIPTOR_2))
            .build(SEQUENCE_4);

    private static final LockWatchStateUpdate.Snapshot INITIAL_SNAPSHOT_VERSION_1 = LockWatchStateUpdate.snapshot(
            INITIAL_LEADER, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1));
    private static final LockWatchStateUpdate.Success SUCCESS_VERSION_2_TO_4 = LockWatchStateUpdate.success(
            INITIAL_LEADER,
            SEQUENCE_4,
            ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_1_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4));

    private static final VersionedEventStoreState EMPTY_EVENT_STORE_STATE = ImmutableVersionedEventStoreState.builder()
            .eventMap(ImmutableSortedMap.of())
            .build();
    private static final ClientLockWatchSnapshotState SNAPSHOT_STATE_VERSION_1 =
            ImmutableClientLockWatchSnapshotState.builder()
                    .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1))
                    .addLocked(DESCRIPTOR_1)
                    .addWatches(REFERENCE_1)
                    .build();

    private final LockWatchEventLog eventLog =
            LockWatchEventLog.create(CacheMetrics.create(MetricsManagers.createForTests()), MIN_EVENTS, MAX_EVENTS);

    @Test
    public void doesNotHaveInitialVersion() {
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void successUpdateWithoutContextLeadsToFailure() {
        CacheUpdate cacheUpdate = eventLog.processUpdate(SUCCESS_VERSION_2_TO_4);
        assertThat(cacheUpdate).isEqualTo(CacheUpdate.FAILED);
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void snapshotUpdateSetsContextAndInstructsClientsToClearCache() {
        CacheUpdate cacheUpdate = eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        LockWatchVersion initialLeaderAtSequenceOne = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1);

        assertThat(cacheUpdate.shouldClearCache()).isTrue();
        assertThat(cacheUpdate.getVersion()).hasValue(initialLeaderAtSequenceOne);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(initialLeaderAtSequenceOne);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceOne)
                        .snapshotState(SNAPSHOT_STATE_VERSION_1)
                        .eventStoreState(EMPTY_EVENT_STORE_STATE)
                        .build());
    }

    @Test
    public void successUpdateUpdatesContextAndShouldInstructClientsNotToClearCache() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        LockWatchEvent lockEvent =
                LockEvent.builder(ImmutableSet.of(DESCRIPTOR_2), LOCK_TOKEN).build(SEQUENCE_2);
        CacheUpdate cacheUpdate = eventLog.processUpdate(
                LockWatchStateUpdate.success(INITIAL_LEADER, SEQUENCE_2, ImmutableList.of(lockEvent)));

        LockWatchVersion initialLeaderAtSequenceTwo = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_2);
        assertThat(cacheUpdate.shouldClearCache()).isFalse();
        assertThat(cacheUpdate.getVersion()).hasValue(initialLeaderAtSequenceTwo);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(initialLeaderAtSequenceTwo);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceTwo)
                        .snapshotState(SNAPSHOT_STATE_VERSION_1)
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of(Sequence.of(SEQUENCE_2), lockEvent))
                                .build())
                        .build());
    }

    @Test
    public void snapshotUpdateResetsDifferingContextAndInstructsClientsToClearCache() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        CacheUpdate secondSnapshotUpdateResult = eventLog.processUpdate(LockWatchStateUpdate.snapshot(
                DIFFERENT_LEADER, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_2), ImmutableSet.of(REFERENCE_2)));

        LockWatchVersion differentLeaderAtSequenceOne = LockWatchVersion.of(DIFFERENT_LEADER, SEQUENCE_1);
        assertThat(secondSnapshotUpdateResult.shouldClearCache()).isTrue();
        assertThat(secondSnapshotUpdateResult.getVersion()).hasValue(differentLeaderAtSequenceOne);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(differentLeaderAtSequenceOne);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(differentLeaderAtSequenceOne)
                        .snapshotState(ImmutableClientLockWatchSnapshotState.builder()
                                .snapshotVersion(differentLeaderAtSequenceOne)
                                .addLocked(DESCRIPTOR_2)
                                .addWatches(REFERENCE_2)
                                .build())
                        .eventStoreState(EMPTY_EVENT_STORE_STATE)
                        .build());
    }

    @Test
    public void successUpdateWithOverlappingEventsOnlyAppliesNewEvents() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        eventLog.processUpdate(LockWatchStateUpdate.success(
                INITIAL_LEADER,
                SEQUENCE_3,
                ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_1_VERSION_3)));

        LockWatchVersion initialLeaderAtSequenceFour = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_4);
        CacheUpdate spanningUpdate = eventLog.processUpdate(SUCCESS_VERSION_2_TO_4);

        assertThat(spanningUpdate.shouldClearCache()).isFalse();
        assertThat(spanningUpdate.getVersion()).hasValue(initialLeaderAtSequenceFour);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(initialLeaderAtSequenceFour);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceFour)
                        .snapshotState(SNAPSHOT_STATE_VERSION_1)
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of(
                                        Sequence.of(SEQUENCE_2),
                                        LOCK_DESCRIPTOR_2_VERSION_2,
                                        Sequence.of(SEQUENCE_3),
                                        UNLOCK_DESCRIPTOR_1_VERSION_3,
                                        Sequence.of(SEQUENCE_4),
                                        LOCK_DESCRIPTOR_1_VERSION_4))
                                .build())
                        .build());
    }

    @Test
    public void oldSuccessUpdateDoesNotReapplyEvents() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        eventLog.processUpdate(LockWatchStateUpdate.success(
                INITIAL_LEADER,
                SEQUENCE_3,
                ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_1_VERSION_3)));

        CacheUpdate oldUpdate = eventLog.processUpdate(LockWatchStateUpdate.success(
                INITIAL_LEADER, SEQUENCE_2, ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2)));

        LockWatchVersion initialLeaderAtSequenceTwo = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_2);
        LockWatchVersion initialLeaderAtSequenceThree = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_3);
        assertThat(oldUpdate.shouldClearCache()).isFalse();
        assertThat(oldUpdate.getVersion()).hasValue(initialLeaderAtSequenceTwo);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(initialLeaderAtSequenceThree);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceThree)
                        .snapshotState(SNAPSHOT_STATE_VERSION_1)
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of(
                                        Sequence.of(SEQUENCE_2),
                                        LOCK_DESCRIPTOR_2_VERSION_2,
                                        Sequence.of(SEQUENCE_3),
                                        UNLOCK_DESCRIPTOR_1_VERSION_3))
                                .build())
                        .build());
    }

    @Test
    public void successUpdateOlderThanSnapshotThrows() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        assertThatThrownBy(() ->
                        eventLog.processUpdate(LockWatchStateUpdate.success(INITIAL_LEADER, 0L, ImmutableList.of())))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot process events before the oldest event. The transaction should be retried, although"
                        + " this should only happen very rarely.");
    }

    @Test
    public void successUpdateFromDifferentLeaderClearsState() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        CacheUpdate cacheUpdate = eventLog.processUpdate(LockWatchStateUpdate.success(
                DIFFERENT_LEADER, SEQUENCE_2, ImmutableList.of(LOCK_DESCRIPTOR_2_VERSION_2)));
        assertThat(cacheUpdate.shouldClearCache()).isTrue();
        assertThat(cacheUpdate.getVersion()).isEmpty();
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void successUpdateWithNoEventsDoesNotThrow() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        CacheUpdate cacheUpdate =
                eventLog.processUpdate(LockWatchStateUpdate.success(INITIAL_LEADER, SEQUENCE_1, ImmutableList.of()));

        LockWatchVersion initialLeaderAtSequenceOne = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1);
        assertThat(cacheUpdate.shouldClearCache()).isFalse();
        assertThat(cacheUpdate.getVersion()).hasValue(initialLeaderAtSequenceOne);
    }

    @Test
    public void successUpdateWithoutBridgingEventsThrows() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        assertThatThrownBy(() -> eventLog.processUpdate(
                        LockWatchStateUpdate.success(INITIAL_LEADER, SEQUENCE_4, ImmutableList.of())))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Success event has a later version than the current "
                        + "version, but has no events to bridge the gap. The transaction should be retried, but this "
                        + "should only happen rarely.");
    }

    @Test
    public void snapshotUpdateAfterSuccessEventResetsState() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        eventLog.processUpdate(SUCCESS_VERSION_2_TO_4);
        CacheUpdate cacheUpdate = eventLog.processUpdate(LockWatchStateUpdate.snapshot(
                INITIAL_LEADER, 6L, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1, REFERENCE_2)));

        LockWatchVersion initialLeaderAtSequenceSix = LockWatchVersion.of(INITIAL_LEADER, 6L);
        assertThat(cacheUpdate.shouldClearCache()).isTrue();
        assertThat(cacheUpdate.getVersion()).hasValue(initialLeaderAtSequenceSix);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(initialLeaderAtSequenceSix);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceSix)
                        .snapshotState(ImmutableClientLockWatchSnapshotState.builder()
                                .snapshotVersion(initialLeaderAtSequenceSix)
                                .addLocked(DESCRIPTOR_1)
                                .addWatches(REFERENCE_1, REFERENCE_2)
                                .build())
                        .eventStoreState(EMPTY_EVENT_STORE_STATE)
                        .build());
    }

    @Test
    public void retentionedEventsAreAppliedToSnapshot() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        eventLog.retentionEvents(Optional.of(Sequence.of(SEQUENCE_4)));
        LockWatchVersion initialLeaderAtSequenceThree = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_3);
        LockWatchVersion initialLeaderAtSequenceFour = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_4);

        assertThat(eventLog.getLatestKnownVersion()).hasValue(initialLeaderAtSequenceFour);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceFour)
                        .snapshotState(ImmutableClientLockWatchSnapshotState.builder()
                                .snapshotVersion(initialLeaderAtSequenceThree)
                                .addLocked(DESCRIPTOR_2)
                                .addWatches(REFERENCE_1)
                                .build())
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of(Sequence.of(4L), LOCK_DESCRIPTOR_1_VERSION_4))
                                .build())
                        .build());
    }

    @Test
    public void getEventsBetweenVersionsWithUpToDateVersionsReturnsNoEvents() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        LockWatchVersion initialLeaderAtSequenceFour = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_4);
        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .startVersion(initialLeaderAtSequenceFour)
                .endVersion(initialLeaderAtSequenceFour)
                .build());

        assertThat(events.clearCache()).isFalse();
        assertThat(events.events().events()).isEmpty();
    }

    @Test
    public void getEventsBetweenVersionsForRecentUpdateReturnsMinimalUpdate() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        LockWatchVersion initialLeaderAtSequenceOne = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_1);
        LockWatchVersion initialLeaderAtSequenceThree = LockWatchVersion.of(INITIAL_LEADER, SEQUENCE_3);
        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .startVersion(initialLeaderAtSequenceOne)
                .endVersion(initialLeaderAtSequenceThree)
                .build());

        assertThat(events.clearCache()).isFalse();
        assertThat(events.events().events())
                .containsExactly(LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_1_VERSION_3);
    }

    @Test
    public void getEventsBetweenVersionsForNoStartVersionReturnsSnapshot() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .endVersion(eventLog.getLatestKnownVersion().get())
                .build());

        assertThat(events.clearCache()).isTrue();
        assertThat(events.events().events()).containsExactly(CREATED_UP_TO_VERSION_4);
    }

    @Test
    public void getEventsBetweenVersionsForDifferentLeaderReturnsSnapshot() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .startVersion(LockWatchVersion.of(DIFFERENT_LEADER, SEQUENCE_1))
                .endVersion(eventLog.getLatestKnownVersion().get())
                .build());

        assertThat(events.clearCache()).isTrue();
        assertThat(events.events().events()).containsExactly(CREATED_UP_TO_VERSION_4);
    }

    @Test
    public void getEventsBetweenVersionsReturnsSnapshotWhenTooFarBehind() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .startVersion(LockWatchVersion.of(INITIAL_LEADER, 0L))
                .endVersion(eventLog.getLatestKnownVersion().get())
                .build());

        assertThat(events.clearCache()).isTrue();
        assertThat(events.events().events()).containsExactly(CREATED_UP_TO_VERSION_4);
    }

    @Test
    public void getEventsBetweenVersionsReturnsPartiallyCondensedSnapshotWhenLimitProvided() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .earliestSnapshotVersion(SEQUENCE_3)
                .endVersion(eventLog.getLatestKnownVersion().get())
                .build());

        assertThat(events.clearCache()).isTrue();
        assertThat(events.events().events())
                .containsExactly(
                        LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_2))
                                .build(SEQUENCE_3),
                        LOCK_DESCRIPTOR_1_VERSION_4);
    }

    @Test
    public void getEventsBetweenVersionsReturnsFullyCondensedSnapshotWhenUpToDateLimitProvided() {
        processInitialSnapshotAndSuccessUpToVersionFour();

        LockWatchVersion latestVersion = eventLog.getLatestKnownVersion().get();
        ClientLogEvents events = eventLog.getEventsBetweenVersions(VersionBounds.builder()
                .earliestSnapshotVersion(latestVersion.version())
                .endVersion(latestVersion)
                .build());

        assertThat(events.clearCache()).isTrue();
        assertThat(events.events().events()).containsExactly(CREATED_UP_TO_VERSION_4);
    }

    private void processInitialSnapshotAndSuccessUpToVersionFour() {
        eventLog.processUpdate(INITIAL_SNAPSHOT_VERSION_1);
        eventLog.processUpdate(SUCCESS_VERSION_2_TO_4);
    }
}
