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

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ImmutableLockWatchCreatedEvent;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.UUID;
import org.junit.Test;

public class ClientLockWatchSnapshotTest {
    private static final LockDescriptor LOCK_DESCRIPTOR_1 = StringLockDescriptor.of("lock-one");
    private static final LockDescriptor LOCK_DESCRIPTOR_2 = StringLockDescriptor.of("lock-two");
    private static final ImmutableSet<LockDescriptor> ONLY_LOCK_DESCRIPTOR_1 = ImmutableSet.of(LOCK_DESCRIPTOR_1);
    private static final LockToken LOCK_TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken LOCK_TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final LockWatchReferences.LockWatchReference LOCK_WATCH_REFERENCE_1 =
            LockWatchReferences.entireTable("table.water");
    private static final LockWatchReferences.LockWatchReference LOCK_WATCH_REFERENCE_2 =
            LockWatchReferences.entireTable("table.salt");
    private static final long SEQUENCE_1 = 1L;
    private static final long SEQUENCE_2 = 2L;
    private static final long SEQUENCE_3 = 3L;
    private static final UUID INITIAL_LEADER_ID = UUID.randomUUID();
    private static final UUID DIFFERENT_LEADER_ID = UUID.randomUUID();

    private final ClientLockWatchSnapshot snapshot = ClientLockWatchSnapshot.create();

    @Test
    public void noInitialSnapshotVersion() {
        assertThat(snapshot.getSnapshotVersion()).isEmpty();
    }

    @Test
    public void lockEventAddsDescriptors() {
        LockWatchEvents events = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_1))
                        .build());
    }

    @Test
    public void singleLockEventCanAddMultipleDescriptors() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(LockEvent.builder(ImmutableSet.of(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2), LOCK_TOKEN_1)
                        .build(SEQUENCE_1))
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_1))
                        .build());
    }

    @Test
    public void multipleLockEventsCanAddMultipleDescriptors() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        LockEvent.builder(ONLY_LOCK_DESCRIPTOR_1, LOCK_TOKEN_1).build(SEQUENCE_1),
                        LockEvent.builder(ImmutableSet.of(LOCK_DESCRIPTOR_2), LOCK_TOKEN_2)
                                .build(SEQUENCE_2))
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    @Test
    public void canCombineProcessedAndFreshEvents() {
        LockWatchEvents events = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(events, INITIAL_LEADER_ID);

        LockWatchEvents freshEvents = singleLockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_2);
        LockWatchStateUpdate.Snapshot updateSnapshot = snapshot.getSnapshotWithEvents(freshEvents, INITIAL_LEADER_ID);
        assertThat(updateSnapshot.lastKnownVersion()).isEqualTo(SEQUENCE_2);
        assertThat(updateSnapshot.locked()).containsExactly(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2);
        assertThat(updateSnapshot.lockWatches()).isEmpty();
        assertThat(updateSnapshot.logId()).isEqualTo(INITIAL_LEADER_ID);
    }

    @Test
    public void handlesSequentialProcessedEvents() {
        LockWatchEvents firstEvent = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(firstEvent, INITIAL_LEADER_ID);

        LockWatchEvents secondEvent = singleLockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_2);
        snapshot.processEvents(secondEvent, INITIAL_LEADER_ID);

        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    @Test
    public void lockedThenUnlockedLockIsNotLocked() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        LockEvent.builder(ONLY_LOCK_DESCRIPTOR_1, LOCK_TOKEN_1).build(SEQUENCE_1),
                        UnlockEvent.builder(ONLY_LOCK_DESCRIPTOR_1).build(SEQUENCE_2))
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    @Test
    public void unlockedThenLockedLockIsLocked() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        UnlockEvent.builder(ONLY_LOCK_DESCRIPTOR_1).build(SEQUENCE_1),
                        LockEvent.builder(ONLY_LOCK_DESCRIPTOR_1, LOCK_TOKEN_1).build(SEQUENCE_2))
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    @Test
    public void partialUnlockDoesNotUnlockUntouchedLocks() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        LockEvent.builder(ImmutableSet.of(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2), LOCK_TOKEN_1)
                                .build(SEQUENCE_1),
                        UnlockEvent.builder(ONLY_LOCK_DESCRIPTOR_1).build(SEQUENCE_2))
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    @Test
    public void lockWatchEventTracked() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(ImmutableLockWatchCreatedEvent.builder()
                        .addReferences(LOCK_WATCH_REFERENCE_1)
                        .addLockDescriptors(LOCK_DESCRIPTOR_1)
                        .sequence(SEQUENCE_1)
                        .build())
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addWatches(LOCK_WATCH_REFERENCE_1)
                        .addLocked(LOCK_DESCRIPTOR_1)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_1))
                        .build());
    }

    @Test
    public void aggregatesDescriptorsAndReferencesAcrossWatchEvents() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        ImmutableLockWatchCreatedEvent.builder()
                                .addReferences(LOCK_WATCH_REFERENCE_1)
                                .addLockDescriptors(LOCK_DESCRIPTOR_1)
                                .sequence(SEQUENCE_1)
                                .build(),
                        ImmutableLockWatchCreatedEvent.builder()
                                .addReferences(LOCK_WATCH_REFERENCE_2)
                                .addLockDescriptors(LOCK_DESCRIPTOR_2)
                                .sequence(SEQUENCE_2)
                                .build())
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addWatches(LOCK_WATCH_REFERENCE_1, LOCK_WATCH_REFERENCE_2)
                        .addLocked(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    @Test
    public void mixedEventTypes() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        ImmutableLockWatchCreatedEvent.builder()
                                .addReferences(LOCK_WATCH_REFERENCE_1)
                                .addLockDescriptors(LOCK_DESCRIPTOR_2)
                                .sequence(SEQUENCE_1)
                                .build(),
                        LockEvent.builder(ONLY_LOCK_DESCRIPTOR_1, LOCK_TOKEN_1).build(SEQUENCE_2),
                        UnlockEvent.builder(ImmutableSet.of(LOCK_DESCRIPTOR_2)).build(SEQUENCE_3))
                .build();
        snapshot.processEvents(events, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addWatches(LOCK_WATCH_REFERENCE_1)
                        .addLocked(LOCK_DESCRIPTOR_1)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_3))
                        .build());
    }

    @Test
    public void resetClearsAllTrackedState() {
        snapshot.processEvents(singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1), INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isNotEqualTo(ImmutableClientLockWatchSnapshotState.builder().build());
        snapshot.reset();
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder().build());
    }

    @Test
    public void throwsIfMissingEventsDetected() {
        LockWatchEvents initialEvent = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(initialEvent, INITIAL_LEADER_ID);

        LockWatchEvents eventPostGap = singleLockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_3);
        assertThatThrownBy(() -> snapshot.processEvents(eventPostGap, INITIAL_LEADER_ID))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Events missing between last snapshot and this batch of events");
    }

    @Test
    public void canProcessSameEventMultipleTimes() {
        LockWatchEvents initialEvent = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(initialEvent, INITIAL_LEADER_ID);
        assertThatCode(() -> snapshot.processEvents(initialEvent, INITIAL_LEADER_ID))
                .doesNotThrowAnyException();
    }

    @Test
    public void canBeginFromEventWithHighSequenceNumber() {
        long sequenceNumber = 6021023L;

        LockWatchEvents initialEvent = singleLockEvent(LOCK_DESCRIPTOR_1, sequenceNumber);
        snapshot.processEvents(initialEvent, INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, sequenceNumber))
                        .build());
    }

    @Test
    public void throwsIfProcessingEventsFromDifferentLeader() {
        LockWatchEvents initialEvent = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(initialEvent, INITIAL_LEADER_ID);

        LockWatchEvents eventFromDifferentLeader = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        assertThatThrownBy(() -> snapshot.processEvents(eventFromDifferentLeader, DIFFERENT_LEADER_ID))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Attempted to update a snapshot to a different version ID");
    }

    @Test
    public void canChangeLeadershipThroughReset() {
        LockWatchEvents initialEvent = singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1);
        snapshot.processEvents(initialEvent, INITIAL_LEADER_ID);

        snapshot.reset();
        LockWatchEvents eventFromDifferentLeader = singleLockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_1);
        snapshot.processEvents(eventFromDifferentLeader, DIFFERENT_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(DIFFERENT_LEADER_ID, SEQUENCE_1))
                        .build());
    }

    @Test
    public void cannotGetSnapshotViewIfPreviousStateWasReset() {
        assertThatThrownBy(() -> snapshot.getSnapshotWithEvents(
                        singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1), INITIAL_LEADER_ID))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage("Snapshot was reset on fail and has not been seeded since");
    }

    @Test
    public void resetWithSnapshotSetsState() {
        snapshot.processEvents(singleLockEvent(LOCK_DESCRIPTOR_1, SEQUENCE_1), INITIAL_LEADER_ID);

        LockWatchStateUpdate.Snapshot snapshotWithBothLocked =
                snapshot.getSnapshotWithEvents(singleLockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_2), INITIAL_LEADER_ID);

        snapshot.processEvents(singleLockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_2), INITIAL_LEADER_ID);
        snapshot.processEvents(singleUnlockEvent(LOCK_DESCRIPTOR_2, SEQUENCE_3), INITIAL_LEADER_ID);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_3))
                        .build());

        snapshot.resetWithSnapshot(snapshotWithBothLocked);
        assertThat(snapshot.getStateForTesting())
                .isEqualTo(ImmutableClientLockWatchSnapshotState.builder()
                        .addLocked(LOCK_DESCRIPTOR_1, LOCK_DESCRIPTOR_2)
                        .snapshotVersion(LockWatchVersion.of(INITIAL_LEADER_ID, SEQUENCE_2))
                        .build());
    }

    private static LockWatchEvents singleLockEvent(LockDescriptor lockDescriptor, long sequence) {
        return LockWatchEvents.builder()
                .addEvents(LockEvent.builder(ImmutableSet.of(lockDescriptor), LOCK_TOKEN_1)
                        .build(sequence))
                .build();
    }

    private static LockWatchEvents singleUnlockEvent(LockDescriptor lockDescriptor, long sequence) {
        return LockWatchEvents.builder()
                .addEvents(UnlockEvent.builder(ImmutableSet.of(lockDescriptor)).build(sequence))
                .build();
    }
}
