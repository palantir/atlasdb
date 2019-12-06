/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import static com.palantir.lock.watch.LockWatchInfo.State.LOCKED;
import static com.palantir.lock.watch.LockWatchInfo.State.UNLOCKED;

import java.util.OptionalLong;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.LockDescriptor;

public class LockWatchEventTrackerImplTest {
    private static final String TABLE_NAME = "test.table";
    private static final byte[] ROW = new byte[] { 'r', 'o', 'w' };
    private static final byte[] COL = new byte[] { 'c', 'o', 'l' };
    private static final byte[] LOL = new byte[] { 'l', 'o', 'l' };
    private static final LockDescriptor DESCRIPTOR = AtlasCellLockDescriptor.of(TABLE_NAME, ROW, COL);
    private static final LockDescriptor OTHER_DESCRIPTOR = AtlasCellLockDescriptor.of(TABLE_NAME, ROW, LOL);
    private static final LockWatchRequest ROW_PREFIX_REQ = LockWatchRequest.of(
            ImmutableSet.of(LockWatchReferences.rowPrefix(TABLE_NAME, ROW)));

    private final LockWatchEventTracker tracker = new LockWatchEventTrackerImpl();

    @Test
    public void stateWithNoUpdatesIsNotWatched() {
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEmpty();
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);
    }

    @Test
    public void lockWatchCreatedWithoutOpenLockGivesNoInformationOnLocks() {
        UUID leaderId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(leaderId, 0, ImmutableList.of(
                LockWatchCreatedEvent.builder(ROW_PREFIX_REQ, UUID.randomUUID()).build(0)));

        tracker.updateState(update);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(0));
        assertThat(state.leaderId()).isEqualTo(leaderId);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);
    }

    @Test
    public void lockWatchCreatedAfterOpenLockMeansLockIsLocked() {
        UUID leaderId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(leaderId, 1, ImmutableList.of(
                LockWatchOpenLocksEvent.builder(ImmutableSet.of(DESCRIPTOR), requestId).build(0),
                LockWatchCreatedEvent.builder(ROW_PREFIX_REQ, requestId).build(1)));

        tracker.updateState(update);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(1));
        assertThat(state.leaderId()).isEqualTo(leaderId);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, OptionalLong.of(0)));
        assertThat(state.lockWatchState(OTHER_DESCRIPTOR)).isEqualTo(LockWatchInfo.of(UNLOCKED, OptionalLong.empty()));
    }

    @Test
    public void lockCanBeUpdated() {
        UUID leaderId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(leaderId, 1, ImmutableList.of(
                        LockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(0),
                        UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(1)));

        tracker.updateState(update);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(1));
        assertThat(state.leaderId()).isEqualTo(leaderId);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(UNLOCKED, OptionalLong.of(0)));

        LockWatchStateUpdate secondUpdate = LockWatchStateUpdate.update(leaderId, 2, ImmutableList.of(
                        LockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(2)));

        tracker.updateState(secondUpdate);
        state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(2));
        assertThat(state.leaderId()).isEqualTo(leaderId);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, OptionalLong.of(2)));
    }

    @Test
    public void failedUpdateResetsState() {
        UUID oldLeader = UUID.randomUUID();
        UUID lockWatchId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(oldLeader, 0, ImmutableList.of(
                LockWatchOpenLocksEvent.builder(ImmutableSet.of(DESCRIPTOR), lockWatchId).build(0)));

        tracker.updateState(update);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(0));
        assertThat(state.leaderId()).isEqualTo(oldLeader);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, OptionalLong.of(0)));

        LockWatchStateUpdate newUpdate = LockWatchStateUpdate.failure(oldLeader, OptionalLong.of(6));

        tracker.updateState(newUpdate);
        state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(6));
        assertThat(state.leaderId()).isEqualTo(oldLeader);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);

        LockWatchStateUpdate attemptToRegister = LockWatchStateUpdate.update(oldLeader, 7,
                ImmutableList.of(LockWatchCreatedEvent.builder(ROW_PREFIX_REQ, lockWatchId).build(7)));

        tracker.updateState(attemptToRegister);
        state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(7));
        assertThat(state.leaderId()).isEqualTo(oldLeader);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);
    }

    @Test
    public void leaderChangeResetsStateAndReflectsUpdate() {
        UUID oldLeader = UUID.randomUUID();
        UUID lockWatchId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(oldLeader, 0, ImmutableList.of(
                LockWatchOpenLocksEvent.builder(ImmutableSet.of(DESCRIPTOR), lockWatchId).build(0)));

        tracker.updateState(update);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(0));
        assertThat(state.leaderId()).isEqualTo(oldLeader);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, OptionalLong.of(0)));

        UUID newLeader = UUID.randomUUID();
        LockWatchStateUpdate newUpdate = LockWatchStateUpdate.update(newLeader, 6, ImmutableList.of(
                UnlockEvent.builder(ImmutableSet.of(OTHER_DESCRIPTOR)).build(5),
                LockEvent.builder(ImmutableSet.of(OTHER_DESCRIPTOR)).build(6)));

        tracker.updateState(newUpdate);
        state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(6));
        assertThat(state.leaderId()).isEqualTo(newLeader);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);
        assertThat(state.lockWatchState(OTHER_DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, 6));

        LockWatchStateUpdate attemptToRegister = LockWatchStateUpdate.update(newLeader, 7,
                ImmutableList.of(LockWatchCreatedEvent.builder(ROW_PREFIX_REQ, lockWatchId).build(7)));

        tracker.updateState(attemptToRegister);
        state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(7));
        assertThat(state.leaderId()).isEqualTo(newLeader);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);
    }

    @Test
    public void repeatingUpdatesIsNoOp() {
        UUID leaderId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(leaderId, 0, ImmutableList.of(
                        LockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(0)));

        tracker.updateState(update);
        tracker.updateState(update);
        tracker.updateState(update);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(0));
        assertThat(state.leaderId()).isEqualTo(leaderId);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, OptionalLong.of(0)));
    }

    @Test
    public void updateStartingAfterLastKnownVersionIsNoOp() {
        UUID leaderId = UUID.randomUUID();
        LockWatchStateUpdate update = LockWatchStateUpdate.update(leaderId, 0, ImmutableList.of(
                LockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(0)));

        LockWatchStateUpdate futureUpdate = LockWatchStateUpdate.update(leaderId, 7, ImmutableList.of(
                LockEvent.builder(ImmutableSet.of(OTHER_DESCRIPTOR)).build(7)));

        tracker.updateState(update);
        tracker.updateState(futureUpdate);
        VersionedLockWatchState state = tracker.currentState();
        assertThat(state.version()).isEqualTo(OptionalLong.of(0));
        assertThat(state.leaderId()).isEqualTo(leaderId);
        assertThat(state.lockWatchState(DESCRIPTOR)).isEqualTo(LockWatchInfo.of(LOCKED, OptionalLong.of(0)));
        assertThat(state.lockWatchState(OTHER_DESCRIPTOR)).isEqualTo(LockWatchInfo.NOT_WATCHED);
    }
}
