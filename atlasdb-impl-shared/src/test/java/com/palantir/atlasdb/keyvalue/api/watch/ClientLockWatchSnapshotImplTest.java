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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class ClientLockWatchSnapshotImplTest {
    private static final String TABLE = "table";
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE, new byte[] {1});
    private static final LockDescriptor DESCRIPTOR_2 = AtlasRowLockDescriptor.of(TABLE, new byte[] {2});
    private static final LockDescriptor DESCRIPTOR_3 = AtlasRowLockDescriptor.of(TABLE, new byte[] {3});
    private static final LockWatchReferences.LockWatchReference REFERENCE_1 = LockWatchReferences.entireTable("table1");
    private static final LockWatchReferences.LockWatchReference REFERENCE_2 = LockWatchReferences.entireTable("table2");
    public static final ImmutableSet<LockDescriptor> INITIAL_DESCRIPTORS = ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_2);
    private static final LockWatchEvent WATCH_EVENT =
            LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE_1), INITIAL_DESCRIPTORS)
                    .build(0L);
    private static final LockWatchEvent UNLOCK_EVENT = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_2)).build(1L);
    private static final LockWatchEvent LOCK_EVENT = LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3),
            LockToken.of(UUID.randomUUID())).build(2L);
    private static final IdentifiedVersion WATCH_VERSION = IdentifiedVersion.of(UUID.randomUUID(), 0L);
    private static final IdentifiedVersion SECOND_VERSION = IdentifiedVersion.of(UUID.randomUUID(), 2L);
    private static final LockWatchStateUpdate.Snapshot SNAPSHOT_TO_RESET = LockWatchStateUpdate.snapshot(
            UUID.randomUUID(), 1002L, ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_2), ImmutableSet.of(REFERENCE_2));

    private ClientLockWatchSnapshot snapshot;

    @Before
    public void before() {
        snapshot = ClientLockWatchSnapshotImpl.create();
    }

//    @Test
//    public void eventsProcessedAsExpected() {
//        snapshot.processEvents(events, versionId);
//        LockWatchStateUpdate.Snapshot snapshotUpdate = snapshot.getSnapshot();
//        assertThat(snapshotUpdate.locked()).containsExactlyInAnyOrderElementsOf(INITIAL_DESCRIPTORS);
//        assertThat(snapshotUpdate.lockWatches()).containsExactlyInAnyOrder(REFERENCE_1);
//
//        snapshot.processEvents(events, versionId);
//        LockWatchStateUpdate.Snapshot snapshotUpdate2 = snapshot.getSnapshot();
//        assertThat(snapshotUpdate2.locked()).containsExactlyInAnyOrder(DESCRIPTOR, DESCRIPTOR_3);
//        assertThat(snapshotUpdate2.lockWatches()).containsExactlyInAnyOrder(REFERENCE_1);
//    }
//
//    @Test
//    public void stateIsResetOnSnapshot() {
//        setupInitialEvents();
//
//        snapshot.resetWithSnapshot(SNAPSHOT_TO_RESET);
//        LockWatchStateUpdate.Snapshot snapshotUpdate = snapshot.getSnapshot();
//        assertThat(snapshotUpdate.locked()).containsExactlyInAnyOrder(DESCRIPTOR, DESCRIPTOR_2);
//        assertThat(snapshotUpdate.lockWatches()).containsExactlyInAnyOrder(REFERENCE_2);
//    }
//
//    @Test
//    public void stateIsFullyClearedOnReset() {
//        setupInitialEvents();
//
//        snapshot.reset();
//        assertThatThrownBy(() -> snapshot.getSnapshot())
//                .isExactlyInstanceOf(SafeIllegalStateException.class)
//                .hasMessage("Snapshot was reset on fail and has not been seeded since");
//    }
//
//    @Test
//    public void nonContiguousEventsThrows() {
//        assertThatThrownBy(() -> snapshot.processEvents(events, versionId))
//                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
//                .hasMessage("Events form a non-contiguous sequence");
//    }
//
//    @Test
//    public void missedEventsWhenUpdatingThrows() {
//        snapshot.processEvents(events, versionId);
//        assertThatThrownBy(() -> snapshot.processEvents(events, versionId))
//                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
//                .hasMessage("Events missing between last snapshot and this batch of events");
//    }
//
//    private void setupInitialEvents() {
//        snapshot.processEvents(events, versionId);
//        snapshot.processEvents(events, versionId);
//        LockWatchStateUpdate.Snapshot snapshotUpdate = snapshot.getSnapshot();
//        assertThat(snapshotUpdate.locked()).containsExactlyInAnyOrder(DESCRIPTOR, DESCRIPTOR_3);
//        assertThat(snapshotUpdate.lockWatches()).containsExactlyInAnyOrder(REFERENCE_1);
//    }
}
