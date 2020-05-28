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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;

@RunWith(MockitoJUnitRunner.class)
public final class ClientLockWatchSnapshotUpdaterImplTest {
    private static final String TABLE = "table";
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE, new byte[] {1});
    private static final LockDescriptor DESCRIPTOR_2 = AtlasRowLockDescriptor.of(TABLE, new byte[] {2});
    private static final LockDescriptor DESCRIPTOR_3 = AtlasRowLockDescriptor.of(TABLE, new byte[] {3});
    private static final LockWatchReferences.LockWatchReference REFERENCE = LockWatchReferences.entireTable("table");
    private static final LockWatchEvent WATCH_EVENT =
            LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_2))
                    .build(0L);
    private static final LockWatchEvent UNLOCK_EVENT = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_2)).build(1L);
    private static final LockWatchEvent LOCK_EVENT = LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3),
            LockToken.of(UUID.randomUUID())).build(2L);
    private static final IdentifiedVersion VERSION = IdentifiedVersion.of(UUID.randomUUID(), 999L);

    private ClientLockWatchSnapshotUpdater snapshotUpdater;

    @Before
    public void before() {
        snapshotUpdater = ClientLockWatchSnapshotUpdaterImpl.create();
    }

    @Test
    public void eventsProcessedAsExpected() {
        snapshotUpdater.processEvents(ImmutableList.of(WATCH_EVENT));
        LockWatchStateUpdate.Snapshot snapshot = snapshotUpdater.getSnapshot(VERSION);
        assertThat(snapshot.locked()).containsExactlyInAnyOrder(DESCRIPTOR, DESCRIPTOR_2);
        assertThat(snapshot.lockWatches()).containsExactlyInAnyOrder(REFERENCE);

        snapshotUpdater.processEvents(ImmutableList.of(UNLOCK_EVENT, LOCK_EVENT));
        LockWatchStateUpdate.Snapshot snapshot2 = snapshotUpdater.getSnapshot(VERSION);
        assertThat(snapshot2.locked()).containsExactlyInAnyOrder(DESCRIPTOR, DESCRIPTOR_3);
        assertThat(snapshot2.lockWatches()).containsExactlyInAnyOrder(REFERENCE);
    }
}
