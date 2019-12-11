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
import static org.assertj.core.api.Assertions.entry;

import static com.palantir.lock.watch.LockWatchInfo.State.LOCKED;
import static com.palantir.lock.watch.LockWatchInfo.State.UNLOCKED;
import static com.palantir.lock.watch.LockWatchReferences.TO_RANGES_VISITOR;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;

public class LockWatchStateUpdaterTest {
    private static final String TABLE_NAME = "test.table1";
    private static final byte[] ROW = {1};
    private static final LockWatchReference TABLE_1 = LockWatchReferences.entireTable(TABLE_NAME);
    private static final LockWatchReference TABLE_2 = LockWatchReferences.entireTable("test.table2");
    private static final LockWatchReference TABLE_3 = LockWatchReferences.entireTable("test.table3");
    private static final LockWatchReference ROW_REF = LockWatchReferences.exactRow(TABLE_NAME, ROW);
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE_NAME, ROW);
    private static final LockWatchEvent OPEN_LOCKS_EVENT = LockWatchOpenLocksEvent
            .builder(ImmutableSet.of(DESCRIPTOR), UUID.randomUUID())
            .build(1);
    private static final LockToken TOKEN = LockToken.of(UUID.randomUUID());

    private final RangeSet<LockDescriptor> watches = TreeRangeSet.create();
    private final Map<LockDescriptor, LockWatchInfo> lockWatchState = new HashMap<>();
    private ConcurrentSkipListSet<UUID> unmatchedOpenLocksEvents = new ConcurrentSkipListSet<>();
    private final LockWatchStateUpdater updater = new LockWatchStateUpdater(watches, lockWatchState,
            unmatchedOpenLocksEvents);

    @Test
    public void watchesAreUpdatedOnSingleEventWhenOpenLocksHappenedBefore() {
        UUID lockWatchId = UUID.randomUUID();

        LockWatchEvent openLocks = LockWatchOpenLocksEvent
                .builder(ImmutableSet.of(DESCRIPTOR), lockWatchId)
                .build(1);
        LockWatchEvent watchCreatedEvent = LockWatchCreatedEvent
                .builder(LockWatchRequest.of(ImmutableSet.of(TABLE_1, TABLE_3)), lockWatchId)
                .build(10);

        openLocks.accept(updater);
        watchCreatedEvent.accept(updater);
        assertThat(watches.asRanges())
                .containsExactlyInAnyOrder(TABLE_1.accept(TO_RANGES_VISITOR), TABLE_3.accept(TO_RANGES_VISITOR));
    }

    @Test
    public void watchesAreNotUpdatedOnSingleEventWhenOpenLocksHasNotHappenedWithSameId() {
        LockWatchEvent watchCreatedEvent = LockWatchCreatedEvent
                .builder(LockWatchRequest.of(ImmutableSet.of(TABLE_1, TABLE_3)), UUID.randomUUID())
                .build(10);

        OPEN_LOCKS_EVENT.accept(updater);
        watchCreatedEvent.accept(updater);
        assertThat(watches.asRanges()).isEmpty();
    }

    @Test
    public void watchesAreUpdatedOnMultipleEvents() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        LockWatchEvent firstOpenLocks = LockWatchOpenLocksEvent
                .builder(ImmutableSet.of(DESCRIPTOR), first)
                .build(1);
        LockWatchEvent secondOpenLocks = LockWatchOpenLocksEvent
                .builder(ImmutableSet.of(), second)
                .build(2);

        LockWatchEvent firstEvent = LockWatchCreatedEvent
                .builder(LockWatchRequest.of(ImmutableSet.of(TABLE_1)), second).build(10);
        LockWatchEvent secondEvent = LockWatchCreatedEvent
                .builder(LockWatchRequest.of(ImmutableSet.of(TABLE_3)), first).build(20);

        firstOpenLocks.accept(updater);
        secondOpenLocks.accept(updater);
        firstEvent.accept(updater);
        secondEvent.accept(updater);

        assertThat(watches.asRanges())
                .containsExactlyInAnyOrder(TABLE_1.accept(TO_RANGES_VISITOR), TABLE_3.accept(TO_RANGES_VISITOR));
    }

    @Test
    public void overlappingWatchesAreMergedOnUpdates() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        LockWatchEvent firstOpenLocks = LockWatchOpenLocksEvent
                .builder(ImmutableSet.of(DESCRIPTOR), first)
                .build(1);
        LockWatchEvent secondOpenLocks = LockWatchOpenLocksEvent
                .builder(ImmutableSet.of(), second)
                .build(2);

        LockWatchEvent firstEvent = LockWatchCreatedEvent
                .builder(LockWatchRequest.of(ImmutableSet.of(TABLE_1, TABLE_3)), first)
                .build(10);
        LockWatchEvent secondEvent = LockWatchCreatedEvent
                .builder(LockWatchRequest.of(ImmutableSet.of(TABLE_2, ROW_REF)), second).build(20);

        firstOpenLocks.accept(updater);
        secondOpenLocks.accept(updater);
        firstEvent.accept(updater);
        secondEvent.accept(updater);

        assertThat(watches.asRanges()).hasSize(1);
        Range<LockDescriptor> range = Iterables.getOnlyElement(watches.asRanges());

        assertThat(range.encloses(TABLE_1.accept(TO_RANGES_VISITOR))).isTrue();
        assertThat(range.encloses(TABLE_2.accept(TO_RANGES_VISITOR))).isTrue();
        assertThat(range.encloses(TABLE_3.accept(TO_RANGES_VISITOR))).isTrue();
        assertThat(range.encloses(ROW_REF.accept(TO_RANGES_VISITOR))).isTrue();
    }

    @Test
    public void watchesAreNotUpdatedOnOtherEvents() {
        LockWatchEvent lock = LockEvent.builder(ImmutableSet.of(DESCRIPTOR), TOKEN).build(1);
        LockWatchEvent unlock = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(2);
        LockWatchEvent openLocks = LockWatchOpenLocksEvent
                .builder(ImmutableSet.of(DESCRIPTOR), UUID.randomUUID())
                .build(3);

        lock.accept(updater);
        unlock.accept(updater);
        openLocks.accept(updater);

        assertThat(watches.asRanges()).isEmpty();
    }

    @Test
    public void correctlyUpdatesOnUnlock() {
        LockWatchEvent unlock = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(1);
        unlock.accept(updater);
        assertThat(lockWatchState).containsExactly(entry(DESCRIPTOR, LockWatchInfo.of(UNLOCKED, OptionalLong.empty())));
    }

    @Test
    public void correctlyUpdatesOnLock() {
        LockWatchEvent lock = LockEvent.builder(ImmutableSet.of(DESCRIPTOR), TOKEN).build(1);
        lock.accept(updater);
        assertThat(lockWatchState).containsExactly(entry(DESCRIPTOR, LockWatchInfo.of(LOCKED, OptionalLong.of(1))));
    }

    @Test
    public void correctlyUpdatesOnOpenLocks() {
        OPEN_LOCKS_EVENT.accept(updater);
        assertThat(lockWatchState).containsExactly(entry(DESCRIPTOR, LockWatchInfo.of(LOCKED, OptionalLong.of(1))));
    }

    @Test
    public void correctlyUpdatesLockThenUnlock() {
        LockWatchEvent lock = LockEvent.builder(ImmutableSet.of(DESCRIPTOR), TOKEN).build(1);
        LockWatchEvent unlock = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(2);

        lock.accept(updater);
        unlock.accept(updater);

        assertThat(lockWatchState)
                .containsExactly(entry(DESCRIPTOR, LockWatchInfo.of(UNLOCKED, 1)));
    }

    @Test
    public void correctlyUpdatesOpenLocksThenUnlock() {
        LockWatchEvent unlock = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(2);

        OPEN_LOCKS_EVENT.accept(updater);
        unlock.accept(updater);

        assertThat(lockWatchState)
                .containsExactly(entry(DESCRIPTOR, LockWatchInfo.of(UNLOCKED, 1)));
    }

    @Test
    public void correctlyUpdatesUnlockThenLock() {
        LockWatchEvent unlock = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR)).build(1);
        LockWatchEvent lock = LockEvent.builder(ImmutableSet.of(DESCRIPTOR), TOKEN).build(2);

        unlock.accept(updater);
        lock.accept(updater);

        assertThat(lockWatchState)
                .containsExactly(entry(DESCRIPTOR, LockWatchInfo.of(LOCKED, 2)));
    }

    @Test
    public void updatesForDifferentLockDescriptorsAreIndependent() {
        LockWatchEvent lock = LockEvent.builder(ImmutableSet.of(DESCRIPTOR), TOKEN).build(1);
        LockDescriptor cellDescriptor = AtlasCellLockDescriptor.of(TABLE_NAME, ROW, ROW);
        LockWatchEvent unlock = UnlockEvent.builder(ImmutableSet.of(cellDescriptor)).build(2);

        unlock.accept(updater);
        lock.accept(updater);

        assertThat(lockWatchState).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                DESCRIPTOR, LockWatchInfo.of(LOCKED, 1),
                cellDescriptor, LockWatchInfo.of(UNLOCKED, OptionalLong.empty())));
    }
}
