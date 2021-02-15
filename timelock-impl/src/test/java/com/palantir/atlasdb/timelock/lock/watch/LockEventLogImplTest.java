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

package com.palantir.atlasdb.timelock.lock.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchReferenceUtils;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.ExclusiveLock;
import com.palantir.atlasdb.timelock.lock.HeldLocks;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class LockEventLogImplTest {
    private final AtomicReference<LockWatches> lockWatches = new AtomicReference<>(LockWatches.create());
    private final HeldLocksCollection heldLocksCollection = mock(HeldLocksCollection.class);
    private final HeldLocks heldLocks = mock(HeldLocks.class);
    private final LockEventLog log = new LockEventLogImpl(LOG_ID, lockWatches::get, heldLocksCollection);

    private static final UUID LOG_ID = UUID.randomUUID();
    private static final UUID STALE_LOG_ID = UUID.randomUUID();
    private static final Optional<LockWatchVersion> NEGATIVE_VERSION_CURRENT_LOG_ID =
            Optional.of(LockWatchVersion.of(LOG_ID, -1L));
    private static final Optional<LockWatchVersion> FUTURE_VERSION_CURRENT_LOG_ID =
            Optional.of(LockWatchVersion.of(LOG_ID, 100L));
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("test.table");
    private static final String TABLE = TABLE_REF.getQualifiedName();
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE, PtBytes.toBytes("1"));
    private static final LockDescriptor DESCRIPTOR_2 = AtlasRowLockDescriptor.of(TABLE, PtBytes.toBytes("2"));
    private static final LockDescriptor DESCRIPTOR_3 = AtlasRowLockDescriptor.of(TABLE, PtBytes.toBytes("3"));
    private static final AsyncLock LOCK_2 = new ExclusiveLock(DESCRIPTOR_2);
    private static final AsyncLock LOCK_3 = new ExclusiveLock(DESCRIPTOR_3);
    private static final LockToken TOKEN = LockToken.of(UUID.randomUUID());

    @Before
    public void setupMocks() {
        when(heldLocks.getLocks()).thenReturn(ImmutableSet.of(LOCK_2, LOCK_3));
        when(heldLocks.getToken()).thenReturn(TOKEN);
        when(heldLocksCollection.locksHeld()).thenReturn(ImmutableSet.of(heldLocks));
    }

    @Test
    public void emptyLogTest() {
        LockWatchStateUpdate update = log.getLogDiff(NEGATIVE_VERSION_CURRENT_LOG_ID);
        LockWatchStateUpdate.Success success = UpdateVisitors.assertSuccess(update);
        assertThat(success.lastKnownVersion()).isEqualTo(-1L);
        assertThat(success.events()).isEmpty();
    }

    @Test
    public void lockUpdateTest() {
        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_2);
        log.logLock(locks, TOKEN);
        LockWatchStateUpdate update = log.getLogDiff(NEGATIVE_VERSION_CURRENT_LOG_ID);

        LockWatchStateUpdate.Success success = UpdateVisitors.assertSuccess(update);
        assertThat(success.events())
                .containsExactly(LockEvent.builder(locks, TOKEN).build(0L));
    }

    @Test
    public void unlockUpdateTest() {
        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_2);
        log.logUnlock(locks);
        LockWatchStateUpdate update = log.getLogDiff(NEGATIVE_VERSION_CURRENT_LOG_ID);

        LockWatchStateUpdate.Success success = UpdateVisitors.assertSuccess(update);
        assertThat(success.events()).containsExactly(UnlockEvent.builder(locks).build(0L));
    }

    @Test
    public void createLockWatchUpdateTest() {
        LockWatchReference secondRowReference = LockWatchReferenceUtils.rowPrefix(TABLE_REF, PtBytes.toBytes("2"));
        LockWatches newWatches = createWatchesFor(secondRowReference);
        log.logLockWatchCreated(newWatches);
        LockWatchStateUpdate update = log.getLogDiff(NEGATIVE_VERSION_CURRENT_LOG_ID);

        LockWatchStateUpdate.Success success = UpdateVisitors.assertSuccess(update);
        assertThat(success.events())
                .containsExactly(LockWatchCreatedEvent.builder(newWatches.references(), ImmutableSet.of(DESCRIPTOR_2))
                        .build(0L));
    }

    @Test
    public void noKnownVersionReturnsSnapshotContainingCurrentMatchingLocks() {
        LockWatchReference entireTable = LockWatchReferenceUtils.entireTable(TABLE_REF);
        lockWatches.set(createWatchesFor(entireTable));

        LockWatchStateUpdate update = log.getLogDiff(Optional.empty());

        LockWatchStateUpdate.Snapshot snapshot = UpdateVisitors.assertSnapshot(update);
        assertThat(snapshot.lastKnownVersion()).isEqualTo(-1L);
        assertThat(snapshot.locked()).isEqualTo(ImmutableSet.of(DESCRIPTOR_2, DESCRIPTOR_3));
        assertThat(snapshot.lockWatches()).containsExactly(entireTable);
    }

    @Test
    public void snapshotIgnoresPreviousLogEntriesInLocksCalculation() {
        LockWatchReference entireTable = LockWatchReferenceUtils.entireTable(TABLE_REF);
        lockWatches.set(createWatchesFor(entireTable));

        log.logLock(ImmutableSet.of(DESCRIPTOR), TOKEN);
        LockWatchStateUpdate update = log.getLogDiff(Optional.empty());

        LockWatchStateUpdate.Snapshot snapshot = UpdateVisitors.assertSnapshot(update);
        assertThat(snapshot.lastKnownVersion()).isEqualTo(0L);
        assertThat(snapshot.locked()).isEqualTo(ImmutableSet.of(DESCRIPTOR_2, DESCRIPTOR_3));
        assertThat(snapshot.lockWatches()).containsExactly(entireTable);
    }

    @Test
    public void requestForTheFutureReturnsSnapshot() {
        LockWatchReference entireTable = LockWatchReferenceUtils.entireTable(TABLE_REF);
        lockWatches.set(createWatchesFor(entireTable));

        LockWatchStateUpdate update = log.getLogDiff(FUTURE_VERSION_CURRENT_LOG_ID);

        LockWatchStateUpdate.Snapshot snapshot = UpdateVisitors.assertSnapshot(update);
        assertThat(snapshot.lastKnownVersion()).isEqualTo(-1L);
        assertThat(snapshot.locked()).isEqualTo(ImmutableSet.of(DESCRIPTOR_2, DESCRIPTOR_3));
        assertThat(snapshot.lockWatches()).containsExactly(entireTable);
    }

    @Test
    public void requestWithStaleLogIdReturnsSnapshot() {
        LockWatchReference entireTable = LockWatchReferenceUtils.entireTable(TABLE_REF);
        lockWatches.set(createWatchesFor(entireTable));

        LockWatchStateUpdate update = log.getLogDiff(Optional.of(LockWatchVersion.of(STALE_LOG_ID, -1L)));

        LockWatchStateUpdate.Snapshot snapshot = UpdateVisitors.assertSnapshot(update);
        assertThat(snapshot.lastKnownVersion()).isEqualTo(-1L);
        assertThat(snapshot.locked()).isEqualTo(ImmutableSet.of(DESCRIPTOR_2, DESCRIPTOR_3));
        assertThat(snapshot.lockWatches()).containsExactly(entireTable);
    }

    private LockWatches createWatchesFor(LockWatchReference... references) {
        return ImmutableLockWatches.of(
                Arrays.stream(references).collect(Collectors.toSet()),
                Arrays.stream(references)
                        .map(ref -> ref.accept(LockWatchReferences.TO_RANGES_VISITOR))
                        .collect(TreeRangeSet::create, RangeSet::add, RangeSet::addAll));
    }
}
