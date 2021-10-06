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

package com.palantir.atlasdb.timelock.lock.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchReferenceUtils;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.ExclusiveLock;
import com.palantir.atlasdb.timelock.lock.HeldLocks;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.UnlockEvent;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class LockWatchingServiceImplTest {
    private static final UUID LOG_ID = UUID.randomUUID();
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final TableReference TABLE_2 = TableReference.createFromFullyQualifiedName("prod.table");
    private static final LockToken TOKEN = LockToken.of(UUID.randomUUID());

    private static final byte[] ROW = PtBytes.toBytes("row");
    private static final Cell CELL = Cell.create(ROW, PtBytes.toBytes("col"));
    private static final LockDescriptor CELL_DESCRIPTOR =
            AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), CELL.getRowName(), CELL.getColumnName());
    private static final LockDescriptor ROW_DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW);
    private static final AsyncLock LOCK = new ExclusiveLock(ROW_DESCRIPTOR);
    private static final AsyncLock LOCK_2 = new ExclusiveLock(descriptorForOtherTable());

    private final HeldLocksCollection locks = mock(HeldLocksCollection.class);
    private final LockWatchingService lockWatcher = new LockWatchingServiceImpl(LOG_ID, locks);

    private final HeldLocks heldLocks = mock(HeldLocks.class);

    private int sequenceCounter = 0;

    @Before
    public void setup() {
        when(heldLocks.getLocks()).thenReturn(ImmutableList.of(LOCK, LOCK_2));
        when(heldLocks.getToken()).thenReturn(TOKEN);
        when(locks.locksHeld()).thenReturn(ImmutableSet.of(heldLocks));
    }

    @Test
    public void runTaskRunsExclusivelyOnLockLog() throws InterruptedException {
        LockWatchRequest request = tableRequest();
        lockWatcher.startWatching(request);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch runTaskStarted = new CountDownLatch(1);
        CountDownLatch otherTaskCompleted = new CountDownLatch(1);

        Future<?> runTask = executor.submit(() -> lockWatcher.runTask(Optional.empty(), () -> {
            runTaskStarted.countDown();
            Uninterruptibles.awaitUninterruptibly(otherTaskCompleted);
            return null;
        }));

        // runTask started
        Uninterruptibles.awaitUninterruptibly(runTaskStarted);
        Set<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR);

        Future<?> registerLock = executor.submit(() -> {
            lockWatcher.registerLock(locks, TOKEN);
            otherTaskCompleted.countDown();
            return null;
        });
        Future<?> registerUnlock = executor.submit(() -> {
            lockWatcher.registerUnlock(locks);
            otherTaskCompleted.countDown();
            return null;
        });

        // lock tasks are blocked
        assertThat(otherTaskCompleted.await(2, TimeUnit.SECONDS)).isFalse();

        assertThat(runTask).isNotDone();
        assertThat(registerLock).isNotDone();
        assertThat(registerUnlock).isNotDone();

        // unblock runTask
        otherTaskCompleted.countDown();

        // all tasks complete
        assertThatCode(() -> runTask.get(2, TimeUnit.SECONDS)).doesNotThrowAnyException();
        assertThatCode(() -> registerLock.get(2, TimeUnit.SECONDS)).doesNotThrowAnyException();
        assertThatCode(() -> registerUnlock.get(2, TimeUnit.SECONDS)).doesNotThrowAnyException();

        executor.shutdownNow();
    }

    @Test
    public void watchNewLockWatchLogsHeldLocksInRange() {
        LockWatchRequest request = tableRequest();
        lockWatcher.startWatching(request);

        List<LockWatchEvent> expectedEvents =
                ImmutableList.of(createdEvent(request.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void registeringWatchWithWiderScopeLogsAlreadyWatchedLocksAgain() {
        LockDescriptor secondRow = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("other_row"));
        when(heldLocks.getLocks()).thenReturn(ImmutableList.of(LOCK, new ExclusiveLock(secondRow)));

        LockWatchRequest prefixRequest = prefixRequest(ROW);
        lockWatcher.startWatching(prefixRequest);

        LockWatchRequest entireTableRequest = tableRequest();
        lockWatcher.startWatching(entireTableRequest);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(prefixRequest.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)),
                createdEvent(entireTableRequest.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR, secondRow)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void registeringWatchWithOverlappingScopeLogsAlreadyWatchedLocksInScopeAgain() {
        LockDescriptor ab = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("ab"));
        LockDescriptor bc = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("bc"));
        LockDescriptor cd = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("cd"));
        when(heldLocks.getLocks())
                .thenReturn(
                        ImmutableList.of(LOCK, new ExclusiveLock(ab), new ExclusiveLock(bc), new ExclusiveLock(cd)));

        LockWatchReference acRange =
                LockWatchReferenceUtils.rowRange(TABLE, PtBytes.toBytes("a"), PtBytes.toBytes("c"));
        LockWatchReference bdRange =
                LockWatchReferenceUtils.rowRange(TABLE, PtBytes.toBytes("b"), PtBytes.toBytes("d"));

        lockWatcher.startWatching(LockWatchRequest.of(ImmutableSet.of(acRange)));
        lockWatcher.startWatching(LockWatchRequest.of(ImmutableSet.of(bdRange)));

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(ImmutableSet.of(acRange), ImmutableSet.of(ab, bc)),
                createdEvent(ImmutableSet.of(bdRange), ImmutableSet.of(bc, cd)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void registeringWatchWithNarrowerScopeIsNoop() {
        LockWatchRequest request = tableRequest();
        lockWatcher.startWatching(request);

        LockWatchRequest prefixRequest = prefixRequest(ROW);
        lockWatcher.startWatching(prefixRequest);

        List<LockWatchEvent> expectedEvents =
                ImmutableList.of(createdEvent(request.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void onlyAlreadyWatchedRangesAreIgnoredOnRegistration() {
        LockWatchRequest request = tableRequest();
        lockWatcher.startWatching(request);

        LockWatchReference newWatch = LockWatchReferenceUtils.entireTable(TABLE_2);
        LockWatchRequest prefixAndOtherTableRequest =
                LockWatchRequest.of(ImmutableSet.of(LockWatchReferenceUtils.rowPrefix(TABLE, ROW), newWatch));
        lockWatcher.startWatching(prefixAndOtherTableRequest);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(request.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)),
                createdEvent(ImmutableSet.of(newWatch), ImmutableSet.of(descriptorForOtherTable())));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void exactCellWatchMatchesExactDescriptorOnly() {
        LockDescriptor cellSuffixDescriptor =
                AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), CELL.getRowName(), PtBytes.toBytes("col2"));

        LockWatchRequest request = LockWatchRequest.of(ImmutableSet.of(LockWatchReferenceUtils.exactCell(TABLE, CELL)));
        lockWatcher.startWatching(request);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR, cellSuffixDescriptor);
        lockWatcher.registerLock(locks, TOKEN);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(request.getReferences(), ImmutableSet.of()), lockEvent(ImmutableSet.of(CELL_DESCRIPTOR)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void exactRowWatchMatchesExactDescriptorOnly() {
        LockWatchRequest rowRequest =
                LockWatchRequest.of(ImmutableSet.of(LockWatchReferenceUtils.exactRow(TABLE, ROW)));
        lockWatcher.startWatching(rowRequest);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR, ROW_DESCRIPTOR);
        lockWatcher.registerLock(locks, TOKEN);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(rowRequest.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)),
                lockEvent(ImmutableSet.of(ROW_DESCRIPTOR)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void rowPrefixWatchMatchesSuffixes() {
        byte[] rowPrefix = PtBytes.toBytes("ro");
        LockDescriptor notPrefixDescriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("r"));

        LockWatchRequest prefixRequest = prefixRequest(rowPrefix);
        lockWatcher.startWatching(prefixRequest);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR, ROW_DESCRIPTOR, notPrefixDescriptor);
        lockWatcher.registerLock(locks, TOKEN);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(prefixRequest.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)),
                lockEvent(ImmutableSet.of(CELL_DESCRIPTOR, ROW_DESCRIPTOR)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void rowRangeWatchTest() {
        byte[] startOfRange = PtBytes.toBytes("aaz");
        byte[] endOfRange = PtBytes.toBytes("cca");

        LockDescriptor cellInRange = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), startOfRange, ROW);
        LockDescriptor cellOutOfRange = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), endOfRange, ROW);

        LockDescriptor rowInRange = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("b"));
        LockDescriptor rowInRange2 = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("cc"));
        LockDescriptor rowOutOfRange = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("cca"));

        LockWatchRequest rangeRequest =
                LockWatchRequest.of(ImmutableSet.of(LockWatchReferenceUtils.rowRange(TABLE, startOfRange, endOfRange)));
        lockWatcher.startWatching(rangeRequest);

        ImmutableSet<LockDescriptor> locks =
                ImmutableSet.of(cellInRange, cellOutOfRange, rowInRange, rowInRange2, rowOutOfRange);
        lockWatcher.registerLock(locks, TOKEN);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(rangeRequest.getReferences(), ImmutableSet.of()),
                lockEvent(ImmutableSet.of(cellInRange, rowInRange, rowInRange2)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void entireTableWatchTest() {
        LockDescriptor cellOutOfRange = AtlasCellLockDescriptor.of(TABLE_2.getQualifiedName(), ROW, ROW);
        LockDescriptor rowInRange = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW);
        LockDescriptor rowOutOfRange = AtlasRowLockDescriptor.of(TABLE_2.getQualifiedName(), ROW);

        LockWatchRequest tableRequest = tableRequest();
        lockWatcher.startWatching(tableRequest);

        ImmutableSet<LockDescriptor> locks =
                ImmutableSet.of(CELL_DESCRIPTOR, cellOutOfRange, rowInRange, rowOutOfRange);
        lockWatcher.registerLock(locks, TOKEN);

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(tableRequest.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)),
                lockEvent(ImmutableSet.of(ROW_DESCRIPTOR, CELL_DESCRIPTOR, rowInRange)));
        assertLoggedEvents(expectedEvents);
    }

    @Test
    public void unlockWithoutLockTest() {
        LockWatchRequest tableRequest = tableRequest();
        lockWatcher.startWatching(tableRequest);

        lockWatcher.registerUnlock(ImmutableSet.of(CELL_DESCRIPTOR));

        List<LockWatchEvent> expectedEvents = ImmutableList.of(
                createdEvent(tableRequest.getReferences(), ImmutableSet.of(ROW_DESCRIPTOR)),
                unlockEvent(ImmutableSet.of(CELL_DESCRIPTOR)));
        assertLoggedEvents(expectedEvents);
    }

    private LockWatchEvent createdEvent(Set<LockWatchReference> references, Set<LockDescriptor> descriptors) {
        return LockWatchCreatedEvent.builder(references, descriptors).build(sequenceCounter++);
    }

    private LockWatchEvent lockEvent(Set<LockDescriptor> lockDescriptors) {
        return LockEvent.builder(lockDescriptors, TOKEN).build(sequenceCounter++);
    }

    private LockWatchEvent unlockEvent(Set<LockDescriptor> lockDescriptors) {
        return UnlockEvent.builder(lockDescriptors).build(sequenceCounter++);
    }

    private static LockWatchRequest tableRequest() {
        return LockWatchRequest.of(ImmutableSet.of(LockWatchReferenceUtils.entireTable(TABLE)));
    }

    private static LockWatchRequest prefixRequest(byte[] prefix) {
        return LockWatchRequest.of(ImmutableSet.of(LockWatchReferenceUtils.rowPrefix(TABLE, prefix)));
    }

    private static LockDescriptor descriptorForOtherTable() {
        return AtlasRowLockDescriptor.of(TABLE_2.getQualifiedName(), ROW);
    }

    private void assertLoggedEvents(List<LockWatchEvent> expectedEvents) {
        LockWatchStateUpdate update = lockWatcher.getWatchStateUpdate(Optional.of(LockWatchVersion.of(LOG_ID, -1L)));
        List<LockWatchEvent> events = UpdateVisitors.assertSuccess(update).events();
        assertThat(events).isEqualTo(expectedEvents);
    }
}
