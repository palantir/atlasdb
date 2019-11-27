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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.TableElements;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.HeldLocks;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchRequest;

public class LockWatchingServiceImplTest {
    private final static TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private final static TableReference TABLE_2 = TableReference.createFromFullyQualifiedName("prod.table");
    private final static LockToken TOKEN = LockToken.of(UUID.randomUUID());

    private final static byte[] ROW = PtBytes.toBytes("row");
    private static final Cell CELL = Cell.create(ROW, ROW);
    private static final LockDescriptor CELL_DESCRIPTOR = AtlasCellLockDescriptor
            .of(TABLE.getQualifiedName(), CELL.getRowName(), CELL.getColumnName());
    private static final LockDescriptor ROW_DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW);

    private final LockEventLog log = mock(LockEventLog.class);
    private final HeldLocksCollection locks = mock(HeldLocksCollection.class);
    private final LockWatchingService lockWatcher = new LockWatchingServiceImpl(log, locks);

    private final HeldLocks heldLocks = mock(HeldLocks.class);
    private final AsyncLock asyncLock = mock(AsyncLock.class);
    private final AsyncLock asyncLock2 = mock(AsyncLock.class);

    @Before
    public void setup() {
        when(heldLocks.getLocks()).thenReturn(ImmutableList.of(asyncLock, asyncLock2));
        when(asyncLock.getDescriptor()).thenReturn(ROW_DESCRIPTOR);
        when(asyncLock2.getDescriptor()).thenReturn(descriptorForTable(TABLE_2));
        when(heldLocks.getToken()).thenReturn(TOKEN);
        when(locks.locksHeld()).thenReturn(ImmutableSet.of(heldLocks));
    }

    @Test
    public void watchNewLockWatchLogsOpenLocksInRange() {
        LockWatchRequest request = tableRequest(TABLE);
        lockWatcher.startWatching(request);

        verifyLoggedOpenLocks(1, ImmutableSet.of(ROW_DESCRIPTOR));
        verify(log).logLockWatchCreated(request);
        verifyNoMoreInteractions(log);
    }

    @Test
    public void registeringWatchLogsAllCoveredLocksAgain() {
        LockDescriptor secondRow = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("other_row"));
        when(asyncLock2.getDescriptor()).thenReturn(secondRow);

        LockWatchRequest prefixRequest = prefixRequest(TABLE, ROW);
        lockWatcher.startWatching(prefixRequest);

        verifyLoggedOpenLocks(1, ImmutableSet.of(ROW_DESCRIPTOR));

        LockWatchRequest entireTableRequest = tableRequest(TABLE);
        lockWatcher.startWatching(entireTableRequest);

        verifyLoggedOpenLocks(2, ImmutableSet.of(ROW_DESCRIPTOR, secondRow));
        verify(log).logLockWatchCreated(entireTableRequest);
    }

    @Test
    public void watchThatIsAlreadyCoveredIsLoggedAgain() {
        LockWatchRequest request = tableRequest(TABLE);
        lockWatcher.startWatching(request);

        verifyLoggedOpenLocks(1, ImmutableSet.of(ROW_DESCRIPTOR));
        verify(log).logLockWatchCreated(request);

        LockWatchRequest prefixRequest = prefixRequest(TABLE, ROW);
        lockWatcher.startWatching(prefixRequest);
        verify(log).logLockWatchCreated(prefixRequest);
    }

    @Test
    public void exactCellWatchTest() {
        LockDescriptor cellSuffixDescriptor = AtlasCellLockDescriptor
                .of(TABLE.getQualifiedName(), CELL.getRowName(), PtBytes.toBytes("row2"));

        LockWatchRequest request = LockWatchRequest.of(
                ImmutableSet.of(TableElements.exactCell(TABLE, CELL).getAsRange()));
        lockWatcher.startWatching(request);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR, cellSuffixDescriptor);
        lockWatcher.registerLock(locks);
        verifyLoggedLocks(1, ImmutableSet.of(CELL_DESCRIPTOR));
    }

    @Test
    public void exactRowWatchTest() {
        LockDescriptor rowDescriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW);

        LockWatchRequest rowRequest = LockWatchRequest.of(
                ImmutableSet.of(TableElements.exactRow(TABLE, ROW).getAsRange()));
        lockWatcher.startWatching(rowRequest);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR, rowDescriptor);
        lockWatcher.registerLock(locks);
        verifyLoggedLocks(1, ImmutableSet.of(rowDescriptor));
    }

    @Test
    public void rowPrefixWatchTest() {
        byte[] rowPrefix = PtBytes.toBytes("ro");
        LockDescriptor rowDescriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW);
        LockDescriptor notPrefixDescriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("r"));

        LockWatchRequest prefixRequest = prefixRequest(TABLE, rowPrefix);
        lockWatcher.startWatching(prefixRequest);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(CELL_DESCRIPTOR, rowDescriptor, notPrefixDescriptor);
        lockWatcher.registerLock(locks);
        verifyLoggedLocks(1, ImmutableSet.of(CELL_DESCRIPTOR, rowDescriptor));
    }

    @Test
    public void rowRangeWatchTest() {
        byte[] startOfRange = PtBytes.toBytes("aaz");
        byte[] endofRange = PtBytes.toBytes("cca");

        LockDescriptor cellInRange = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), startOfRange, ROW);
        LockDescriptor cellOutOfRange = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), endofRange, ROW);

        LockDescriptor rowInRange = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("b"));
        LockDescriptor rowInRange2 = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("cc"));
        LockDescriptor rowOutOfRange = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes("cca"));

        LockWatchRequest rangeRequest = LockWatchRequest.of(ImmutableSet.of(
                TableElements.rowRange(TABLE, startOfRange, endofRange).getAsRange()));
        lockWatcher.startWatching(rangeRequest);

        ImmutableSet<LockDescriptor> locks = ImmutableSet.of(
                cellInRange, cellOutOfRange, rowInRange, rowInRange2, rowOutOfRange);
        lockWatcher.registerLock(locks);
        verifyLoggedLocks(1, ImmutableSet.of(cellInRange, rowInRange, rowInRange2));
    }

    @Test
    public void entireTableWatchTest() {
        LockDescriptor cellOutOfRange = AtlasCellLockDescriptor.of(TABLE_2.getQualifiedName(), ROW, ROW);
        LockDescriptor rowInRange = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW);
        LockDescriptor rowOutOfRange = AtlasRowLockDescriptor.of(TABLE_2.getQualifiedName(), ROW);

        LockWatchRequest tableRequest = tableRequest(TABLE);
        lockWatcher.startWatching(tableRequest);

        ImmutableSet<LockDescriptor> locks = ImmutableSet
                .of(CELL_DESCRIPTOR, cellOutOfRange, rowInRange, rowOutOfRange);
        lockWatcher.registerLock(locks);
        verifyLoggedLocks(1, ImmutableSet.of(CELL_DESCRIPTOR, rowInRange));
    }

    @Test
    public void unlockWithoutLockTest() {
        LockWatchRequest tableRequest = tableRequest(TABLE);
        lockWatcher.startWatching(tableRequest);

        lockWatcher.registerUnlock(ImmutableSet.of(CELL_DESCRIPTOR));
        verifyLoggedUnlocks(1, ImmutableSet.of(CELL_DESCRIPTOR));
    }

    private static LockWatchRequest tableRequest(TableReference tableRef) {
        return LockWatchRequest.of(ImmutableSet.of(TableElements.entireTable(tableRef).getAsRange()));
    }

    private static LockWatchRequest prefixRequest(TableReference tableRef, byte[] prefix) {
        return LockWatchRequest.of(ImmutableSet.of(TableElements.rowPrefix(tableRef, prefix).getAsRange()));
    }

    private static LockDescriptor descriptorForTable(TableReference tableRef) {
        return AtlasRowLockDescriptor.of(tableRef.getQualifiedName(), ROW);
    }

    @SuppressWarnings("unchecked")
    private void verifyLoggedOpenLocks(int invocations, Set<LockDescriptor> locks) {
        ArgumentCaptor<Set<LockDescriptor>> captor = ArgumentCaptor.forClass(Set.class);
        verify(log, times(invocations)).logOpenLocks(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrderElementsOf(locks);
    }

    @SuppressWarnings("unchecked")
    private void verifyLoggedLocks(int invocations, Set<LockDescriptor> locks) {
        ArgumentCaptor<Set<LockDescriptor>> captor = ArgumentCaptor.forClass(Set.class);
        verify(log, times(invocations)).logLock(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrderElementsOf(locks);
    }

    @SuppressWarnings("unchecked")
    private void verifyLoggedUnlocks(int invocations, Set<LockDescriptor> locks) {
        ArgumentCaptor<Set<LockDescriptor>> captor = ArgumentCaptor.forClass(Set.class);
        verify(log, times(invocations)).logUnlock(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrderElementsOf(locks);
    }
}
