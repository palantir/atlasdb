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

package com.palantir.atlasdb.ete.lock.watch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.lock.EteLockWatchResource;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchInfo;
import com.palantir.lock.watch.LockWatchReferences;

public class LockWatchEteTest {
    private static final String TABLE = "lwtest.table";
    private static final String OTHER_TABLE = "other.table";
    private static final LockWatchReferences.LockWatchReference FULL_TABLE = LockWatchReferences.entireTable(TABLE);
    private final EteLockWatchResource lockWatcher = EteSetup.createClientToSingleNode(EteLockWatchResource.class);

    @Test
    public void transactionIdentifiesWatchedLocksAsUnlocked() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockWatchInfoGetter getter = startTransaction();
        assertThat(getter.getState(getRandomRowDescriptor(TABLE))).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(getRandomRowDescriptor(OTHER_TABLE))).isEqualTo(LockWatchInfo.State.NOT_WATCHED);
    }

    @Test
    public void transactionCanSeeLocksTakenOutBeforeRegistering() {
        LockDescriptor lockedBefore = getRandomRowDescriptor(TABLE);
        lock(lockedBefore);
        lockWatcher.registerLockWatch(FULL_TABLE);

        LockWatchInfoGetter getter = startTransaction();
        assertThat(getter.getState(lockedBefore)).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(getter.getState(getRandomRowDescriptor(TABLE))).isEqualTo(LockWatchInfo.State.UNLOCKED);
    }

    @Test
    public void transactionCanSeeLocksTakenOutAfterRegistering() {
        LockDescriptor lockedAfter = getRandomRowDescriptor(TABLE);
        lockWatcher.registerLockWatch(FULL_TABLE);
        lock(lockedAfter);

        LockWatchInfoGetter getter = startTransaction();
        assertThat(getter.getState(lockedAfter)).isEqualTo(LockWatchInfo.State.LOCKED);
    }

    @Test
    public void transactionCanSeeUnlocksBeforeStartTransaction() {
        LockDescriptor unlockBeforeRegister = getRandomRowDescriptor(TABLE);
        LockDescriptor lockBeforeUnlockAfterRegister = getRandomRowDescriptor(TABLE);
        LockDescriptor lockAfterRegister = getRandomRowDescriptor(TABLE);

        LockToken unlockBeforeToken = lock(unlockBeforeRegister);
        LockToken lockBeforeUnlockAfterToken = lock(lockBeforeUnlockAfterRegister);
        lockWatcher.unlock(unlockBeforeToken);

        lockWatcher.registerLockWatch(FULL_TABLE);

        LockToken lockAfterToken = lock(lockAfterRegister);
        lockWatcher.unlock(lockBeforeUnlockAfterToken);
        lockWatcher.unlock(lockAfterToken);

        LockWatchInfoGetter getter = startTransaction();

        assertThat(getter.getState(unlockBeforeRegister)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(lockBeforeUnlockAfterRegister)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(lockAfterRegister)).isEqualTo(LockWatchInfo.State.UNLOCKED);
    }

    @Test
    public void transactionsHaveSeparateViewsOfLockWatchState() {
        LockDescriptor lockBeforeFirst = getRandomRowDescriptor(TABLE);
        LockDescriptor lockBetweenFirstAndSecond = getRandomRowDescriptor(TABLE);
        LockDescriptor lockAfterSecond = getRandomRowDescriptor(TABLE);

        lockWatcher.registerLockWatch(FULL_TABLE);
        lock(lockBeforeFirst);
        LockWatchInfoGetter first = startTransaction();
        lock(lockBetweenFirstAndSecond);
        LockWatchInfoGetter second = startTransaction();
        lock(lockAfterSecond);

        assertThat(first.getState(lockBeforeFirst)).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(first.getState(lockBetweenFirstAndSecond)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(first.getState(lockAfterSecond)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(second.getState(lockBeforeFirst)).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(second.getState(lockBetweenFirstAndSecond)).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(second.getState(lockAfterSecond)).isEqualTo(LockWatchInfo.State.UNLOCKED);

        assertThat(first.commitAndGetLocks()).containsExactlyInAnyOrder(lockBetweenFirstAndSecond, lockAfterSecond);
        assertThat(second.commitAndGetLocks()).containsExactly(lockAfterSecond);
    }

    @Test
    public void transactionDoesNotSeeUnlocksAfterStartTransaction() {
        LockDescriptor lockBeforeRegister = getRandomRowDescriptor(TABLE);
        LockDescriptor lockAfterRegister = getRandomRowDescriptor(TABLE);

        LockToken lockBeforeToken = lock(lockBeforeRegister);
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockToken lockAfterToken = lock(lockAfterRegister);

        LockWatchInfoGetter getter = startTransaction();

        lockWatcher.unlock(lockBeforeToken);
        lockWatcher.unlock(lockAfterToken);

        assertThat(getter.getState(lockBeforeRegister)).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(getter.getState(lockAfterRegister)).isEqualTo(LockWatchInfo.State.LOCKED);
    }

    @Test
    public void commitCanSeeLocksTakenOutAfterStartTransaction() {
        LockDescriptor lockedBeforeRelockedAfterStart = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedAfterStart = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedAndUnlockedAfterStart = getRandomRowDescriptor(TABLE);

        lockWatcher.registerLockWatch(FULL_TABLE);

        LockToken lockedBeforeRelockedAfterToken = lock(lockedBeforeRelockedAfterStart);

        LockWatchInfoGetter getter = startTransaction();

        lockWatcher.unlock(lockedBeforeRelockedAfterToken);
        lock(lockedBeforeRelockedAfterStart);
        lock(lockedAfterStart);
        LockToken lockedAndUnlockedAfterToken = lock(lockedAndUnlockedAfterStart);
        lockWatcher.unlock(lockedAndUnlockedAfterToken);

        assertThat(getter.commitAndGetLocks()).containsExactlyInAnyOrder(
                lockedBeforeRelockedAfterStart,
                lockedAfterStart,
                lockedAndUnlockedAfterStart);
    }

    @Test
    public void commitDoesNotSeeLocksTakenOutOnlyBeforeStartTransaction() {
        LockDescriptor lockedBeforeStart = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedBeforeUnlockedAfterStart = getRandomRowDescriptor(TABLE);

        lockWatcher.registerLockWatch(FULL_TABLE);
        lock(lockedBeforeStart);
        LockToken token = lock(lockedBeforeUnlockedAfterStart);

        LockWatchInfoGetter getter = startTransaction();

        lockWatcher.unlock(token);

        assertThat(getter.commitAndGetLocks()).isEmpty();
    }

    @Test
    public void filtersOutOurOwnLocks() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockWatchInfoGetter getter = startTransaction();
        LockDescriptor ourLockDescriptor = getRandomRowDescriptor(TABLE);
        LockToken ourToken = lock(ourLockDescriptor);
        assertThat(getter.commitAndGetLocksIgnoring(ourToken)).isEmpty();
    }

    @Test
    public void doesNotFilerOutOurOwnLockIfLockedTwice() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockWatchInfoGetter getter = startTransaction();
        LockDescriptor ourLockDescriptor = getRandomRowDescriptor(TABLE);
        LockToken token = lock(ourLockDescriptor);
        lockWatcher.unlock(token);
        LockToken ourToken = lock(ourLockDescriptor);
        assertThat(getter.commitAndGetLocksIgnoring(ourToken)).containsExactly(ourLockDescriptor);
    }

    private LockWatchInfoGetter startTransaction() {
        return new LockWatchInfoGetter(lockWatcher.startTransaction());
    }

    private static LockDescriptor getRandomRowDescriptor(String table) {
        return AtlasRowLockDescriptor.of(table, PtBytes.toBytes(UUID.randomUUID().toString()));
    }

    private LockToken lock(LockDescriptor descriptor) {
        return lockWatcher.lock(LockRequest.of(ImmutableSet.of(descriptor), 1000));
    }

    private class LockWatchInfoGetter {
        private final long startTimestamp;

        private LockWatchInfoGetter(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        LockWatchInfo.State getState(LockDescriptor descriptor) {
            return lockWatcher.getLockWatchInfo(startTimestamp, descriptor).state();
        }

        Set<LockDescriptor> commitAndGetLocks() {
            return lockWatcher.getCommitTimestampAndLockInfo(startTimestamp, LockToken.of(UUID.randomUUID()))
                    .locksSinceLastKnownState();
        }

        Set<LockDescriptor> commitAndGetLocksIgnoring(LockToken lockToken) {
            return lockWatcher.getCommitTimestampAndLockInfo(startTimestamp, lockToken).locksSinceLastKnownState();
        }
    }
}
