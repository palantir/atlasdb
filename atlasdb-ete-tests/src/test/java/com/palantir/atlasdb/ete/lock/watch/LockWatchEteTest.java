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
import com.palantir.lock.watch.TimestampWithLockInfo;

public class LockWatchEteTest {
    private static final String TABLE = "lwtest.table";
    private static final String OTHER_TABLE = "other.table";
    private static final LockWatchReferences.LockWatchReference FULL_TABLE = LockWatchReferences.entireTable(TABLE);
    private final EteLockWatchResource lockWatcher = EteSetup.createClientToSingleNode(EteLockWatchResource.class);

    @Test
    public void transactionCanSeeWatchedLocksAsUnlocked() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockWatchInfoGetter getter = startTransaction();
        assertThat(getter.getState(getRandomRowDescriptor(TABLE))).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(getRandomRowDescriptor(OTHER_TABLE))).isEqualTo(LockWatchInfo.State.NOT_WATCHED);
    }

    @Test
    public void transactionCanSeeLocksTakenOutBeforeRegistering() {
        LockDescriptor lockedBefore = getRandomRowDescriptor(TABLE);
        LockDescriptor notLocked = getRandomRowDescriptor(TABLE);
        lock(lockedBefore);
        lockWatcher.registerLockWatch(FULL_TABLE);

        LockWatchInfoGetter getter = startTransaction();
        assertThat(getter.getState(lockedBefore)).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(getter.getState(notLocked)).isEqualTo(LockWatchInfo.State.UNLOCKED);
    }

    @Test
    public void transactionCanSeeLocksTakenOutAfterRegistering() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockDescriptor lockedAfter = getRandomRowDescriptor(TABLE);
        lockWatcher.lock(LockRequest.of(ImmutableSet.of(lockedAfter), 1000));

        LockWatchInfoGetter getter = startTransaction();
        assertThat(getter.getState(lockedAfter)).isEqualTo(LockWatchInfo.State.LOCKED);
    }

    @Test
    public void transactionSeesUnlocksBeforeStartTransaction() {
        LockDescriptor unlockBeforeRegister = getRandomRowDescriptor(TABLE);
        LockDescriptor lockBeforeUnlockAfterRegister = getRandomRowDescriptor(TABLE);
        LockDescriptor lockAfterUnlockAfterRegister = getRandomRowDescriptor(TABLE);
        LockDescriptor unlockAfterStartTransaction = getRandomRowDescriptor(TABLE);

        LockToken firstToken = lock(unlockBeforeRegister);
        LockToken secondToken = lock(lockBeforeUnlockAfterRegister);
        lockWatcher.unlock(firstToken);

        lockWatcher.registerLockWatch(FULL_TABLE);

        LockToken thirdToken = lock(lockAfterUnlockAfterRegister);
        LockToken fourthToken = lock(unlockAfterStartTransaction);
        lockWatcher.unlock(secondToken);
        lockWatcher.unlock(thirdToken);

        LockWatchInfoGetter getter = startTransaction();

        lockWatcher.unlock(fourthToken);

        assertThat(getter.getState(unlockBeforeRegister)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(lockBeforeUnlockAfterRegister)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(lockAfterUnlockAfterRegister)).isEqualTo(LockWatchInfo.State.UNLOCKED);
        assertThat(getter.getState(unlockAfterStartTransaction)).isEqualTo(LockWatchInfo.State.LOCKED);
    }

    @Test
    public void transactionSeesLocksTakenOutAfterStartTransaction() {
        LockDescriptor lockedBeforeNotTouchedAfter = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedBeforeUnlockedAfter = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedBeforeRelockedAfter = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedAfter = getRandomRowDescriptor(TABLE);
        LockDescriptor lockedAndUnlockedAfter = getRandomRowDescriptor(TABLE);

        lockWatcher.registerLockWatch(FULL_TABLE);

        LockToken throwAway = lock(getRandomRowDescriptor(OTHER_TABLE));
        LockToken token1 = lock(lockedBeforeNotTouchedAfter);
        LockToken token2 = lock(lockedBeforeUnlockedAfter);
        LockToken token3 = lock(lockedBeforeRelockedAfter);

        long startTs = lockWatcher.startTransaction();

        lockWatcher.unlock(token2);
        lockWatcher.unlock(token3);
        lock(lockedBeforeRelockedAfter);
        lock(lockedAfter);
        LockToken token4 = lock(lockedAndUnlockedAfter);
        lockWatcher.unlock(token4);

        TimestampWithLockInfo result = lockWatcher.getCommitTimestampAndLockInfo(startTs, throwAway);
        assertThat(result.locksSinceLastKnownState()).containsExactlyInAnyOrder(lockedBeforeRelockedAfter, lockedAfter, lockedAndUnlockedAfter);
    }

    @Test
    public void filtersOutOurOwnLocks() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        long startTs = lockWatcher.startTransaction();
        LockDescriptor descriptor = getRandomRowDescriptor(TABLE);
        LockToken first = lockWatcher.lock(LockRequest.of(ImmutableSet.of(descriptor), 1000));
        TimestampWithLockInfo result = lockWatcher.getCommitTimestampAndLockInfo(startTs, first);
        assertThat(result.locksSinceLastKnownState()).doesNotContain(descriptor);
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
    }
}
