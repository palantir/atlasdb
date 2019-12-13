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

import java.util.OptionalLong;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.lock.EteLockWatchResource;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchInfo;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TimestampWithLockInfo;

public class LockWatchEteTest {
    private static final String TABLE = "lwtest.table";
    private static final LockWatchReferences.LockWatchReference FULL_TABLE = LockWatchReferences.entireTable(TABLE);
    private final EteLockWatchResource lockWatcher = EteSetup.createClientToSingleNode(EteLockWatchResource.class);

    @Test
    public void transactionCanSeeWatchedLocks() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        long startTs = lockWatcher.startTransaction();
        assertThat(lockWatcher.getLockWatchInfo(startTs, getRandomRowDescriptor(TABLE)))
                .isEqualTo(LockWatchInfo.of(LockWatchInfo.State.UNLOCKED, OptionalLong.empty()));
        assertThat(lockWatcher.getLockWatchInfo(startTs, getRandomRowDescriptor("oth.tab")))
                .isEqualTo(LockWatchInfo.UNKNOWN);
    }

    @Test
    public void transactionCanSeeLocksTakenOutBeforeAndAfterRegistering() {
        LockDescriptor lockedBefore = getRandomRowDescriptor(TABLE);
        LockToken first = lockWatcher.lock(
                LockRequest.of(ImmutableSet.of(lockedBefore), 1000));
        lockWatcher.registerLockWatch(FULL_TABLE);
        LockDescriptor lockedAfter = getRandomRowDescriptor(TABLE);
        LockToken second = lockWatcher.lock(
                LockRequest.of(ImmutableSet.of(lockedAfter), 1000));

        long startTs = lockWatcher.startTransaction();
        LockWatchInfo info1 = lockWatcher.getLockWatchInfo(startTs, lockedBefore);
        LockWatchInfo info2 = lockWatcher.getLockWatchInfo(startTs, lockedAfter);
        LockWatchInfo info3 = lockWatcher.getLockWatchInfo(startTs, getRandomRowDescriptor(TABLE));
        assertThat(info1.state()).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(info2.state()).isEqualTo(LockWatchInfo.State.LOCKED);
        assertThat(info3.state()).isEqualTo(LockWatchInfo.State.UNLOCKED);
    }

    @Test
    public void filtersOutOurOwnLocks() {
        lockWatcher.registerLockWatch(FULL_TABLE);
        long startTs = lockWatcher.startTransaction();
        LockDescriptor descriptor = getRandomRowDescriptor(TABLE);
        LockToken first = lockWatcher.lock(LockRequest.of(ImmutableSet.of(descriptor), 1000));
        TimestampWithLockInfo result = lockWatcher.getCommitTimestampAndLockInfo(startTs,
                first);
        assertThat(result.locksSinceLastKnownState()).doesNotContain(descriptor);
    }

    private static LockDescriptor getRandomRowDescriptor(String table) {
        return AtlasRowLockDescriptor.of(table, PtBytes.toBytes(UUID.randomUUID().toString()));
    }
}
