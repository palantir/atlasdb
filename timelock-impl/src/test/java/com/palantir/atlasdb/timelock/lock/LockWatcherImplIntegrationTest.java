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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.UUID;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.config.ImmutableRateLimitConfig;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.LockWatch;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;

public class LockWatcherImplIntegrationTest {
    private static final UUID SERVICE = UUID.randomUUID();
    private static final LockDescriptor LOCK = StringLockDescriptor.of("lock");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("lock2");
    private static final LockDescriptor LOCK_3 = StringLockDescriptor.of("lock3");

    private ManagedTimestampService timestampService = new InMemoryTimestampService();

    private LockWatcher lockWatcher = new LockWatcherImpl(timestampService::getFreshTimestamp);
    private AsyncLockService lockService = AsyncLockService.createDefault(
            new LockLog(MetricsManagers.createForTests().getRegistry(), timestampService::getFreshTimestamp),
            PTExecutors.newSingleThreadScheduledExecutor(),
            PTExecutors.newSingleThreadScheduledExecutor(),
            () -> ImmutableRateLimitConfig.builder().build(),
            lockWatcher);

    @After
    public void cleanup() {
        lockService.close();
    }

    @Test
    public void watchedLocksGetUpdated() {
        lockWatcher.startWatching(SERVICE, ImmutableSet.of(LOCK, LOCK_2));
        lockService.lock(UUID.randomUUID(), ImmutableSet.of(LOCK, LOCK_3), TimeLimit.of(1000L));

        assertThat(lockWatcher.getWatchState(SERVICE)).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.uncommitted(1L),
                LOCK_2, LockWatch.INVALID));
    }

    @Test
    public void unlockedLocksGetUpdated() {
        lockWatcher.startWatching(SERVICE, ImmutableSet.of(LOCK, LOCK_2, LOCK_3));
        lockService.lock(UUID.randomUUID(), ImmutableSet.of(LOCK_2), TimeLimit.of(1000L));

        AsyncResult<Leased<LockToken>> result = lockService.lock(UUID.randomUUID(), ImmutableSet.of(LOCK, LOCK_3),
                TimeLimit.of(1000L));
        LockToken token = result.get().value();
        lockService.unlock(token);

        assertThat(lockWatcher.getWatchState(SERVICE)).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.committed(2L),
                LOCK_2, LockWatch.uncommitted(1L),
                LOCK_3, LockWatch.committed(2L)));
    }

    @Test
    public void waitForLocksDoesNotUpdateLockWatches() {
        lockWatcher.startWatching(SERVICE, ImmutableSet.of(LOCK));
        lockService.waitForLocks(UUID.randomUUID(), ImmutableSet.of(LOCK), TimeLimit.of(1000L));

        assertThat(lockWatcher.getWatchState(SERVICE)).containsExactly(entry(LOCK, LockWatch.INVALID));
    }
}
