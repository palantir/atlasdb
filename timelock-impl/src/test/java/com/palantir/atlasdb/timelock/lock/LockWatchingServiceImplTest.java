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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.LockWatch;

public class LockWatchingServiceImplTest {
    private static final long DEFAULT_TIMESTAMP = 1L;
    private static final UUID SERVICE = UUID.randomUUID();
    private static final UUID SERVICE_2 = UUID.randomUUID();
    private static final LockDescriptor LOCK = StringLockDescriptor.of("lock");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("lock2");
    private static final LockDescriptor LOCK_3 = StringLockDescriptor.of("lock3");
    private static final LockToken TOKEN = LockToken.of(UUID.randomUUID());

    private final AtomicLong timestamps = new AtomicLong();
    private final LongSupplier timestampSupplier = timestamps::getAndIncrement;

    private final LockWatchingService lockWatchingService = new LockWatchingServiceImpl(timestampSupplier);

    @Before
    public void setup() {
        resetTimestampSupplierTo(DEFAULT_TIMESTAMP);
    }

    @Test
    public void canWatchLock() {
        startWatch(SERVICE, LOCK);
        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactly(entry(LOCK, LockWatch.INVALID));
    }

    @Test
    public void multipleWatchesAreUnioned() {
        startWatch(SERVICE, LOCK, LOCK_2);
        startWatch(SERVICE, LOCK_2, LOCK_3);

        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactlyEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.INVALID,
                LOCK_2, LockWatch.INVALID,
                LOCK_3, LockWatch.INVALID));
    }

    @Test
    public void canRegisterLock() {
        startWatch(SERVICE, LOCK);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK), TOKEN);

        assertThat(lockWatchingService.getWatchState(SERVICE))
                .containsExactly(entry(LOCK, LockWatch.uncommitted(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void canRegisterUnlock() {
        startWatch(SERVICE, LOCK);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK), TOKEN);

        timestampSupplier.getAsLong();
        lockWatchingService.registerUnlock(TOKEN);
        assertThat(lockWatchingService.getWatchState(SERVICE))
                .containsExactly(entry(LOCK, LockWatch.committed(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void canOnlySeeRegisteredWatches() {
        startWatch(SERVICE, LOCK);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), TOKEN);

        assertThat(lockWatchingService.getWatchState(SERVICE))
                .containsExactly(entry(LOCK, LockWatch.uncommitted(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void multipleClientsHaveSeparateViews() {
        startWatch(SERVICE, LOCK);
        startWatch(SERVICE_2, LOCK_2);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), TOKEN);

        assertThat(lockWatchingService.getWatchState(SERVICE))
                .containsExactly(entry(LOCK, LockWatch.uncommitted(DEFAULT_TIMESTAMP)));
        assertThat(lockWatchingService.getWatchState(SERVICE_2))
                .containsExactly(entry(LOCK_2, LockWatch.uncommitted(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void lockUpdatesWithNewestLockWatch() {
        startWatch(SERVICE, LOCK, LOCK_2);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK), TOKEN);

        resetTimestampSupplierTo(100L);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK_2), LockToken.of(UUID.randomUUID()));

        resetTimestampSupplierTo(50L);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), LockToken.of(UUID.randomUUID()));

        assertThat(lockWatchingService.getWatchState(SERVICE))
                .containsExactlyEntriesOf(ImmutableMap.of(
                        LOCK, LockWatch.uncommitted(50L),
                        LOCK_2, LockWatch.uncommitted(100L)));

    }

    @Test
    public void canStopWatching() {
        startWatch(SERVICE, LOCK, LOCK_2);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), TOKEN);

        lockWatchingService.stopWatching(SERVICE, ImmutableSet.of(LOCK_2));
        assertThat(lockWatchingService.getWatchState(SERVICE))
                .containsExactly(entry(LOCK, LockWatch.uncommitted(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void onlyOneStopWatchIsRequiredForClient() {
        startWatch(SERVICE, LOCK, LOCK_2);
        startWatch(SERVICE, LOCK_2, LOCK_3);

        lockWatchingService.stopWatching(SERVICE, ImmutableSet.of(LOCK_2));

        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactlyEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.INVALID,
                LOCK_3, LockWatch.INVALID));
    }

    @Test
    public void canStartWatchingAfterUnwatchingButLosesPreviousState() {
        startWatch(SERVICE, LOCK, LOCK_2);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), TOKEN);

        lockWatchingService.stopWatching(SERVICE, ImmutableSet.of(LOCK_2));
        startWatch(SERVICE, LOCK_2);

        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactlyEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.uncommitted(DEFAULT_TIMESTAMP),
                LOCK_2, LockWatch.INVALID));
    }

    @Test
    public void canStartWatchingAfterUnwatchingAndRegisterLocks() {
        startWatch(SERVICE, LOCK, LOCK_2);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), TOKEN);

        lockWatchingService.stopWatching(SERVICE, ImmutableSet.of(LOCK_2));
        startWatch(SERVICE, LOCK_2);

        lockWatchingService.registerLock(ImmutableSet.of(LOCK_2), TOKEN);


        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactlyEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.uncommitted(DEFAULT_TIMESTAMP),
                LOCK_2, LockWatch.uncommitted(DEFAULT_TIMESTAMP + 1)));
    }


    @Test
    public void unlockOnlyAffectsCurrentLocks() {
        startWatch(SERVICE, LOCK, LOCK_2, LOCK_3);
        lockWatchingService.registerLock(ImmutableSet.of(LOCK, LOCK_2), TOKEN);

        LockToken secondToken = LockToken.of(UUID.randomUUID());
        lockWatchingService.registerLock(ImmutableSet.of(LOCK_2, LOCK_3), secondToken);

        lockWatchingService.registerLock(ImmutableSet.of(LOCK_3), LockToken.of(UUID.randomUUID()));


        lockWatchingService.registerUnlock(TOKEN);
        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactlyEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.committed(DEFAULT_TIMESTAMP),
                LOCK_2, LockWatch.uncommitted(DEFAULT_TIMESTAMP + 1),
                LOCK_3, LockWatch.uncommitted(DEFAULT_TIMESTAMP + 2)));

        lockWatchingService.registerUnlock(secondToken);
        assertThat(lockWatchingService.getWatchState(SERVICE)).containsExactlyEntriesOf(ImmutableMap.of(
                LOCK, LockWatch.committed(DEFAULT_TIMESTAMP),
                LOCK_2, LockWatch.committed(DEFAULT_TIMESTAMP + 1),
                LOCK_3, LockWatch.uncommitted(DEFAULT_TIMESTAMP + 2)));
    }

    private void startWatch(UUID serviceId, LockDescriptor... locks) {
        lockWatchingService.startWatching(serviceId, ImmutableSet.copyOf(locks));
    }

    private void resetTimestampSupplierTo(long timestamp) {
        timestamps.getAndUpdate(ignore -> timestamp);
    }
}
