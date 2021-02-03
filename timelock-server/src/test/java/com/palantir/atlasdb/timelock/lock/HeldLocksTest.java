/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class HeldLocksTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private static final LockDescriptor LOCK_DESCRIPTOR = StringLockDescriptor.of("foo");

    private final ExclusiveLock lockA = spy(new ExclusiveLock(LOCK_DESCRIPTOR));
    private final ExclusiveLock lockB = spy(new ExclusiveLock(LOCK_DESCRIPTOR));

    private final LeaseExpirationTimer timer = mock(LeaseExpirationTimer.class);

    private HeldLocks heldLocks;

    @Before
    public void before() {
        when(timer.isExpired()).thenReturn(false);
        lockA.lock(REQUEST_ID);
        lockB.lock(REQUEST_ID);
        heldLocks = new HeldLocks(
                new LockLog(new MetricRegistry(), () -> 2L),
                ImmutableList.of(lockA, lockB),
                REQUEST_ID,
                timer,
                mock(LockWatchingService.class));
    }

    @Test
    public void unlocksHeldLocks() {
        heldLocks.unlockExplicitly();

        verify(lockA).unlock(REQUEST_ID);
        verify(lockB).unlock(REQUEST_ID);
    }

    @Test
    public void canRefreshBeforeUnlocking() {
        assertThat(heldLocks.refresh()).isTrue();
    }

    @Test
    public void cannotRefreshAfterUnlocking() {
        heldLocks.unlockExplicitly();

        assertThat(heldLocks.refresh()).isFalse();
    }

    @Test
    public void canOnlyUnlockOnce() {
        assertThat(heldLocks.unlockExplicitly()).isTrue();
        assertThat(heldLocks.unlockExplicitly()).isFalse();
    }

    @Test
    public void unlocksIfExpired() {
        when(timer.isExpired()).thenReturn(true);
        assertThat(heldLocks.unlockIfExpired()).isTrue();
    }

    @Test
    public void doesNotUnlockIfNotExpired() {
        when(timer.isExpired()).thenReturn(false);
        assertThat(heldLocks.unlockIfExpired()).isFalse();
    }

    @Test
    public void refreshRefreshesExpirationTimer() {
        heldLocks.refresh();
        verify(timer).refresh();
    }
}
