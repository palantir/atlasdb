/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;

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
        heldLocks = new HeldLocks(new LockLog(new MetricRegistry(), () -> 2L),
                ImmutableList.of(lockA, lockB), REQUEST_ID, timer);
    }

    @Test
    public void unlocksHeldLocks() {
        heldLocks.unlock();

        verify(lockA).unlock(REQUEST_ID);
        verify(lockB).unlock(REQUEST_ID);
    }

    @Test
    public void canRefreshBeforeUnlocking() {
        assertTrue(heldLocks.refresh());
    }

    @Test
    public void cannotRefreshAfterUnlocking() {
        heldLocks.unlock();

        assertFalse(heldLocks.refresh());
    }

    @Test
    public void canOnlyUnlockOnce() {
        assertTrue(heldLocks.unlock());
        assertFalse(heldLocks.unlock());
    }

    @Test
    public void unlocksIfExpired() {
        when(timer.isExpired()).thenReturn(true);
        assertTrue(heldLocks.unlockIfExpired());
    }

    @Test
    public void doesNotUnlockIfNotExpired() {
        when(timer.isExpired()).thenReturn(false);
        assertFalse(heldLocks.unlockIfExpired());
    }

    @Test
    public void refreshRefreshesExpirationTimer() {
        heldLocks.refresh();
        verify(timer).refresh();
    }

}
