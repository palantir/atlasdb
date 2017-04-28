/*
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.sweep;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.persistentlock.PersistentLockId;
import com.palantir.atlasdb.persistentlock.PersistentLockService;

public class PersistentLockManagerTest {
    private PersistentLockService mockPls = mock(PersistentLockService.class);
    private PersistentLockId mockLockId = mock(PersistentLockId.class);

    private PersistentLockManager manager;

    @Before
    public void setUp() {
        when(mockPls.acquireBackupLock(anyString())).thenReturn(mockLockId);

        manager = new PersistentLockManager(mockPls, 1);
    }

    @Test
    public void canAcquireLock() {
        manager.acquirePersistentLockWithRetry();

        assertThat(manager.lockId, is(mockLockId));
        verify(mockPls, times(1)).acquireBackupLock("Sweep");
    }

    @Test
    public void acquireLockRetries() {
        when(mockPls.acquireBackupLock(anyString()))
                .thenThrow(mock(CheckAndSetException.class))
                .thenReturn(mockLockId);

        manager.acquirePersistentLockWithRetry();

        verify(mockPls, times(2)).acquireBackupLock("Sweep");
    }

    @Test(expected = IllegalStateException.class)
    public void callingAcquireTwiceFails() {
        manager.acquirePersistentLockWithRetry();
        manager.acquirePersistentLockWithRetry();
    }

    @Test
    public void canAcquireAndReleaseLock() {
        manager.acquirePersistentLockWithRetry();
        manager.releasePersistentLock();

        assertThat(manager.lockId, nullValue());
        verify(mockPls, times(1)).releaseBackupLock(mockLockId);
    }

    @Test
    public void releaseWithoutAcquireIsNoOp() {
        manager.releasePersistentLock();

        verifyZeroInteractions(mockPls);
    }

    @Test
    public void releaseFailureIsSwallowed() {
        doThrow(CheckAndSetException.class).when(mockPls).releaseBackupLock(any());

        manager.acquirePersistentLockWithRetry();
        manager.releasePersistentLock();
    }

    @Test
    public void cannotAcquireAfterShutdown() {
        manager.shutdown();
        manager.acquirePersistentLockWithRetry();

        verify(mockPls, never()).acquireBackupLock("Sweep");
    }

    @Test
    public void shutdownReleasesLock() {
        manager.acquirePersistentLockWithRetry();
        manager.shutdown();

        verify(mockPls, times(1)).releaseBackupLock(mockLockId);
    }

    @Test
    public void shutdownWithoutAcquireIsNoOp() {
        manager.shutdown();

        verifyZeroInteractions(mockPls);
    }
}
