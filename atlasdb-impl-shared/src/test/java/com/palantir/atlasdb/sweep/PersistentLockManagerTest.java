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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.persistentlock.ImmutableLockEntry;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLockId;
import com.palantir.atlasdb.persistentlock.PersistentLockService;

import net.jcip.annotations.GuardedBy;

public class PersistentLockManagerTest {
    private PersistentLockService mockPls = mock(PersistentLockService.class);
    private PersistentLockId mockLockId = mock(PersistentLockId.class);
    private ExecutorService executor = Executors.newCachedThreadPool();

    private PersistentLockManager manager;

    @Before
    public void setUp() {
        when(mockPls.acquireBackupLock(anyString())).thenReturn(mockLockId);

        manager = new PersistentLockManager(mockPls, 1);
    }

    @Test
    @GuardedBy("manager")
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
        whenWeGetTheLockFirstTimeAndThenHoldItForever();

        manager.acquirePersistentLockWithRetry();
        manager.acquirePersistentLockWithRetry();
    }

    @Test
    @GuardedBy("manager")
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
    public void canAcquireAfterReleaseFailureDueToLockClearedFromUnderUs() {
        doThrow(CheckAndSetException.class).when(mockPls).releaseBackupLock(any());

        manager.acquirePersistentLockWithRetry();
        manager.releasePersistentLock();

        // The assumption in this test is that the first lock was released from under us.
        // In this case, we should be able to try and acquire a second lock.
        manager.acquirePersistentLockWithRetry();
    }

    @Test(expected = IllegalStateException.class, timeout = 5_000)
    public void cannotAcquireAfterReleaseFailureDueToDatabaseError() {
        doThrow(RuntimeException.class).when(mockPls).releaseBackupLock(any());
        whenWeGetTheLockFirstTimeAndThenHoldItForever();

        manager.acquirePersistentLockWithRetry();

        try {
            manager.releasePersistentLock();
        } catch (RuntimeException e) {
            // Expected
        }

        manager.acquirePersistentLockWithRetry();
    }

    @Test
    public void canAcquireAfterReleaseSeemsToFailButSecretlySucceeds() {
        doThrow(RuntimeException.class).when(mockPls).releaseBackupLock(any());

        manager.acquirePersistentLockWithRetry();

        try {
            manager.releasePersistentLock();
        } catch (RuntimeException e) {
            // Expected
        }

        // The state we're simulating here is that the underlying key value store reports
        // that the lock release failed, but it actually succeeds under the hood (we timed out waiting for an ack)
        // So at this point, the manager believes we hold lockId, but the DB actually says available.
        // Therefore, the CAS here should succeed.
        manager.acquirePersistentLockWithRetry();
    }

    @Test
    public void cannotAcquireAfterReleaseSeemsToFailButSecretlySucceedsAndThenSomeoneElseTakesTheLock() {
        doThrow(RuntimeException.class).when(mockPls).releaseBackupLock(any());

        manager.acquirePersistentLockWithRetry();

        try {
            manager.releasePersistentLock();
        } catch (RuntimeException e) {
            // Expected
        }

        LockEntry usurper = ImmutableLockEntry.builder()
                .lockName("BackupLock")
                .instanceId(UUID.randomUUID())
                .reason("backup")
                .build();
        CheckAndSetException casException =
                new CheckAndSetException(Cell.create(PtBytes.toBytes("unu"), PtBytes.toBytes("sed")),
                        TableReference.createFromFullyQualifiedName("unu.sed"),
                        PtBytes.toBytes("unused"),
                        ImmutableList.of(usurper.value())
                        );
        when(mockPls.acquireBackupLock(anyString()))
                .thenThrow(casException);

        // We call the non-retrying version here, because we:
        //   (a) don't want the test to hang
        //   (b) want to verify that we adjusted our view of the lockId.
        manager.tryAcquirePersistentLock();

        verify(mockPls, times(2)).acquireBackupLock("Sweep");
        assertNull(manager.lockId);
    }

    @Test
    public void cannotAcquireAfterShutdown() {
        manager.shutdown();
        assertThatThrownBy(() -> manager.acquirePersistentLockWithRetry())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shut down");

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

    @Test(timeout = 10_000)
    public void doesNotDeadlockOnShutdownIfLockCannotBeAcquired() throws InterruptedException {
        CountDownLatch acquireStarted = new CountDownLatch(1);
        when(mockPls.acquireBackupLock(any())).then(inv -> {
            acquireStarted.countDown();
            throw new CheckAndSetException("foo");
        });

        executor.submit(manager::acquirePersistentLockWithRetry);
        acquireStarted.await();

        manager.shutdown();
    }

    private void whenWeGetTheLockFirstTimeAndThenHoldItForever() {
        PersistentLockId oldId = PersistentLockId.fromString("2-4-6-0-1");
        LockEntry oldEntry = ImmutableLockEntry.builder()
                .lockName("BackupLock")
                .instanceId(oldId.value())
                .reason("Sweep")
                .build();
        CheckAndSetException casException =
                new CheckAndSetException(Cell.create(PtBytes.toBytes("unu"), PtBytes.toBytes("sed")),
                        TableReference.createFromFullyQualifiedName("unu.sed"),
                        PtBytes.toBytes("unused"),
                        ImmutableList.of(oldEntry.value())
                );
        when(mockPls.acquireBackupLock(anyString()))
                .thenReturn(oldId)
                .thenThrow(casException);
    }
}
