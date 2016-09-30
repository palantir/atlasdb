/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.persistentlock;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class PersistentLocksShould {
    private static final String REASON = "for testing";

    private static final Supplier<Void> NO_ACTION = () -> null;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void createPersistedLocksTable() {
        KeyValueService keyValueService = mock(KeyValueService.class);
        when(keyValueService.getAllTableNames()).thenReturn(ImmutableSet.of());

        new PersistentLock(keyValueService);

        verify(keyValueService).createTable(eq(AtlasDbConstants.PERSISTED_LOCKS_TABLE), any(byte[].class));
    }

    @Test
    public void notCreatePersistedLocksTableIfItAlreadyExists() {
        KeyValueService keyValueService = mock(KeyValueService.class);
        when(keyValueService.getAllTableNames()).thenReturn(ImmutableSet.of(AtlasDbConstants.PERSISTED_LOCKS_TABLE));

        new PersistentLock(keyValueService);

        verify(keyValueService, times(0)).createTable(any(TableReference.class), any(byte[].class));
    }

    @Test
    public void createAndReleaseLockIfNotAlreadyLocked() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = spy(new InMemoryKeyValueService(false));
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        persistentLock.runWithExclusiveLock(NO_ACTION, PersistentLockName.of("deletionLock"), REASON);

        verify(keyValueService).put(eq(AtlasDbConstants.PERSISTED_LOCKS_TABLE), anyMap(), anyLong());
        verify(keyValueService).delete(eq(AtlasDbConstants.PERSISTED_LOCKS_TABLE), any(Multimap.class));
    }

    @Test
    public void successfullyGrabLockAfterItWasReleased() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        persistentLock.runWithExclusiveLock(NO_ACTION, PersistentLockName.of("deletionLock"), REASON);
        persistentLock.runWithExclusiveLock(NO_ACTION, PersistentLockName.of("deletionLock"), REASON);
    }

    @Test
    public void runActionWhileLockIsHeld() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        persistentLock.runWithExclusiveLock(
                () -> {
                    ImmutableList<RowResult<Value>> rowResults = ImmutableList.copyOf(
                            keyValueService.getRange(AtlasDbConstants.PERSISTED_LOCKS_TABLE, RangeRequest.all(), 1));
                    assertThat(rowResults.size(), equalTo(1));
                    return null;
                },
                PersistentLockName.of("deletionLock"),
                REASON
        );
    }

    @Test
    public void throwIfLockAlreadyExists() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);
        PersistentLockName deletionLock = PersistentLockName.of("deletionLock");
        LockEntry existingLock = LockEntry.of(PersistentLockName.of("deletionLock"), 4321, REASON);
        keyValueService.put(AtlasDbConstants.PERSISTED_LOCKS_TABLE, existingLock.insertionMap(), 0);

        expectedException.expect(PersistentLockIsTakenException.class);
        expectedException.expectMessage(containsString("deletionLock"));
        expectedException.expectMessage(containsString(REASON));
        persistentLock.runWithExclusiveLock(NO_ACTION, deletionLock, REASON);
    }

    @Test(timeout = 1000)
    public void forbidTwoProcessesFromRunningConcurrently()
            throws InterruptedException, ExecutionException, BrokenBarrierException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);
        PersistentLockName deletionLock = PersistentLockName.of("deletionLock");

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Semaphore semaphore = new Semaphore(0);

        Future<Void> keepLockForever = executorService.submit(() -> {
            persistentLock.runWithExclusiveLock(() -> {
                try {
                    barrier.await();
                    semaphore.acquire();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }, deletionLock, REASON);
            return null;
        });

        barrier.await();
        Future<Void> task = executorService.submit(() -> {
            persistentLock.runWithExclusiveLock(NO_ACTION, deletionLock, REASON);
            return null;
        });

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(instanceOf(PersistentLockIsTakenException.class));
        task.get();

        semaphore.release();
        keepLockForever.get();
    }

    @Test
    public void allowNonExclusiveLocksToBeTakenOutConcurrently() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        persistentLock.acquireLock(PersistentLockName.of("deletionLock"), REASON, false);
        persistentLock.acquireLock(PersistentLockName.of("deletionLock"), REASON, false);
    }

    @Test
    public void forbidExclusiveLockIfNonExlusiveLockIsTaken() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        persistentLock.acquireLock(PersistentLockName.of("deletionLock"), REASON, false);

        expectedException.expect(PersistentLockIsTakenException.class);
        persistentLock.acquireLock(PersistentLockName.of("deletionLock"), REASON, true);
    }

    @Test
    public void forbidNonExclusiveLockIfExclusiveLockIsTaken() throws PersistentLockIsTakenException {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        persistentLock.acquireLock(PersistentLockName.of("deletionLock"), REASON, true);

        expectedException.expect(PersistentLockIsTakenException.class);
        persistentLock.acquireLock(PersistentLockName.of("deletionLock"), REASON, false);
    }
}
