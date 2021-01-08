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
package com.palantir.atlasdb.persistentlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class LockStoreTest {
    private static final String REASON = "reason";
    private static final UUID OTHER_ID = UUID.fromString("4-8-15-16-23" /*-42*/);
    private static final String OTHER_REASON = "bar";

    private InMemoryKeyValueService kvs;
    private LockStoreImpl lockStore;

    @Before
    public void setUp() throws Exception {
        kvs = spy(new InMemoryKeyValueService(false));
        lockStore = LockStoreImpl.createImplForTest(kvs);
    }

    @Test
    public void createsPersistedLocksTable() {
        LockStoreImpl.create(kvs);
        verify(kvs, atLeastOnce()).createTable(eq(AtlasDbConstants.PERSISTED_LOCKS_TABLE), any(byte[].class));
    }

    @Test
    public void lockIsInitiallyOpen() {
        Set<LockEntry> lockEntries = lockStore.allLockEntries();

        assertThat(lockEntries).containsExactly(LockStoreImpl.LOCK_OPEN);
    }

    @Test
    public void noErrorIfLockOpenedWhileCreatingTable() {
        doThrow(new CheckAndSetException("foo", null, null, ImmutableList.of()))
                .when(kvs)
                .checkAndSet(anyObject());

        new LockStoreImpl.LockStorePopulator(kvs).populate(); // should not throw
    }

    @Test
    public void canAcquireLock() throws Exception {
        LockEntry lockEntry = lockStore.acquireBackupLock(REASON);

        assertThat(lockStore.allLockEntries()).containsExactly(lockEntry);
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotAcquireLockTwice() throws Exception {
        lockStore.acquireBackupLock(REASON);
        lockStore.acquireBackupLock(REASON);
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotAcquireLockTwiceForDifferentReasons() throws Exception {
        lockStore.acquireBackupLock(REASON);
        lockStore.acquireBackupLock("other-reason");
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotAcquireLockThatWasTakenOutByAnotherStore() throws Exception {
        LockStore otherLockStore = LockStoreImpl.create(kvs);
        otherLockStore.acquireBackupLock("grabbed by other store");

        lockStore.acquireBackupLock(REASON);
    }

    @Test
    public void canViewLockAcquiredByAnotherLockStore() {
        LockStore otherLockStore = LockStoreImpl.create(kvs);
        LockEntry otherLockEntry = otherLockStore.acquireBackupLock("grabbed by other store");

        assertThat(lockStore.allLockEntries()).containsExactly(otherLockEntry);
    }

    @Test
    public void releaseLockPopulatesStoreWithOpenValue() throws Exception {
        LockEntry lockEntry = lockStore.acquireBackupLock(REASON);
        lockStore.releaseLock(lockEntry);

        assertThat(lockStore.allLockEntries()).containsExactly(LockStoreImpl.LOCK_OPEN);
    }

    @Test(expected = CheckAndSetException.class)
    public void cannotReleaseLockWhenLockIsOpen() throws Exception {
        LockEntry otherLockEntry = ImmutableLockEntry.builder()
                .lockName("name")
                .instanceId(OTHER_ID)
                .reason(OTHER_REASON)
                .build();
        lockStore.releaseLock(otherLockEntry);
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotReleaseNonExistentLock() throws Exception {
        LockEntry lockEntry = lockStore.acquireBackupLock(REASON);

        LockEntry otherLockEntry = ImmutableLockEntry.builder()
                .from(lockEntry)
                .instanceId(OTHER_ID)
                .reason(OTHER_REASON)
                .build();
        lockStore.releaseLock(otherLockEntry);
    }

    @Test
    public void canReleaseLockAndReacquire() throws Exception {
        LockEntry lockEntry = lockStore.acquireBackupLock(REASON);
        lockStore.releaseLock(lockEntry);

        lockStore.acquireBackupLock(REASON);
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotReacquireAfterReleasingDifferentLock() throws Exception {
        LockEntry lockEntry = lockStore.acquireBackupLock(REASON);

        LockEntry otherLockEntry = ImmutableLockEntry.builder()
                .from(lockEntry)
                .instanceId(OTHER_ID)
                .reason(OTHER_REASON)
                .build();
        try {
            lockStore.releaseLock(otherLockEntry);
        } catch (CheckAndSetException e) {
            // expected
        }

        lockStore.acquireBackupLock(REASON);
    }
}
