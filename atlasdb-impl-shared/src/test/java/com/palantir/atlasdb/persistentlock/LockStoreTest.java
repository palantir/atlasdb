/**
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
package com.palantir.atlasdb.persistentlock;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class LockStoreTest {
    private static final String REASON = "reason";
    private static final UUID OTHER_ID = UUID.fromString("4-8-15-16-23"/*-42*/);
    private static final String OTHER_REASON = "bar";

    private InMemoryKeyValueService kvs;
    private LockStore lockStore;

    @Before
    public void setUp() throws Exception {
        kvs = spy(new InMemoryKeyValueService(false));
        lockStore = LockStore.create(kvs);
    }

    @Test
    public void createsPersistedLocksTable() {
        LockStore.create(kvs);
        verify(kvs, atLeastOnce()).createTable(eq(AtlasDbConstants.PERSISTED_LOCKS_TABLE), any(byte[].class));
    }

    @Test
    public void lockIsInitiallyOpen() {
        Set<LockEntry> lockEntries = lockStore.allLockEntries();

        assertThat(lockEntries, contains(LockStore.LOCK_OPEN));
    }

    @Test
    public void noErrorIfLockOpenedWhileCreatingTable() {
        doThrow(new CheckAndSetException("foo", null, null, ImmutableList.of())).when(kvs).checkAndSet(anyObject());

        new LockStore.LockStorePopulator(kvs).populate(); // should not throw
    }

    @Test
    public void canAcquireLock() throws Exception {
        LockEntry lockEntry = lockStore.acquireLock(REASON);

        assertThat(lockStore.allLockEntries(), contains(lockEntry));
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotAcquireLockTwice() throws Exception {
        lockStore.acquireLock(REASON);
        lockStore.acquireLock(REASON);
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotAcquireLockTwiceForDifferentReasons() throws Exception {
        lockStore.acquireLock(REASON);
        lockStore.acquireLock("other-reason");
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotAcquireLockThatWasTakenOutByAnotherStore() throws Exception {
        LockStore otherLockStore = LockStore.create(kvs);
        otherLockStore.acquireLock("grabbed by other store");

        lockStore.acquireLock(REASON);
    }

    @Test
    public void canViewLockAcquiredByAnotherLockStore() {
        LockStore otherLockStore = LockStore.create(kvs);
        LockEntry otherLockEntry = otherLockStore.acquireLock("grabbed by other store");

        assertThat(lockStore.allLockEntries(), contains(otherLockEntry));
    }

    @Test
    public void releaseLockPopulatesStoreWithOpenValue() throws Exception {
        LockEntry lockEntry = lockStore.acquireLock(REASON);
        lockStore.releaseLock(lockEntry);

        assertThat(lockStore.allLockEntries(), contains(LockStore.LOCK_OPEN));
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
        LockEntry lockEntry = lockStore.acquireLock(REASON);

        LockEntry otherLockEntry = ImmutableLockEntry.builder()
                .from(lockEntry)
                .instanceId(OTHER_ID)
                .reason(OTHER_REASON)
                .build();
        lockStore.releaseLock(otherLockEntry);
    }

    @Test
    public void canReleaseLockAndReacquire() throws Exception {
        LockEntry lockEntry = lockStore.acquireLock(REASON);
        lockStore.releaseLock(lockEntry);

        lockStore.acquireLock(REASON);
    }

    @Test(expected = CheckAndSetException.class)
    public void canNotReacquireAfterReleasingDifferentLock() throws Exception {
        LockEntry lockEntry = lockStore.acquireLock(REASON);

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

        lockStore.acquireLock(REASON);
    }
}
