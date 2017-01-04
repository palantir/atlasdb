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
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class LockStoreTest {
    private static final String REASON = "reason";

    private KeyValueService kvs;
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
    public void canAcquireLock() {
        LockEntry lockEntry = lockStore.acquireLock(REASON);

        assertThat(lockStore.allLockEntries(), contains(lockEntry));
    }

    @Test(expected = KeyAlreadyExistsException.class)
    public void canNotAcquireLockTwice() {
        lockStore.acquireLock(REASON);
        lockStore.acquireLock(REASON);
    }

    @Test(expected = KeyAlreadyExistsException.class)
    public void canNotAcquireLockTwiceForDifferentReasons() {
        lockStore.acquireLock(REASON);
        lockStore.acquireLock("other-reason");
    }

    @Test(expected = KeyAlreadyExistsException.class)
    public void canNotAcquireLockThatWasTakenOutByAnotherStore() {
        LockStore otherLockStore = LockStore.create(kvs);
        otherLockStore.acquireLock("grabbed by other store");

        lockStore.acquireLock(REASON);
    }

    @Test
    public void releaseLockRemovesItFromEntryList() {
        LockEntry lockEntry = lockStore.acquireLock(REASON);
        lockStore.releaseLock(lockEntry);

        assertThat(lockStore.allLockEntries(), empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void canNotReleaseNonExistentLock() {
        LockEntry lockEntry = lockStore.acquireLock(REASON);

        LockEntry otherLockEntry = ImmutableLockEntry.builder().from(lockEntry).lockId("42").reason("other").build();
        lockStore.releaseLock(otherLockEntry);
    }

    @Test
    public void canReleaseLockAndReacquire() {
        LockEntry lockEntry = lockStore.acquireLock(REASON);
        lockStore.releaseLock(lockEntry);

        lockStore.acquireLock(REASON);
    }

    @Test(expected = KeyAlreadyExistsException.class)
    public void canNotReacquireAfterReleasingDifferentLock() {
        LockEntry lockEntry = lockStore.acquireLock(REASON);

        LockEntry otherLockEntry = ImmutableLockEntry.builder().from(lockEntry).lockId("42").reason("other").build();
        try {
            lockStore.releaseLock(otherLockEntry);
        } catch (IllegalArgumentException e) {
            // expected
        }

        lockStore.acquireLock(REASON);
    }
}
