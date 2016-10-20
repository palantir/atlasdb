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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class LockStoreShould {
    private LockStore lockStore;
    private KeyValueService keyValueService;

    private final LockEntry lockEntry = LockEntry.of(PersistentLockName.of("lockName"), 1234);

    @Before
    public void setup() {
        keyValueService = spy(new InMemoryKeyValueService(false));
        lockStore = LockStore.create(keyValueService);
    }

    @Test
    public void createPersistedLocksTable() {
        LockStore.create(keyValueService);

        verify(keyValueService, atLeastOnce())
                .createTable(eq(AtlasDbConstants.PERSISTED_LOCKS_TABLE), any(byte[].class));
    }

    @Test
    public void insertedEntriesShouldBeVisible() {
        lockStore.insertLockEntry(lockEntry);

        Set<LockEntry> lockEntries = lockStore.allLockEntries();

        assertThat(lockEntries, hasItems(lockEntry));
    }

    @Test
    public void deletedEntriesShouldNotBeVisible() {
        lockStore.insertLockEntry(lockEntry);
        lockStore.releaseLockEntry(lockEntry);

        Set<LockEntry> lockEntries = lockStore.allLockEntries();

        assertThat(lockEntries, not(hasItems(lockEntry)));
    }

    @Test
    public void deletedEntriesShouldNotBeVisibleIfDeletionFails() {
        doThrow(new RuntimeException("deletion failed"))
                .when(keyValueService).delete(any(TableReference.class), any(Multimap.class));
        lockStore.insertLockEntry(lockEntry);
        lockStore.releaseLockEntry(lockEntry);

        Set<LockEntry> lockEntries = lockStore.allLockEntries();

        assertThat(lockEntries, not(hasItems(lockEntry)));
    }

    @Test
    public void deletedEntriesShouldNotContainTombstonedEntries() {
        doThrow(new RuntimeException("deletion failed"))
                .when(keyValueService).delete(any(TableReference.class), any(Multimap.class));
        lockStore.insertLockEntry(lockEntry);
        lockStore.releaseLockEntry(lockEntry);

        Set<LockEntry> lockEntries = lockStore.allLockEntries();

        assertThat(lockEntries.size(), equalTo(0));
    }
}
