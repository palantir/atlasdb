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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class PersistentLockServiceTest {
    private PersistentLockService service;
    private LockStore lockStore;

    @Before
    public void setUp() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        lockStore = spy(LockStore.create(kvs));
        service = new PersistentLockService(lockStore);
    }

    @Test
    public void canCreatePersistentLockService() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        PersistentLockService pls = PersistentLockService.create(kvs);
        assertNotNull(pls);
    }

    @Test
    public void canAcquireLock() throws CheckAndSetException {
        service.acquireLock("for-test");
        verify(lockStore, times(1)).acquireLock("for-test");
    }

    @Test
    public void canReleaseLock() {
        LockEntry entry = lockStore.acquireLock("reason");
        service.releaseLock(entry);

        verify(lockStore, times(1)).releaseLock(entry);
    }
}
