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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import org.junit.Before;
import org.junit.Test;

public class KvsBackedPersistentLockServiceTest {
    private static final String TEST_REASON = "for-test";

    private PersistentLockService service;
    private LockStoreImpl lockStore;

    @Before
    public void setUp() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        lockStore = spy(LockStoreImpl.createImplForTest(kvs));
        service = new KvsBackedPersistentLockService(lockStore);
    }

    @Test
    public void canCreatePersistentLockService() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        PersistentLockService pls = KvsBackedPersistentLockService.create(kvs);
        assertThat(pls).isNotNull();
    }

    @Test
    public void canAcquireLock() throws CheckAndSetException {
        service.acquireBackupLock(TEST_REASON);
        verify(lockStore, times(1)).acquireBackupLock(TEST_REASON);
    }

    @Test
    public void canReleaseLock() {
        PersistentLockId lockId = service.acquireBackupLock(TEST_REASON);
        LockEntry lockEntry = Iterables.getOnlyElement(lockStore.allLockEntries());
        service.releaseBackupLock(lockId);

        verify(lockStore, times(1)).releaseLock(eq(lockEntry));
    }
}
