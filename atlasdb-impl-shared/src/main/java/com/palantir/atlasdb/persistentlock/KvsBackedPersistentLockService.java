/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.remoting3.servers.jersey.WebPreconditions;

public class KvsBackedPersistentLockService implements PersistentLockService {
    private static final Logger log = LoggerFactory.getLogger(KvsBackedPersistentLockService.class);

    private final LockStore lockStore;

    @VisibleForTesting
    KvsBackedPersistentLockService(LockStore lockStore) {
        this.lockStore = lockStore;
    }

    public static PersistentLockService create(KeyValueService kvs) {
        return create(kvs, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static PersistentLockService create(KeyValueService kvs, boolean initializeAsync) {
        LockStore lockStore = LockStoreImpl.create(kvs, initializeAsync);
        return new KvsBackedPersistentLockService(lockStore);
    }

    @Override
    public PersistentLockId acquireBackupLock(String reason) {
        WebPreconditions.checkNotNull(reason, "Please provide a reason for acquiring the lock.");
        return PersistentLockId.of(lockStore.acquireBackupLock(reason).instanceId());
    }

    @Override
    public void releaseBackupLock(PersistentLockId lockId) {
        WebPreconditions.checkNotNull(lockId, "Please provide a PersistentLockId to release.");

        LockEntry lockToRelease = lockStore.getLockEntryWithLockId(lockId);

        try {
            lockStore.releaseLock(lockToRelease);
        } catch (CheckAndSetException e) {
            log.error("Failed to release the persistent lock. This means that somebody already cleared this lock. "
                    + "You should investigate this, as this means your operation didn't necessarily hold the lock when "
                    + "it should have done.", e);
            throw e;
        }
    }

}
