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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class KvsBackedPersistentLockService implements PersistentLockService {
    private static final Logger log = LoggerFactory.getLogger(KvsBackedPersistentLockService.class);

    private final LockStore lockStore;

    @VisibleForTesting
    KvsBackedPersistentLockService(LockStore lockStore) {
        this.lockStore = lockStore;
    }

    public static PersistentLockService create(KeyValueService kvs) {
        LockStore lockStore = LockStore.create(kvs);
        return new KvsBackedPersistentLockService(lockStore);
    }

    @Override
    public PersistentLockServiceResponse acquireBackupLock(String reason) {
        Preconditions.checkNotNull(reason, "Please provide a reason for acquiring the lock.");
        try {
            return PersistentLockServiceResponse.successfulResponseWithLockEntry(lockStore.acquireBackupLock(reason));
        } catch (CheckAndSetException e) {
            LockEntry actualEntry = extractStoredLockEntry(e);
            log.error("Request failed. Stored persistent lock: {}", actualEntry);
            return PersistentLockServiceResponse.failureResponseWithMessage(
                    String.format("The lock has already been taken out; reason: %s", actualEntry.reason()));
        }
    }

    @Override
    public PersistentLockServiceResponse releaseLock(LockEntry lockEntry) {
        Preconditions.checkNotNull(lockEntry, "Please provide a LockEntry to release.");

        try {
            lockStore.releaseLock(lockEntry);
            return PersistentLockServiceResponse.successfulResponseWithMessage(
                    "The lock was released successfully.");
        } catch (CheckAndSetException e) {
            LockEntry actualEntry = extractStoredLockEntry(e);

            log.error("Request failed. Stored persistent lock: {}", actualEntry);
            log.error("Failed to release the persistent lock. This means that somebody already cleared this lock. "
                    + "You should investigate this, as this means your operation didn't necessarily hold the lock when "
                    + "it should have done.", e);

            String message = LockStore.LOCK_OPEN.equals(actualEntry)
                    ? "The lock has already been released."
                    : String.format("The lock doesn't match the lock that was taken out with reason: %s", actualEntry.reason());

            return PersistentLockServiceResponse.failureResponseWithMessage(message);
        }
    }

    private LockEntry extractStoredLockEntry(CheckAndSetException ex) {
        // Want a slightly different response if the lock was already open
        List<byte[]> actualValues = ex.getActualValues();
        if (actualValues == null || actualValues.size() != 1) {
            // Rethrow - something odd happened in the db, and here we _do_ want the log message/stack trace.
            throw ex;
        }

        byte[] actualValue = Iterables.getOnlyElement(actualValues);
        return LockEntry.fromStoredValue(actualValue);
    }
}
