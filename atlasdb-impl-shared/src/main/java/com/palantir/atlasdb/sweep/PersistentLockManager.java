/*
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
package com.palantir.atlasdb.sweep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.persistentlock.PersistentLockId;
import com.palantir.atlasdb.persistentlock.PersistentLockService;

// TODO add tests!
// TODO move to persistentlock package?
public class PersistentLockManager {
    private static final Logger log = LoggerFactory.getLogger(PersistentLockManager.class);

    private final PersistentLockService persistentLockService;

    private PersistentLockId lockId;
    private long persistentLockRetryWaitMillis;

    public PersistentLockManager(PersistentLockService persistentLockService, long persistentLockRetryWaitMillis) {
        this.persistentLockService = persistentLockService;
        this.persistentLockRetryWaitMillis = persistentLockRetryWaitMillis;

        this.lockId = null;
    }

    public synchronized void acquirePersistentLockWithRetry() {
        // TODO what should happen if we've already acquired a lock?

        while (true) {
            try {
                lockId = persistentLockService.acquireBackupLock("Sweep");
                log.info("Successfully acquired persistent lock for sweep: {}", lockId);
                return;
            } catch (CheckAndSetException e) {
                log.info("Failed to acquire persistent lock for sweep. Waiting and retrying.");
                waitForRetry();
            }
        }
    }

    public synchronized void releasePersistentLock() {
        // TODO what should happen if we have no lock?
        try {
            persistentLockService.releaseBackupLock(lockId);
            lockId = null;
        } catch (CheckAndSetException e) {
            log.error("Failed to release persistent lock {}. "
                    + "Either the lock was already released, or communications with the database failed.", lockId, e);
        }
    }

    private void waitForRetry() {
        try {
            Thread.sleep(persistentLockRetryWaitMillis);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}
