/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.List;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLockId;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;

// TODO move to persistentlock package?
public class PersistentLockManager {
    private static final Logger log = LoggerFactory.getLogger(PersistentLockManager.class);

    private final PersistentLockService persistentLockService;
    private final long persistentLockRetryWaitMillis;

    private static final String ACQUIRE_FAILURE_METRIC_NAME = "unableToAcquirePersistentLock";

    private final MetricsManager metricsManager;
    private final Meter lockFailureMeter;

    @VisibleForTesting
    @GuardedBy("this")
    PersistentLockId lockId;

    @GuardedBy("this")
    private boolean isShutDown = false;

    public PersistentLockManager(PersistentLockService persistentLockService, long persistentLockRetryWaitMillis) {
        this.persistentLockService = persistentLockService;
        this.persistentLockRetryWaitMillis = persistentLockRetryWaitMillis;
        this.metricsManager = new MetricsManager();
        this.lockFailureMeter = metricsManager.registerMeter(this.getClass(), null, ACQUIRE_FAILURE_METRIC_NAME);
        this.lockId = null;
    }

    public synchronized void shutdown() {
        log.info("Shutting down...");
        isShutDown = true;
        if (lockId != null) {
            releasePersistentLock();
        }
        log.info("Shutdown completed!");
    }

    // We don't synchronize this method, to avoid deadlocking if {@link #shutdown} is called while attempting
    // to acquire a lock.
    public void acquirePersistentLockWithRetry() {
        while (!tryAcquirePersistentLock()) {
            waitForRetry();
        }
    }

    @VisibleForTesting
    synchronized boolean tryAcquirePersistentLock() {
        Preconditions.checkState(!isShutDown,
                "This PersistentLockManager is shut down, and cannot be used to acquire locks.");

        try {
            lockId = persistentLockService.acquireBackupLock("Sweep");
            log.info("Successfully acquired persistent lock for sweep: {}", SafeArg.of("lockId", lockId));
            return true;
        } catch (CheckAndSetException e) {
            List<byte[]> actualValues = e.getActualValues();
            if (!actualValues.isEmpty()) {
                // This should be the only element, otherwise something really odd happened.
                LockEntry actualEntry = LockEntry.fromStoredValue(actualValues.get(0));
                log.debug("CAS failed on lock acquire. We thought the lockId was {}, and the database has {}",
                        SafeArg.of("lockId", lockId),
                        SafeArg.of("actualEntry", actualEntry));
                if (lockId != null && actualEntry.instanceId().equals(lockId.value())) {
                    // We tried to acquire while already holding the lock. Welp - but we still have the lock.
                    log.info("Attempted to acquire the a new lock when we already held a lock."
                            + " The acquire failed, but our lock is still valid, so we still hold the lock.");
                    return true;
                } else {
                    // In this case, some other process holds the lock. Therefore, we don't hold the lock.
                    lockId = null;
                }
            }

            lockFailureMeter.mark();
            log.info("Failed to acquire persistent lock for sweep. Waiting and retrying.");
            return false;
        } catch (NotInitializedException e) {
            log.info("The LockStore is not initialized yet. Waiting and retrying.");
            return false;
        }
    }

    public synchronized void releasePersistentLock() {
        if (lockId == null) {
            log.info("Called releasePersistentLock, but no lock has been taken! Returning.");
            return;
        }

        log.info("Releasing persistent lock {}", SafeArg.of("lockId", lockId));
        try {
            persistentLockService.releaseBackupLock(lockId);
            lockId = null;
        } catch (CheckAndSetException e) {
            log.error("Failed to release persistent lock {}. The lock must have been released from under us. "
                            + "Future sweeps should correctly be able to re-acquire the lock.",
                    SafeArg.of("lockId", lockId), e);
            lockId = null;
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
