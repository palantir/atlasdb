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
package com.palantir.atlasdb.sweep;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLockId;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO move to persistentlock package?
public class PersistentLockManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PersistentLockManager.class);

    private final PersistentLockService persistentLockService;
    private final long persistentLockRetryWaitMillis;

    private static final String ACQUIRE_FAILURE_METRIC_NAME = "unableToAcquirePersistentLock";

    private final MetricsManager metricsManager;
    private final Meter lockFailureMeter;

    @VisibleForTesting
    @GuardedBy("this")
    PersistentLockId lockId;

    /* This is used to prevent the following error case:
     * 1. Sweep thread 1 grabs the lock for deleting stuff
     * 2. Sweep thread 2 grabs the lock for deleting stuff
     * 3. Sweep thread 1 releases the lock
     * 4. A backup starts
     * 5. Sweep thread 2 releases the lock
     *
     * A backup taken between steps 4 and 5 is not guaranteed to be consistent, because sweep may be deleting data
     * that it relies on.
     */
    @GuardedBy("this")
    volatile int referenceCount = 0;

    @GuardedBy("this")
    private boolean isShutDown = false;

    public PersistentLockManager(MetricsManager metricsManager,
            PersistentLockService persistentLockService, long persistentLockRetryWaitMillis) {
        this.persistentLockService = persistentLockService;
        this.persistentLockRetryWaitMillis = persistentLockRetryWaitMillis;
        this.metricsManager = metricsManager;
        this.lockFailureMeter = metricsManager.registerOrGetMeter(this.getClass(), null, ACQUIRE_FAILURE_METRIC_NAME);
        this.lockId = null;
    }

    @Override
    public void close() {
        shutdown();
    }

    public synchronized void shutdown() {
        log.info("Shutting down...");

        try {
            isShutDown = true;
            while (lockId != null) {
                releasePersistentLock();
            }
            log.info("Shutdown completed!");
        } catch (Exception e) {
            logFailedShutdown(e);
        }
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
            referenceCount++;
            log.info("Successfully acquired persistent lock for sweep: {}", SafeArg.of("lockId", lockId));
            return true;
        } catch (CheckAndSetException e) {
            List<byte[]> actualValues = e.getActualValues();
            if (!actualValues.isEmpty()) {
                // This should be the only element, otherwise something really odd happened.
                LockEntry actualEntry = LockEntry.fromStoredValue(actualValues.get(0));
                log.info("CAS failed on lock acquire. We thought the lockId was {}, and the database has {}",
                        SafeArg.of("lockId", lockId),
                        SafeArg.of("actualEntry", actualEntry));
                if (lockId != null && actualEntry.instanceId().equals(lockId.value())) {
                    // We tried to acquire while already holding the lock. Welp - but we still have the lock.
                    referenceCount++;
                    log.info("Attempted to acquire a new lock when we already held a lock."
                            + " The acquire failed, but our lock is still valid, so we still hold the lock.");
                    return true;
                } else {
                    // In this case, some other process holds the lock. Therefore, we don't hold the lock.
                    referenceCount = 0;
                    lockId = null;
                }
            }

            // This is expected if backups are currently being taken.
            // Not expected if we this continues to be logged for a long period of time — indicates a log has been
            // lost (a service has been bounced and the shutdown hook did not run), and we need to manually clear it,
            // via the CLI or the endpoint.
            lockFailureMeter.mark();
            log.warn("Failed to acquire persistent lock for sweep. Waiting and retrying.");
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

        referenceCount--;
        if (referenceCount <= 0) {
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
        } else {
            log.info("Not releasing the persistent lock, because {} threads still hold it.",
                    SafeArg.of("numLockHolders", referenceCount));
        }
    }

    private void waitForRetry() {
        try {
            Thread.sleep(persistentLockRetryWaitMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void logFailedShutdown(Exception exception) {
        log.warn("An exception occurred while shutting down. This means that we had the backup lock out when "
                        + "the shutdown was triggered, but failed to release it. If this is the case, sweep "
                        + "or backup may fail to take out the lock in future. If this happens consistently, "
                        + "consult the following documentation on how to release the dead lock: "
                        + "https://palantir.github.io/atlasdb/html/troubleshooting/index.html#clearing-the-backup-lock",
                exception);
    }
}
