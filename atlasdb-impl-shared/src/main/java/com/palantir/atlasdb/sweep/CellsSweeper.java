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
package com.palantir.atlasdb.sweep;

import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class CellsSweeper {
    private static final Logger log = LoggerFactory.getLogger(CellsSweeper.class);

    private final TransactionManager txManager;
    private final KeyValueService keyValueService;
    private final PersistentLockService persistentLockService;
    private final long persistentLockRetryWaitMillis;
    private final Collection<Follower> followers;

    public CellsSweeper(
            TransactionManager txManager,
            KeyValueService keyValueService,
            PersistentLockService persistentLockService,
            long persistentLockRetryWaitMillis,
            Collection<Follower> followers) {
        this.txManager = txManager;
        this.keyValueService = keyValueService;
        this.persistentLockService = persistentLockService;
        this.persistentLockRetryWaitMillis = persistentLockRetryWaitMillis;
        this.followers = followers;
    }

    public CellsSweeper(
            TransactionManager txManager,
            KeyValueService keyValueService,
            Collection<Follower> followers) {
        this.txManager = txManager;
        this.keyValueService = keyValueService;
        this.followers = followers;

        this.persistentLockRetryWaitMillis = AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS;
        this.persistentLockService = PersistentLockService.create(keyValueService);
    }

    public void sweepCells(
            TableReference tableRef,
            Multimap<Cell, Long> cellTsPairsToSweep,
            Set<Cell> sentinelsToAdd) {
        if (cellTsPairsToSweep.isEmpty()) {
            return;
        }

        for (Follower follower : followers) {
            follower.run(txManager, tableRef, cellTsPairsToSweep.keySet(), Transaction.TransactionType.HARD_DELETE);
        }

        LockEntry lockEntry = acquirePersistentLockWithRetry();

        try {
            if (!sentinelsToAdd.isEmpty()) {
                keyValueService.addGarbageCollectionSentinelValues(
                        tableRef,
                        sentinelsToAdd);
            }

            keyValueService.delete(tableRef, cellTsPairsToSweep);
        } finally {
            releasePersistentLock(lockEntry);
        }
    }

    private LockEntry acquirePersistentLockWithRetry() {
        while (true) {
            try {
                LockEntry lockEntry = persistentLockService.acquireLock("Sweep");
                log.info("Successfully acquired persistent lock for sweep: {}", lockEntry);
                return lockEntry;
            } catch (CheckAndSetException e) {
                log.info("Failed to acquire persistent lock for sweep. Waiting and retrying.");
                waitForRetry();
            }
        }
    }

    private void waitForRetry() {
        try {
            Thread.sleep(persistentLockRetryWaitMillis);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    private void releasePersistentLock(LockEntry entry) {
        try {
            persistentLockService.releaseLock(entry);
        } catch (CheckAndSetException e) {
            log.error("Failed to release persistent lock {}. "
                    + "Either the lock was already released, or communications with the database failed.", entry, e);
        }
    }
}
