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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.Throwables;

public class PersistentLock {
    public static final long LOCKS_TIMESTAMP = 0L;

    private static final Logger log = LoggerFactory.getLogger(PersistentLock.class);

    private final KeyValueService keyValueService;

    public PersistentLock(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;

        createPersistedLocksTableIfNotExists();
    }

    public interface Action {
        void execute() throws Exception;
    }

    public <T> T runWithLock(
            Supplier<T> supplier,
            PersistentLockName lock,
            String reason) throws PersistentLockIsTakenException {
        LockEntry acquiredLock = acquireLock(lock, reason);

        try {
            return supplier.get();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            releaseLock(acquiredLock);
        }
    }

    public void runWithLock(
            Action action,
            PersistentLockName lock,
            String reason) throws PersistentLockIsTakenException {
        runWithLock(() -> {
            try {
                action.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        }, lock, reason);
    }

    public LockEntry acquireLock(PersistentLockName lockName, String reason) throws PersistentLockIsTakenException {
        return acquireLock(lockName, reason, true);
    }

    public LockEntry acquireLock(PersistentLockName lockName, String reason, boolean exclusive) throws PersistentLockIsTakenException {
        long thisId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        LockEntry lockEntry = LockEntry.of(lockName, thisId, reason, exclusive);

        insertLockEntry(lockEntry);

        return verifyLockWasAcquired(lockEntry);
    }

    public void releaseLock(LockEntry lock) {
        log.debug("Releasing persistent lock " + lock);
        keyValueService.delete(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lock.deletionMapWithTimestamp(LOCKS_TIMESTAMP));
    }

    private LockEntry verifyLockWasAcquired(LockEntry desiredLock) throws PersistentLockIsTakenException {
        List<LockEntry> relevantLocks = allRelevantLockEntries(desiredLock.lockName());
        Preconditions.checkState(relevantLocks.contains(desiredLock), "Lock was not properly inserted");

        List<LockEntry> conflictingLocks = removeSingleLock(desiredLock, relevantLocks);

        if (!desiredLock.exclusive()) {
            conflictingLocks = retainExclusiveLocks(conflictingLocks);
        }
        return verifyLockDoesNotConflict(desiredLock, conflictingLocks);
    }

    private List<LockEntry> retainExclusiveLocks(List<LockEntry> conflictingLocks) {
        return conflictingLocks.stream()
                .filter(LockEntry::exclusive)
                .collect(Collectors.toList());
    }

    private List<LockEntry> removeSingleLock(LockEntry desiredLock, List<LockEntry> relevantLocks) {
        return relevantLocks.stream()
                .filter(otherLock -> !otherLock.equals(desiredLock))
                .collect(Collectors.toList());
    }

    private LockEntry verifyLockDoesNotConflict(
            LockEntry desiredLock,
            List<LockEntry> conflictingLocks) throws PersistentLockIsTakenException {

        if (conflictingLocks.isEmpty()) {
            log.debug("Acquired persistent lock " + desiredLock);
            return desiredLock;
        } else {
            log.info("Failed to acquire persistent lock " + desiredLock);
            releaseLock(desiredLock);
            throw new PersistentLockIsTakenException(conflictingLocks);
        }
    }

    private void insertLockEntry(LockEntry lockEntry) {
        log.debug("Attempting to acquire persistent lock " + lockEntry);
        keyValueService.put(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.insertionMap(), LOCKS_TIMESTAMP);
    }

    private List<LockEntry> allRelevantLockEntries(PersistentLockName lock) {
        return allLockEntries().stream()
                .filter(lockEntry -> lockEntry.lockName().equals(lock))
                .collect(Collectors.toList());
    }

    public List<LockEntry> allLockEntries() {
        List<RowResult<Value>> allLockRows = ImmutableList.copyOf(keyValueService.getRange(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, RangeRequest.all(), LOCKS_TIMESTAMP + 1));

        return allLockRows.stream()
                .map(LockEntry::fromRowResult)
                .collect(Collectors.toList());
    }

    private void createPersistedLocksTableIfNotExists() {
        if (!keyValueService.getAllTableNames().contains(AtlasDbConstants.PERSISTED_LOCKS_TABLE)) {
            keyValueService.createTable(
                    AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        }
    }
}
