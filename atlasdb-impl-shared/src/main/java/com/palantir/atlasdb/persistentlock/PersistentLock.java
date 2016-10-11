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

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.Throwables;

public final class PersistentLock {
    private static final long LOCKS_TIMESTAMP = 0L;

    private static final Logger log = LoggerFactory.getLogger(PersistentLock.class);

    private final KeyValueService keyValueService;

    private PersistentLock(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    public static PersistentLock create(KeyValueService keyValueService) {
        PersistentLock persistentLock = new PersistentLock(keyValueService);
        persistentLock.createPersistedLocksTable();
        return persistentLock;
    }

    public <T> T runWithExclusiveLock(
            Supplier<T> supplier,
            PersistentLockName lock,
            String reason) throws PersistentLockIsTakenException {
        return runWithLockInternal(supplier, lock, reason, true);
    }

    public <T> T runWithNonExclusiveLock(
            Supplier<T> supplier,
            PersistentLockName lock,
            String reason) throws PersistentLockIsTakenException {
        return runWithLockInternal(supplier, lock, reason, false);
    }

    private <T> T runWithLockInternal(
            Supplier<T> supplier,
            PersistentLockName lock,
            String reason,
            boolean exclusive) throws PersistentLockIsTakenException {
        LockEntry acquiredLock = acquireLock(lock, reason, exclusive);

        try {
            return supplier.get();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            releaseLock(acquiredLock);
        }
    }

    public LockEntry acquireLock(PersistentLockName lockName, String reason) throws PersistentLockIsTakenException {
        return acquireLock(lockName, reason, true);
    }

    public LockEntry acquireLock(
            PersistentLockName lockName, String reason, boolean exclusive) throws PersistentLockIsTakenException {
        LockEntry lockEntry = generateUniqueLockEntry(lockName, reason, exclusive);

        insertLockEntry(lockEntry);

        return verifyLockWasAcquired(lockEntry);
    }

    public void releaseLock(LockEntry lock) {
        log.debug("Releasing persistent lock {}", lock);
        keyValueService.delete(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lock.deletionMapWithTimestamp(LOCKS_TIMESTAMP));
    }

    private LockEntry generateUniqueLockEntry(PersistentLockName lockName, String reason, boolean exclusive) {
        long thisId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        return LockEntry.of(lockName, thisId, reason, exclusive);
    }

    private void insertLockEntry(LockEntry lockEntry) {
        log.debug("Attempting to acquire persistent lock {}", lockEntry);
        keyValueService.put(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.insertionMap(), LOCKS_TIMESTAMP);
    }

    private LockEntry verifyLockWasAcquired(LockEntry desiredLock) throws PersistentLockIsTakenException {
        Set<LockEntry> relevantLocks = allRelevantLockEntries(desiredLock.lockName());
        Preconditions.checkState(relevantLocks.contains(desiredLock), "Lock was not properly inserted");

        return verifyLockDoesNotConflict(desiredLock, relevantLocks);
    }

    private Set<LockEntry> retainExclusiveLocks(Set<LockEntry> conflictingLocks) {
        return conflictingLocks.stream()
                .filter(LockEntry::exclusive)
                .collect(Collectors.toSet());
    }

    private LockEntry verifyLockDoesNotConflict(
            LockEntry desiredLock,
            Set<LockEntry> relevantLocks) throws PersistentLockIsTakenException {
        Set<LockEntry> conflictingLocks = getConflictingLocks(desiredLock, relevantLocks);

        if (conflictingLocks.isEmpty()) {
            log.debug("Acquired persistent lock {}", desiredLock);
            return desiredLock;
        } else {
            log.info("Failed to acquire persistent lock {}", desiredLock);
            releaseLock(desiredLock);
            throw new PersistentLockIsTakenException(conflictingLocks);
        }
    }

    private Set<LockEntry> getConflictingLocks(LockEntry desiredLock, Set<LockEntry> relevantLocks) {
        Set<LockEntry> conflictingLocks = Sets.difference(relevantLocks, ImmutableSet.of(desiredLock));

        if (!desiredLock.exclusive()) {
            conflictingLocks = retainExclusiveLocks(conflictingLocks);
        }
        return conflictingLocks;
    }

    private Set<LockEntry> allRelevantLockEntries(PersistentLockName lock) {
        return allLockEntries().stream()
                .filter(lockEntry -> lockEntry.lockName().equals(lock))
                .collect(Collectors.toSet());
    }

    public Set<LockEntry> allLockEntries() {
        Set<RowResult<Value>> allLockRows = ImmutableSet.copyOf(keyValueService.getRange(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, RangeRequest.all(), LOCKS_TIMESTAMP + 1));

        return allLockRows.stream()
                .map(LockEntry::fromRowResult)
                .collect(Collectors.toSet());
    }

    private void createPersistedLocksTable() {
        keyValueService.createTable(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }
}
