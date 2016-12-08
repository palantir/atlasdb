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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.common.base.Throwables;

public final class PersistentLock {
    private static final Logger log = LoggerFactory.getLogger(PersistentLock.class);

    private final LockStore lockStore;

    private PersistentLock(LockStore lockStore) {
        this.lockStore = lockStore;
    }

    public static PersistentLock create(LockStore lockStore) {
        return new PersistentLock(lockStore);
    }

    public static PersistentLock create(KeyValueService keyValueService) {
        LockStore lockStore = LockStore.create(keyValueService);
        return new PersistentLock(lockStore);
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

        lockStore.insertLockEntry(lockEntry);

        return verifyLockWasAcquired(lockEntry);
    }

    public void releaseLock(LockEntry lock) {
        lockStore.releaseLockEntry(lock);
    }

    public LockEntry releaseOnlyLock(PersistentLockName lockName) {
        Set<LockEntry> matchingLockEntries = allLockEntries().stream()
                .filter(lockEntry -> lockEntry.lockName().equals(lockName))
                .collect(Collectors.toSet());

        LockEntry onlyLock = Iterables.getOnlyElement(matchingLockEntries);
        releaseLock(onlyLock);
        return onlyLock;
    }


    public Set<LockEntry> allLockEntries() {
        return lockStore.allLockEntries();
    }

    private LockEntry generateUniqueLockEntry(PersistentLockName lockName, String reason, boolean exclusive) {
        long thisId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        return LockEntry.of(lockName, thisId, reason, exclusive);
    }

    private LockEntry verifyLockWasAcquired(LockEntry desiredLock) throws PersistentLockIsTakenException {
        Set<LockEntry> relevantLocks = allRelevantLockEntries(desiredLock.lockName());
        Preconditions.checkState(relevantLocks.contains(desiredLock), "Lock was not properly inserted");

        return verifyLockDoesNotConflict(desiredLock, relevantLocks);
    }

    private Set<LockEntry> allRelevantLockEntries(PersistentLockName lock) {
        return Sets.filter(allLockEntries(), lockEntry -> lockEntry.lockName().equals(lock));
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

    private Set<LockEntry> retainExclusiveLocks(Set<LockEntry> conflictingLocks) {
        return Sets.filter(conflictingLocks, LockEntry::exclusive);
    }
}
