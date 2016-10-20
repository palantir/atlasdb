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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

public final class LockStore {
    public static final long LOCKS_TIMESTAMP = 0L;

    private static final Logger log = LoggerFactory.getLogger(LockStore.class);

    private final KeyValueService keyValueService;

    private LockStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    public static LockStore create(KeyValueService keyValueService) {
        LockStore lockStore = new LockStore(keyValueService);
        lockStore.createPersistedLocksTable();
        return lockStore;
    }

    private void createPersistedLocksTable() {
        keyValueService.createTable(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    public Set<LockEntry> allLockEntries() {
        Set<RowResult<Value>> allLockRows = ImmutableSet.copyOf(keyValueService.getRange(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, RangeRequest.all(), LOCKS_TIMESTAMP + 1));

        Set<LockEntry> allLockEntriesIncludingTombstones = allLockRows.stream()
                .map(LockEntry::fromRowResult)
                .collect(Collectors.toSet());

        Set<LockEntry> tombstonedLockEntries = allLockEntriesIncludingTombstones.stream()
                .filter(LockEntry::tombstoned)
                .collect(Collectors.toSet());

        attemptToCleanupTombstonedEntries(tombstonedLockEntries);

        return Sets.difference(allLockEntriesIncludingTombstones, tombstonedLockEntries);
    }

    public void releaseLockEntry(LockEntry lockEntry) {
        log.debug("Releasing persistent lock {}", lockEntry);
        keyValueService.put(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.writeTombstoneMap(), LOCKS_TIMESTAMP);
    }

    public void insertLockEntry(LockEntry lockEntry) {
        log.debug("Attempting to acquire persistent lock {}", lockEntry);
        keyValueService.put(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.insertionMap(), LOCKS_TIMESTAMP);
    }

    private void attemptToCleanupTombstonedEntries(Set<LockEntry> tombstonedLockEntries) {
        log.debug("Attempting to clean up {} tombstoned persistent lock entries", tombstonedLockEntries.size());
        try {
            for (LockEntry lockEntry : tombstonedLockEntries) {
                keyValueService.delete(
                        AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.deletionMapWithTimestamp(LOCKS_TIMESTAMP));
            }
        } catch (Exception e) {
            log.info("Failed to clean up one or more tombstoned persistent lock entry. "
                    + "This should not affect normal operation. "
                    + "This could be because delete operations require CONSISTENCY_ALL and one of your nodes is down. "
                    + "See debug-level logging for the exact error. ");
            log.debug("Failed to clean up a tombstoned persistent lock entry for the following reason: ", e);
        }
    }
}
