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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

/**
 * LockStore manages {@link LockEntry} objects, specifically for the "Deletion Lock" (to be taken out by backup and
 * sweep).
 *
 * The PERSISTED_LOCKS_TABLE contains exactly one element, either LOCK_OPEN or (lock taken for some REASON), giving
 * rise to a state machine where we hop between the central "OPEN" state and some "taken because REASON" state:
 *
 * "taken for SWEEP (uuid1)" <- - - - -> OPEN < - - - -> "taken for BACKUP (uuid2)"
 *
 * acquireLock(REASON) attempts to move the lock state from "OPEN" to "taken because REASON", returning a LockEntry
 * that holds a unique identifier for that occasion of taking the lock.
 *
 * releaseLock(LockEntry) attempts to move the state from "taken because REASON" to "OPEN", but only succeeds if the
 * LockEntry currently stored matches the one passed in, ensuring that nobody released the lock when we weren't looking.
 *
 * Upon creating the PERSISTED_LOCKS_TABLE, we attempt to enter this state machine by populating the table.
 * If we fail to do this, it's because someone else also created the table, and populated it before we did.
 * This is actually OK - all we care about is that we're in the state machine _somewhere_.
 */
@SuppressWarnings("checkstyle:FinalClass") // Non-final as we'd like to mock it.
public class LockStore {
    private static final Logger log = LoggerFactory.getLogger(LockStore.class);

    private static final String DELETION_LOCK_NAME = "DeletionLock";
    private final KeyValueService keyValueService;

    public static final LockEntry LOCK_OPEN = ImmutableLockEntry.builder()
            .lockName(DELETION_LOCK_NAME)
            .instanceId("-1")
            .reason("Available")
            .build();

    private LockStore(KeyValueService kvs) {
        this.keyValueService = kvs;
    }

    public static LockStore create(KeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        LockStore lockStore = new LockStore(kvs);
        if (lockStore.allLockEntries().isEmpty()) {
            new LockStorePopulator(kvs).populate();
        }
        return lockStore;
    }

    public LockEntry acquireLock(String reason) throws CheckAndSetException {
        LockEntry lockEntry = generateUniqueLockEntry(reason);
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(AtlasDbConstants.PERSISTED_LOCKS_TABLE,
                lockEntry.cell(),
                LOCK_OPEN.value(),
                lockEntry.value());

        keyValueService.checkAndSet(request);
        log.info("Successfully acquired the persistent lock: {}", lockEntry);
        return lockEntry;
    }

    public void releaseLock(LockEntry lockEntry) throws CheckAndSetException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(AtlasDbConstants.PERSISTED_LOCKS_TABLE,
                lockEntry.cell(),
                lockEntry.value(),
                LOCK_OPEN.value());

        keyValueService.checkAndSet(request);
        log.info("Successfully released the persistent lock.");
    }

    @VisibleForTesting
    Set<LockEntry> allLockEntries() {
        Set<RowResult<Value>> results = ImmutableSet.copyOf(keyValueService.getRange(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE,
                RangeRequest.all(),
                AtlasDbConstants.TRANSACTION_TS + 1));

        return results.stream().map(LockEntry::fromRowResult).collect(Collectors.toSet());
    }

    private LockEntry generateUniqueLockEntry(String reason) {
        String randomLockId = UUID.randomUUID().toString();
        return ImmutableLockEntry.builder()
                .lockName(DELETION_LOCK_NAME)
                .instanceId(randomLockId)
                .reason(reason)
                .build();
    }

    static class LockStorePopulator {
        private KeyValueService kvs;

        LockStorePopulator(KeyValueService kvs) {
            this.kvs = kvs;
        }

        void populate() {
            CheckAndSetRequest request = CheckAndSetRequest.newCell(
                    AtlasDbConstants.PERSISTED_LOCKS_TABLE,
                    LOCK_OPEN.cell(),
                    LOCK_OPEN.value());
            try {
                kvs.checkAndSet(request);
            } catch (CheckAndSetException e) {
                // This can happen if multiple LockStores are started at once. We don't actually mind.
                // All we care about is that we're in the state machine of "LOCK_OPEN"/"LOCK_TAKEN".
                // It still might be interesting, so we'll log it.
                List<String> values = getActualValues(e);
                log.debug("Encountered a CheckAndSetException when creating the LockStore. This means that two "
                        + "LockStore objects were created near-simultaneously, and is probably not a problem. "
                        + "For the record, we observed these values: {}", values);
            }
        }

        private List<String> getActualValues(CheckAndSetException ex) {
            if (ex.getActualValues() != null) {
                return ex.getActualValues().stream()
                        .map(v -> LockEntry.fromStoredValue(v).toString())
                        .collect(Collectors.toList());
            } else {
                return ImmutableList.of();
            }
        }
    }
}
