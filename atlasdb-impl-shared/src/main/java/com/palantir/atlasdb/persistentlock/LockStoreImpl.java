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
package com.palantir.atlasdb.persistentlock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * LockStore manages {@link LockEntry} objects, specifically for the "Backup Lock" (to be taken out by backup and
 * sweep).
 *
 * The PERSISTED_LOCKS_TABLE contains exactly one element, either LOCK_OPEN or (lock taken for some REASON), giving
 * rise to a state machine where we hop between the central "OPEN" state and some "taken because REASON" state:
 *
 * "taken for SWEEP (uuid1)" <- - - - -> OPEN < - - - -> "taken for BACKUP (uuid2)"
 *
 * acquireBackupLock(REASON) attempts to move the lock state from "OPEN" to "taken because REASON",
 * returning a LockEntry that holds a unique identifier for that occasion of taking the lock.
 *
 * releaseBackupLock(LockEntry) attempts to move the state from "taken because REASON" to "OPEN", but only succeeds if
 * the LockEntry currently stored matches the one passed in, ensuring that nobody released the lock when we weren't
 * looking.
 *
 * Upon creating the PERSISTED_LOCKS_TABLE, we attempt to enter this state machine by populating the table.
 * If we fail to do this, it's because someone else also created the table, and populated it before we did.
 * This is actually OK - all we care about is that we're in the state machine _somewhere_.
 */
@SuppressWarnings("checkstyle:FinalClass") // Non-final as we'd like to mock it.
public class LockStoreImpl implements LockStore {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_LockStore {
        @Override
        public LockStore delegate() {
            checkInitialized();
            return LockStoreImpl.this;
        }

        @Override
        protected void tryInitialize() {
            LockStoreImpl.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return "LockStore";
        }
    }

    private static final SafeLogger log = SafeLoggerFactory.get(LockStoreImpl.class);
    private static final String BACKUP_LOCK_NAME = "BackupLock";
    private final InitializingWrapper wrapper = new InitializingWrapper();

    private final KeyValueService keyValueService;

    public static final LockEntry LOCK_OPEN = ImmutableLockEntry.builder()
            .lockName(BACKUP_LOCK_NAME)
            .instanceId(UUID.fromString("0-0-0-0-0"))
            .reason("Available")
            .build();

    LockStoreImpl(KeyValueService kvs) {
        this.keyValueService = kvs;
    }

    public static LockStore create(KeyValueService kvs) {
        return create(kvs, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static LockStore create(KeyValueService kvs, boolean initializeAsync) {
        LockStoreImpl lockStore = createImpl(kvs, initializeAsync);
        return lockStore.wrapper.isInitialized() ? lockStore : lockStore.wrapper;
    }

    @VisibleForTesting
    static LockStoreImpl createImplForTest(KeyValueService kvs) {
        return createImpl(kvs, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    private static LockStoreImpl createImpl(KeyValueService kvs, boolean initializeAsync) {
        LockStoreImpl lockStore = new LockStoreImpl(kvs);
        lockStore.wrapper.initialize(initializeAsync);
        return lockStore;
    }

    private void tryInitialize() {
        keyValueService.createTable(AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        if (allLockEntries().isEmpty()) {
            new LockStorePopulator(keyValueService).populate();
        }
    }

    @Override
    public LockEntry acquireBackupLock(String reason) throws CheckAndSetException {
        LockEntry lockEntry = generateUniqueBackupLockEntry(reason);
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.cell(), LOCK_OPEN.value(), lockEntry.value());

        keyValueService.checkAndSet(request);
        log.info("Successfully acquired the persistent lock: {}", SafeArg.of("lockEntry", lockEntry));
        return lockEntry;
    }

    @Override
    public void releaseLock(LockEntry lockEntry) throws CheckAndSetException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.cell(), lockEntry.value(), LOCK_OPEN.value());

        keyValueService.checkAndSet(request);
        log.info("Successfully released the persistent lock: {}", SafeArg.of("lockEntry", lockEntry));
    }

    @Override
    public LockEntry getLockEntryWithLockId(PersistentLockId lockId) {
        List<LockEntry> locksWithId = allLockEntries().stream()
                .filter(lockEntry -> Objects.equals(lockEntry.instanceId(), lockId.value()))
                .collect(Collectors.toList());

        Preconditions.checkArgument(
                locksWithId.size() == 1,
                "Expected a single lock with id %s but found %s matching locks [%s]",
                lockId,
                locksWithId.size(),
                locksWithId);

        return Iterables.getOnlyElement(locksWithId);
    }

    @VisibleForTesting
    Set<LockEntry> allLockEntries() {
        Set<RowResult<Value>> results = ImmutableSet.copyOf(keyValueService.getRange(
                AtlasDbConstants.PERSISTED_LOCKS_TABLE, RangeRequest.all(), AtlasDbConstants.TRANSACTION_TS + 1));

        return results.stream().map(LockEntry::fromRowResult).collect(Collectors.toSet());
    }

    private LockEntry generateUniqueBackupLockEntry(String reason) {
        UUID randomLockId = UUID.randomUUID();
        return ImmutableLockEntry.builder()
                .lockName(BACKUP_LOCK_NAME)
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
                    AtlasDbConstants.PERSISTED_LOCKS_TABLE, LOCK_OPEN.cell(), LOCK_OPEN.value());
            try {
                kvs.checkAndSet(request);
            } catch (CheckAndSetException e) {
                // This can happen if multiple LockStores are started at once. We don't actually mind.
                // All we care about is that we're in the state machine of "LOCK_OPEN"/"LOCK_TAKEN".
                // It still might be interesting, so we'll log it.
                List<String> values = getActualValues(e);
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Encountered a CheckAndSetException when creating the LockStore. This means that two"
                                + " LockStore objects were created near-simultaneously, and is probably not a problem."
                                + " For the record, we observed these values: {}",
                            UnsafeArg.of("values", values),
                            e);
                }
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
