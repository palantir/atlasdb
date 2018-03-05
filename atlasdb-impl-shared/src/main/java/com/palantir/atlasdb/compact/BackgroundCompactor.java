/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.compact;

import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.CompactMetadataTable;
import com.palantir.atlasdb.schema.generated.CompactTableFactory;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;

public final class BackgroundCompactor implements AutoCloseable {
    private static final int SLEEP_TIME_WHEN_NO_TABLE_TO_COMPACT_MILLIS = 5000;
    private static final int SLEEP_TIME_WHEN_NO_LOCK_MILLIS = 60 * 1000;
    private static final int SLEEP_TIME_AFTER_FAILURE_MILLIS = 60 * 1000;

    private static final Logger log = LoggerFactory.getLogger(BackgroundCompactor.class);

    private final TransactionManager transactionManager;
    private final KeyValueService keyValueService;
    private final LockService lockService;
    private final Supplier<Boolean> inSafeHours;
    private final CompactPriorityCalculator compactPriorityCalculator;

    private final CompactionOutcomeMetrics compactionOutcomeMetrics = new CompactionOutcomeMetrics();

    private Thread daemon;

    public static Optional<BackgroundCompactor> createAndRun(TransactionManager transactionManager,
            KeyValueService keyValueService,
            LockService lockService,
            Supplier<Boolean> inSafeHours) {
        if (!keyValueService.shouldTriggerCompactions()) {
            log.info("Not starting a background compactor, because we don't believe our KVS needs one.");
            return Optional.empty();
        }

        CompactPriorityCalculator compactPriorityCalculator = CompactPriorityCalculator.create(transactionManager);
        BackgroundCompactor backgroundCompactor = new BackgroundCompactor(transactionManager,
                keyValueService,
                lockService,
                inSafeHours,
                compactPriorityCalculator);
        backgroundCompactor.runInBackground();

        log.info("Started background compactor {}", backgroundCompactor);

        return Optional.of(backgroundCompactor);
    }

    @VisibleForTesting
    BackgroundCompactor(TransactionManager transactionManager,
            KeyValueService keyValueService,
            LockService lockService,
            Supplier<Boolean> inSafeHours,
            CompactPriorityCalculator compactPriorityCalculator) {
        this.transactionManager = transactionManager;
        this.keyValueService = keyValueService;
        this.lockService = lockService;
        this.inSafeHours = inSafeHours;
        this.compactPriorityCalculator = compactPriorityCalculator;
    }

    @Override
    public void close() {
        compactionOutcomeMetrics.registerOccurrenceOf(CompactionOutcome.SHUTDOWN);
        log.info("Closing BackgroundCompactor");
        daemon.interrupt();
        try {
            daemon.join();
            daemon = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private synchronized void runInBackground() {
        Preconditions.checkState(daemon == null);
        daemon = new Thread(this::run);
        daemon.setDaemon(true);
        daemon.setName("BackgroundCompactor");
        daemon.start();
        log.info("Set up the background compactor to be run.");
    }

    public void run() {
        log.info("Attempting to start the background compactor for the very first time.");
        try (SingleLockService compactorLock = createSimpleLocks()) {
            log.info("Starting background compactor");
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("Shutting down background compactor because someone interrupted its thread");
                    Thread.currentThread().interrupt();
                    return;
                }

                runOnceRecordingOutcome(compactorLock);
            }
        } catch (InterruptedException e) {
            log.warn("Shutting down background compactor due to InterruptedException. "
                    + "Please restart the service to resume compactions", e);
            compactionOutcomeMetrics.registerOccurrenceOf(CompactionOutcome.SHUTDOWN);
            Thread.currentThread().interrupt();
        }
    }

    private void runOnceRecordingOutcome(SingleLockService compactorLock) throws InterruptedException {
        CompactionOutcome outcome = grabLockAndRunOnce(compactorLock);
        compactionOutcomeMetrics.registerOccurrenceOf(outcome);
        Thread.sleep(outcome.getSleepTime());
    }

    @VisibleForTesting
    CompactionOutcome grabLockAndRunOnce(SingleLockService compactorLock)
            throws InterruptedException {
        compactorLock.lockOrRefresh();
        if (!compactorLock.haveLocks()) {
            log.info("Failed to get the compaction lock. Probably, another host is running compaction.");
            return CompactionOutcome.UNABLE_TO_ACQUIRE_LOCKS;
        }

        Optional<String> tableToCompactOptional = compactPriorityCalculator.selectTableToCompact();
        if (!tableToCompactOptional.isPresent()) {
            log.info("No table to compact");
            return CompactionOutcome.NOTHING_TO_COMPACT;
        }

        String tableToCompact = tableToCompactOptional.get();

        try {
            log.info("Compacting table {}", LoggingArgs.safeInternalTableName(tableToCompact));
            compactTable(tableToCompact);
            log.info("Compacted table {}", LoggingArgs.safeInternalTableName(tableToCompact));
        } catch (Exception e) {
            log.warn("Encountered exception when compacting table {}",
                    LoggingArgs.safeInternalTableName(tableToCompact),
                    e);
            return CompactionOutcome.FAILED_TO_COMPACT;
        }

        try {
            registerCompactedTable(tableToCompact);
            return CompactionOutcome.SUCCESS;
        } catch (Exception e) {
            log.info("Successfully compacted table {}, but failed to register this."
                    + "Nothing bad will happen; we'll probably do a no-op compaction of this very shortly.",
                    LoggingArgs.safeInternalTableName(tableToCompact),
                    e);
            return CompactionOutcome.COMPACTED_BUT_NOT_REGISTERED;
        }
    }

    private SingleLockService createSimpleLocks() {
        return new SingleLockService(lockService, "atlas compact");
    }

    private void registerCompactedTable(String tableToCompact) {
        transactionManager.runTaskWithRetry(tx -> {
            CompactMetadataTable compactMetadataTable = CompactTableFactory.of().getCompactMetadataTable(tx);
            compactMetadataTable.putLastCompactTime(
                    CompactMetadataTable.CompactMetadataRow.of(tableToCompact),
                    System.currentTimeMillis());

            return null;
        });
    }

    private void compactTable(String tableToCompact) {
        // System tables MAY be involved in this process.
        keyValueService.compactInternally(TableReference.createUnsafe(tableToCompact),
                inSafeHours.get());
    }

    enum CompactionOutcome {
        SUCCESS(0),
        NOTHING_TO_COMPACT(SLEEP_TIME_WHEN_NO_TABLE_TO_COMPACT_MILLIS),
        COMPACTED_BUT_NOT_REGISTERED(0),
        UNABLE_TO_ACQUIRE_LOCKS(SLEEP_TIME_WHEN_NO_LOCK_MILLIS),
        FAILED_TO_COMPACT(SLEEP_TIME_AFTER_FAILURE_MILLIS),
        SHUTDOWN(0);

        private final int sleepTime;

        CompactionOutcome(int sleepTime) {
            this.sleepTime = sleepTime;
        }

        public int getSleepTime() {
            return sleepTime;
        }
    }

}
