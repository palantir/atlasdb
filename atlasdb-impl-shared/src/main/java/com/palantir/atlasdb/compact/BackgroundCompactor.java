/*
 * Copyright 2018 Palantir Technologies
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.CompactMetadataTable;
import com.palantir.atlasdb.schema.generated.CompactTableFactory;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SingleLockService;

public final class BackgroundCompactor implements AutoCloseable {
    public static final long SLEEP_TIME_WHEN_NOTHING_TO_COMPACT_MIN_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private static final Logger log = LoggerFactory.getLogger(BackgroundCompactor.class);

    private final TransactionManager transactionManager;
    private final KeyValueService keyValueService;
    private final RemoteLockService lockService;
    private final Supplier<CompactorConfig> compactorConfigSupplier;
    private final CompactPriorityCalculator compactPriorityCalculator;

    private final CompactionOutcomeMetrics compactionOutcomeMetrics = new CompactionOutcomeMetrics();

    private Thread daemon;

    public static Optional<BackgroundCompactor> createAndRun(TransactionManager transactionManager,
            KeyValueService keyValueService,
            RemoteLockService lockService,
            Supplier<CompactorConfig> compactorConfigSupplier) {
        if (!keyValueService.shouldTriggerCompactions()) {
            log.info("Not starting a background compactor, because we don't believe our KVS needs one.");
            return Optional.empty();
        }

        CompactPriorityCalculator compactPriorityCalculator = CompactPriorityCalculator.create(transactionManager);
        BackgroundCompactor backgroundCompactor = new BackgroundCompactor(transactionManager,
                keyValueService,
                lockService,
                compactorConfigSupplier,
                compactPriorityCalculator);
        backgroundCompactor.runInBackground();

        log.info("Started background compactor {}", backgroundCompactor);

        return Optional.of(backgroundCompactor);
    }

    @VisibleForTesting
    BackgroundCompactor(TransactionManager transactionManager,
            KeyValueService keyValueService,
            RemoteLockService lockService,
            Supplier<CompactorConfig> compactorConfigSupplier,
            CompactPriorityCalculator compactPriorityCalculator) {
        this.transactionManager = transactionManager;
        this.keyValueService = keyValueService;
        this.lockService = lockService;
        this.compactorConfigSupplier = compactorConfigSupplier;
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
        CompactionOutcome outcome = CompactionOutcome.FAILED_TO_COMPACT; // default is failed, unless we know otherwise
        try {
            outcome = grabLockAndRunOnce(compactorLock);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Unexpected exception occurred whilst performing background compaction!", e);
        }
        compactionOutcomeMetrics.registerOccurrenceOf(outcome);
        Thread.sleep(getSleepTime(compactorConfigSupplier, outcome));
    }

    @VisibleForTesting
    CompactionOutcome grabLockAndRunOnce(SingleLockService compactorLock)
            throws InterruptedException {
        CompactorConfig config = compactorConfigSupplier.get();
        if (!config.enableCompaction()) {
            log.info("Skipping compaction because it is currently disabled.");
            return CompactionOutcome.DISABLED;
        }

        try {
            compactorLock.lockOrRefresh();
        } catch (Exception e) {
            log.warn("Encountered exception when attempting to acquire the compaction lock.", e);
            return CompactionOutcome.UNABLE_TO_ACQUIRE_LOCKS;
        }
        if (!compactorLock.haveLocks()) {
            log.info("Failed to get the compaction lock. Probably, another host is running compaction.");
            return CompactionOutcome.UNABLE_TO_ACQUIRE_LOCKS;
        }

        Optional<String> tableToCompactOptional;
        try {
            tableToCompactOptional = compactPriorityCalculator.selectTableToCompact();
        } catch (Exception e) {
            log.warn("Encountered exception when attempting to determine which table should be compacted.", e);
            return CompactionOutcome.NOTHING_TO_COMPACT;
        }
        if (!tableToCompactOptional.isPresent()) {
            log.info("No table to compact.");
            return CompactionOutcome.NOTHING_TO_COMPACT;
        }

        String tableToCompact = tableToCompactOptional.get();

        try {
            log.info("Compacting table {}", tableToCompact);
            compactTable(tableToCompact, config);
            log.info("Compacted table {}", tableToCompact);
        } catch (Exception e) {
            log.warn("Encountered exception when compacting table {}",
                    tableToCompact,
                    e);
            return CompactionOutcome.FAILED_TO_COMPACT;
        }

        try {
            registerCompactedTable(tableToCompact);
            return CompactionOutcome.SUCCESS;
        } catch (Exception e) {
            log.info("Successfully compacted table {}, but failed to register this."
                    + "Nothing bad will happen; we'll probably do a no-op compaction of this very shortly.",
                    tableToCompact,
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

    private void compactTable(String tableToCompact, CompactorConfig config) {
        // System tables MAY be involved in this process.
        keyValueService.compactInternally(TableReference.createUnsafe(tableToCompact),
                config.inMaintenanceMode());
    }

    @VisibleForTesting
    static long getSleepTime(
            Supplier<CompactorConfig> compactorConfigSupplier,
            CompactionOutcome outcome) {
        switch (outcome) {
            case SUCCESS:
            case COMPACTED_BUT_NOT_REGISTERED:
            case SHUTDOWN:
                return compactorConfigSupplier.get().compactPauseMillis();
            case NOTHING_TO_COMPACT:
            case DISABLED:
                return Math.max(compactorConfigSupplier.get().compactPauseMillis(),
                        SLEEP_TIME_WHEN_NOTHING_TO_COMPACT_MIN_MILLIS);
            case UNABLE_TO_ACQUIRE_LOCKS:
            case FAILED_TO_COMPACT:
                return compactorConfigSupplier.get().compactPauseOnFailureMillis();
            default:
                throw new IllegalStateException("Unexpected outcome enum type: " + outcome);
        }
    }

    enum CompactionOutcome {
        SUCCESS,
        NOTHING_TO_COMPACT,
        COMPACTED_BUT_NOT_REGISTERED,
        UNABLE_TO_ACQUIRE_LOCKS,
        FAILED_TO_COMPACT,
        SHUTDOWN,
        DISABLED
    }
}
