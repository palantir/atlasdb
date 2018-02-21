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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.CompactMetadataTable;
import com.palantir.atlasdb.schema.generated.CompactTableFactory;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;

public final class BackgroundCompactor implements AutoCloseable {
    private static final int SLEEP_TIME_WHEN_NO_TABLE_TO_COMPACT_MILLIS = 5000;
    private static final int SLEEP_TIME_WHEN_NO_LOCK_MILLIS = 60 * 1000;

    private static final Logger log = LoggerFactory.getLogger(BackgroundCompactor.class);

    private final TransactionManager transactionManager;
    private final KeyValueService keyValueService;
    private final LockService lockService;
    private final Supplier<Boolean> inSafeHours;

    private Thread daemon;

    public static Optional<BackgroundCompactor> createAndRun(TransactionManager transactionManager,
            KeyValueService keyValueService,
            LockService lockService,
            Supplier<Boolean> inSafeHours) {
        if (!keyValueService.shouldManuallyCompact()) {
            return Optional.empty();
        }

        BackgroundCompactor backgroundCompactor = new BackgroundCompactor(transactionManager,
                keyValueService,
                lockService,
                inSafeHours);
        backgroundCompactor.runInBackground();

        return Optional.of(backgroundCompactor);
    }

    private BackgroundCompactor(TransactionManager transactionManager,
            KeyValueService keyValueService,
            LockService lockService,
            Supplier<Boolean> inSafeHours) {
        this.transactionManager = transactionManager;
        this.keyValueService = keyValueService;
        this.lockService = lockService;
        this.inSafeHours = inSafeHours;
    }

    @Override
    public void close() {
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
    }

    public void run() {
        try (SingleLockService compactorLock = createSimpleLocks()) {
            log.info("Starting background compactor");
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("Shutting down background compactor because someone interrupted its thread");
                    Thread.currentThread().interrupt();
                    return;
                }

                compactorLock.lockOrRefresh();
                if (compactorLock.haveLocks()) {
                    Optional<String> tableToCompactOptional = transactionManager.runTaskReadOnly(
                            this::selectTableToCompact);
                    if (!tableToCompactOptional.isPresent()) {
                        log.info("No table to compact");
                        Thread.sleep(SLEEP_TIME_WHEN_NO_TABLE_TO_COMPACT_MILLIS);
                        continue;
                    }

                    String tableToCompact = tableToCompactOptional.get();

                    log.info("Compacting table {}", LoggingArgs.safeInternalTableName(tableToCompact));
                    compactTable(tableToCompact);
                    log.info("Compacted table {}", LoggingArgs.safeInternalTableName(tableToCompact));

                    registerCompactedTable(tableToCompact);
                } else {
                    log.info("Failed to get the compaction lock. Probably, another host is running compaction.");
                    Thread.sleep(SLEEP_TIME_WHEN_NO_LOCK_MILLIS);
                    continue;
                }
            }
        } catch (InterruptedException e) {
            log.warn("Shutting down background compactor due to InterruptedException", e);
            Thread.currentThread().interrupt();
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
        keyValueService.compactInternally(TableReference.createFromFullyQualifiedName(tableToCompact),
                inSafeHours.get());
    }

    private Optional<String> selectTableToCompact(Transaction tx) {
        Map<String, Long> tableToLastTimeSwept = new HashMap<>();
        SweepPriorityTable sweepPriorityTable = SweepTableFactory.of().getSweepPriorityTable(tx);
        sweepPriorityTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastSweepTime = row.getLastSweepTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeSwept.put(tableName, lastSweepTime);
                });

        Map<String, Long> tableToLastTimeCompacted = new HashMap<>();
        CompactMetadataTable compactMetadataTable = CompactTableFactory.of().getCompactMetadataTable(tx);
        compactMetadataTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastCompactTime = row.getLastCompactTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeCompacted.put(tableName, lastCompactTime);
                });

        List<String> uncompactedTables = tableToLastTimeSwept.keySet().stream()
                .filter(table -> !tableToLastTimeCompacted.keySet().contains(table))
                .collect(Collectors.toList());

        if (uncompactedTables.size() > 0) {
            int randomTableIndex = ThreadLocalRandom.current().nextInt(uncompactedTables.size());
            return Optional.of(uncompactedTables.get(randomTableIndex));
        }

        String tableToCompact = null;
        long maxSweptAfterCompact = Long.MIN_VALUE;
        for (Map.Entry<String, Long> entry : tableToLastTimeSwept.entrySet()) {
            String table = entry.getKey();
            long lastSweptTime = entry.getValue();
            long lastCompactTime = tableToLastTimeCompacted.get(table);
            long sweptAfterCompact = lastSweptTime - lastCompactTime;

            if (sweptAfterCompact > maxSweptAfterCompact) {
                tableToCompact = table;
                maxSweptAfterCompact = sweptAfterCompact;
            }
        }

        return Optional.ofNullable(tableToCompact);
    }
}
