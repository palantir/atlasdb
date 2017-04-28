/*
 * Copyright 2015 Palantir Technologies
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRow;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.atlasdb.transaction.impl.UnmodifiableTransaction;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;

public final class BackgroundSweeperImpl implements BackgroundSweeper {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);
    private final LockAwareTransactionManager txManager;
    private final KeyValueService kvs;
    private final SweepTaskRunner sweepRunner;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final Supplier<Integer> sweepRowBatchSize;
    private final Supplier<Integer> sweepCellBatchSize;
    private final SweepTableFactory tableFactory;
    private final BackgroundSweeperPerformanceLogger sweepPerfLogger;
    private final SweepMetrics sweepMetrics;
    private final PersistentLockManager persistentLockManager;

    private volatile float batchSizeMultiplier = 1.0f;
    private Thread daemon;

    // weights one month of no sweeping with the same priority as about 100000 expected cells to sweep.
    private static final double MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT =
            100_000.0 / TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS);

    @VisibleForTesting
    BackgroundSweeperImpl(
            LockAwareTransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<Integer> sweepBatchSize,
            Supplier<Integer> sweepCellBatchSize,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            SweepMetrics sweepMetrics,
            PersistentLockManager persistentLockManager) {
        this.txManager = txManager;
        this.kvs = kvs;
        this.sweepRunner = sweepRunner;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.sweepRowBatchSize = sweepBatchSize;
        this.sweepCellBatchSize = sweepCellBatchSize;
        this.tableFactory = tableFactory;
        this.sweepPerfLogger = sweepPerfLogger;
        this.sweepMetrics = sweepMetrics;
        this.persistentLockManager = persistentLockManager;
    }

    public static BackgroundSweeperImpl create(
            LockAwareTransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<Integer> sweepBatchSize,
            Supplier<Integer> sweepCellBatchSize,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            PersistentLockManager persistentLockManager) {

        SweepMetrics sweepMetrics = SweepMetrics.create();
        return new BackgroundSweeperImpl(txManager,
                kvs,
                sweepRunner,
                isSweepEnabled,
                sweepPauseMillis,
                sweepBatchSize,
                sweepCellBatchSize,
                tableFactory,
                sweepPerfLogger,
                sweepMetrics,
                persistentLockManager);
    }

    @Override
    public synchronized void runInBackground() {
        Preconditions.checkState(daemon == null);
        daemon = new Thread(this);
        daemon.setDaemon(true);
        daemon.setName("BackgroundSweeper");
        daemon.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down persistent lock manager");
            try {
                persistentLockManager.shutdown();
                log.info("Shutdown complete!");
            } catch (Exception e) {
                log.warn("An exception occurred while shutting down. This means that we had the backup lock out when"
                        + "the shutdown was triggered, but failed to release it. If this is the case, sweep or backup"
                        + "may fail to take out the lock in future. If this happens consistently, "
                        + "consult the following documentation on how to release the dead lock: "
                        + "https://palantir.github.io/atlasdb/html/troubleshooting/index.html#clearing-the-backup-lock",
                        e);
            }
        }));
    }

    @Override
    public void run() {
        Optional<LockRefreshToken> locks = Optional.absent();
        try {
            // Wait a while before starting so short lived clis don't try to sweep.
            Thread.sleep(20 * (1000 + sweepPauseMillis.get()));
            log.debug("Starting background sweeper.");
            while (true) {
                boolean sweptSuccessfully = false;
                try {
                    if (isSweepEnabled.get()) {
                        locks = lockOrRefresh(locks);
                        if (locks.isPresent()) {
                            sweptSuccessfully = runOnce();
                        } else {
                            log.debug("Skipping sweep because sweep is running elsewhere.");
                        }
                    } else {
                        log.debug("Skipping sweep because it is currently disabled.");
                    }
                } catch (InsufficientConsistencyException e) {
                    log.warn("Could not sweep because not all nodes of the database are online.", e);
                } catch (RuntimeException e) {
                    if (checkAndRepairTableDrop()) {
                        log.error("The table being swept by the background sweeper was dropped, moving on...");
                    } else {
                        int sweepBatchSize = (int) (batchSizeMultiplier * sweepRowBatchSize.get());
                        log.error("The background sweep job failed unexpectedly with a batch size of {}"
                                + ". Attempting to continue with a lower batch size...", sweepBatchSize, e);
                        // Cut batch size in half, always sweep at least one row (we round down).
                        batchSizeMultiplier = Math.max(batchSizeMultiplier / 2, 1.5f / sweepRowBatchSize.get());
                    }
                }
                if (sweptSuccessfully) {
                    batchSizeMultiplier = Math.min(1.0f, batchSizeMultiplier * 1.01f);
                    Thread.sleep(sweepPauseMillis.get());
                } else {
                    Thread.sleep(20 * (1000 + sweepPauseMillis.get()));
                }
            }
        } catch (InterruptedException e) {
            log.debug("Shutting down background sweeper.");
        } finally {
            if (locks.isPresent()) {
                txManager.getLockService().unlock(locks.get());
            }
        }
    }

    @VisibleForTesting
    boolean runOnce() {
        SweepProgressRowResult progress = txManager.runTaskWithRetry(tx -> {
            SweepProgressTable progressTable = tableFactory.getSweepProgressTable(tx);
            SweepProgressRowResult result = progressTable.getRow(SweepProgressRow.of(0)).orNull();
            if (result == null) {
                result = chooseNextTableToSweep(new SweepTransaction(
                        tx,
                        sweepRunner.getConservativeSweepTimestamp()));
            }
            return result;
        });
        if (progress == null) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        }
        int rowBatchSize = Math.max(1, (int) (sweepRowBatchSize.get() * batchSizeMultiplier));
        int cellBatchSize = sweepCellBatchSize.get();
        Stopwatch watch = Stopwatch.createStarted();
        String tableName = progress.getFullTableName();
        sweepMetrics.registerMetricsIfNecessary(TableReference.createUnsafe(tableName));
        try {
            SweepResults results = sweepRunner.run(TableReference.createUnsafe(
                    tableName),
                    rowBatchSize,
                    cellBatchSize,
                    progress.getStartRow());
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.debug("Swept {} unique cells from {} starting at {}"
                            + " and performed {} deletions in {} ms"
                            + " up to timestamp {}.",
                    results.getCellsExamined(), tableName,
                    progress.getStartRow() == null ? "0" : PtBytes.encodeHexString(progress.getStartRow()),
                    results.getCellsDeleted(), elapsedMillis, results.getSweptTimestamp());
            sweepPerfLogger.logSweepResults(
                    SweepPerformanceResults.builder()
                            .sweepResults(results)
                            .tableName(tableName)
                            .elapsedMillis(elapsedMillis)
                            .build());
            saveSweepResults(progress, results);
            return true;
        } catch (RuntimeException e) {
            // Error logged at a higher log level above.
            log.debug("Failed to sweep {} with row batch size {} and cell batch size {} starting from row {}",
                    tableName,
                    rowBatchSize,
                    cellBatchSize,
                    progress.getStartRow() == null ? "0" : PtBytes.encodeHexString(progress.getStartRow()));
            throw e;
        }
    }

    @Nullable
    private SweepProgressRowResult chooseNextTableToSweep(SweepTransaction tx) {
        Set<TableReference> allTables = Sets.difference(kvs.getAllTableNames(), AtlasDbConstants.hiddenTables);
        SweepPriorityTable oldPriorityTable = tableFactory.getSweepPriorityTable(tx);
        SweepPriorityTable newPriorityTable = tableFactory.getSweepPriorityTable(tx.delegate());

        // We read priorities from the past because we should prioritize based on what the sweeper will
        // actually be able to sweep. We read priorities from the present to make sure we don't repeatedly
        // sweep the same table while waiting for the past to catch up.
        List<SweepPriorityRowResult> oldPriorities = oldPriorityTable.getAllRowsUnordered().immutableCopy();
        List<SweepPriorityRowResult> newPriorities = newPriorityTable.getAllRowsUnordered().immutableCopy();
        Map<TableReference, SweepPriorityRowResult> newPrioritiesByTableName =
                newPriorities.stream().collect(
                        Collectors.toMap(
                                result -> TableReference.createUnsafe(result.getRowName().getFullTableName()),
                                Function.identity()
                        )
                );
        TableReference tableRef = getTableToSweep(tx, allTables, oldPriorities, newPrioritiesByTableName);
        if (tableRef == null) {
            return null;
        }
        RowResult<byte[]> rawResult = RowResult.create(SweepProgressRow.of(0).persistToBytes(),
                ImmutableSortedMap.<byte[], byte[]>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(SweepProgressTable.SweepProgressNamedColumn.FULL_TABLE_NAME.getShortName(),
                                SweepProgressTable.FullTableName.of(tableRef.getQualifiedName()).persistValue())
                        .build());

        log.debug("Now starting to sweep {}.", tableRef);
        return SweepProgressRowResult.of(rawResult);
    }

    @Nullable
    private TableReference getTableToSweep(SweepTransaction tx,
            Set<TableReference> allTables,
            List<SweepPriorityRowResult> oldPriorities,
            Map<TableReference, SweepPriorityRowResult> newPrioritiesByTableName) {
        // Arbitrarily pick the first table alphabetically from the never-before-swept tables
        List<TableReference> unsweptTables = Sets.difference(allTables, newPrioritiesByTableName.keySet())
                .stream().sorted(Comparator.comparing(TableReference::getTablename)).collect(Collectors.toList());
        if (!unsweptTables.isEmpty()) {
            return Iterables.get(unsweptTables, 0);
        }
        double maxPriority = 0.0;
        TableReference toSweep = null;
        Collection<SweepPriorityRow> toDelete = Lists.newArrayList();
        for (SweepPriorityRowResult oldPriority : oldPriorities) {
            TableReference tableRef = TableReference.createUnsafe(oldPriority.getRowName().getFullTableName());
            if (allTables.contains(tableRef)) {
                SweepPriorityRowResult newPriority = newPrioritiesByTableName.get(tableRef);
                double priority = getSweepPriority(oldPriority, newPriority);
                if (priority > maxPriority) {
                    maxPriority = priority;
                    toSweep = tableRef;
                }
            } else {
                toDelete.add(oldPriority.getRowName());
            }
        }

        // Clean up rows for tables that no longer exist.
        tableFactory.getSweepPriorityTable(tx.delegate()).delete(toDelete);

        return toSweep;
    }

    private double getSweepPriority(SweepPriorityRowResult oldPriority, SweepPriorityRowResult newPriority) {
        Stream<String> hiddenTableFullNames = AtlasDbConstants.hiddenTables.stream()
                .map(tableRef -> tableRef.getQualifiedName());
        if (hiddenTableFullNames.anyMatch(Predicate.isEqual(newPriority.getRowName().getFullTableName()))) {
            // Never sweep hidden tables
            return 0.0;
        }
        if (!newPriority.hasLastSweepTime()) {
            // Highest priority if we've never swept it before
            return Double.MAX_VALUE;
        }
        if (oldPriority.getWriteCount() > newPriority.getWriteCount()) {
            // We just swept this, or it got truncated.
            return 0.0;
        }
        long cellsDeleted = Math.max(1, oldPriority.getCellsDeleted());
        long cellsExamined = Math.max(1, oldPriority.getCellsExamined());
        long writeCount = Math.max(1, oldPriority.getWriteCount());
        double previousEfficacy = 1.0 * cellsDeleted / cellsExamined;
        double estimatedCellsToSweep = previousEfficacy * writeCount;
        long millisSinceSweep = System.currentTimeMillis() - oldPriority.getLastSweepTime();

        if (writeCount <= 100 + cellsExamined / 100
                && TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS) < 180) {
            // Not worth the effort if fewer than 1% of cells are new and we've swept in the last 6 months.
            return 0.0;
        }

        // This ordering function weights one month of no sweeping
        // with the same priority as about 100000 expected cells to sweep.
        return estimatedCellsToSweep + millisSinceSweep * MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT;
    }

    private void saveSweepResults(final SweepProgressRowResult progress,
            final SweepResults currentIteration) {
        final long cellsDeleted = fromNullable(progress.getCellsDeleted()) + currentIteration.getCellsDeleted();
        final long cellsExamined = fromNullable(progress.getCellsExamined()) + currentIteration.getCellsExamined();
        final long minimumSweptTimestamp = currentIteration.getSweptTimestamp();
        final SweepResults results = SweepResults.builder()
                .cellsDeleted(cellsDeleted)
                .cellsExamined(cellsExamined)
                .sweptTimestamp(minimumSweptTimestamp)
                .nextStartRow(currentIteration.getNextStartRow())
                .build();
        if (currentIteration.getNextStartRow().isPresent()) {
            saveIntermediateSweepResults(progress, results);
            return;
        }

        saveFinalSweepResults(progress, results);

        log.debug("Finished sweeping {}, examined {} unique cells, deleted {} cells.",
                progress.getFullTableName(), cellsExamined, cellsDeleted);

        if (cellsDeleted > 0) {
            Stopwatch watch = Stopwatch.createStarted();
            kvs.compactInternally(TableReference.createUnsafe(progress.getFullTableName()));
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.debug("Finished performing compactInternally on {} in {} ms.",
                    progress.getFullTableName(), elapsedMillis);
            sweepPerfLogger.logInternalCompaction(
                    SweepCompactionPerformanceResults.builder()
                            .tableName(progress.getFullTableName())
                            .cellsDeleted(cellsDeleted)
                            .cellsExamined(cellsExamined)
                            .elapsedMillis(elapsedMillis)
                            .build());
        }

        clearSweepProgressTable();
    }

    private void saveIntermediateSweepResults(final SweepProgressRowResult progress,
            final SweepResults results) {
        Preconditions.checkArgument(results.getNextStartRow().isPresent(),
                "Next start row should be present when saving intermediate results!");
        txManager.runTaskWithRetry((TxTask) tx -> {
            SweepProgressTable progressTable = tableFactory.getSweepProgressTable(tx);
            SweepProgressRow row = SweepProgressRow.of(0);
            progressTable.putFullTableName(row, progress.getFullTableName());
            //noinspection OptionalGetWithoutIsPresent // covered by precondition above
            progressTable.putStartRow(row, results.getNextStartRow().get());
            progressTable.putCellsDeleted(row, results.getCellsDeleted());
            progressTable.putCellsExamined(row, results.getCellsExamined());
            if (!progress.hasStartRow()) {
                // This is the first set of results being written for this table.
                progressTable.putMinimumSweptTimestamp(row, results.getSweptTimestamp());

                SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(tx);
                SweepPriorityRow priorityRow = SweepPriorityRow.of(progress.getFullTableName());
                priorityTable.putWriteCount(priorityRow, 0L);
            }
            return null;
        });
    }

    private void saveFinalSweepResults(SweepProgressRowResult progress, SweepResults sweepResults) {
        txManager.runTaskWithRetry((TxTask) tx -> {
            SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(tx);
            SweepPriorityRow row = SweepPriorityRow.of(progress.getFullTableName());
            priorityTable.putCellsDeleted(row, sweepResults.getCellsDeleted());
            priorityTable.putCellsExamined(row, sweepResults.getCellsExamined());
            priorityTable.putLastSweepTime(row, System.currentTimeMillis());
            if (!progress.hasStartRow()) {
                // This is the first (and only) set of results being written for this table.
                priorityTable.putWriteCount(row, 0L);
                priorityTable.putMinimumSweptTimestamp(row, sweepResults.getSweptTimestamp());
            } else {
                priorityTable.putMinimumSweptTimestamp(row, fromNullable(progress.getMinimumSweptTimestamp()));
            }
            return null;
        });

        sweepMetrics.recordMetrics(TableReference.createUnsafe(progress.getFullTableName()), sweepResults);
    }

    /**
     * Check whether the table being swept was dropped. If so, stop sweeping it and move on.
     * @return Whether the table being swept was dropped
     */
    private boolean checkAndRepairTableDrop() {
        try {
            Set<String> tables = kvs.getAllTableNames().stream()
                    .map(tableRef -> tableRef.getQualifiedName()).collect(Collectors.toSet());
            SweepProgressRowResult result = txManager.runTaskReadOnly(t ->
                    tableFactory.getSweepProgressTable(t).getRow(SweepProgressRow.of(0L)).orNull());
            if (result == null || tables.contains(result.getFullTableName())) {
                return false;
            }
            clearSweepProgressTable();
            return true;
        } catch (RuntimeException e) {
            log.error("Failed to check whether the table being swept was dropped."
                    + " Continuing under the assumption that it wasn't...", e);
            return false;
        }
    }

    /**
     * Fully remove the contents of the sweep progress table.
     */
    private void clearSweepProgressTable() {
        // Use deleteRange instead of truncate
        // 1) The table should be small, performance difference should be negligible.
        // 2) Truncate takes an exclusive lock in Postgres, which can interfere
        // with concurrently running backups.
        kvs.deleteRange(tableFactory.getSweepProgressTable(null).getTableRef(), RangeRequest.all());
    }

    private long fromNullable(Long num) {
        return num == null ? 0L : num.longValue();
    }

    private Optional<LockRefreshToken> lockOrRefresh(Optional<LockRefreshToken> previousLocks)
            throws InterruptedException {
        if (previousLocks.isPresent()) {
            LockRefreshToken refreshToken = previousLocks.get();
            Set<LockRefreshToken> refreshedTokens = txManager.getLockService()
                    .refreshLockRefreshTokens(ImmutableList.of(refreshToken));
            if (refreshedTokens.isEmpty()) {
                return Optional.absent();
            } else {
                return previousLocks;
            }
        } else {
            LockDescriptor lock = StringLockDescriptor.of("atlas sweep");
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE)).doNotBlock().build();
            LockRefreshToken token = txManager.getLockService().lock(LockClient.ANONYMOUS.getClientId(), request);
            return Optional.fromNullable(token);
        }
    }

    @Override
    public synchronized void shutdown() {
        if (daemon == null) {
            return;
        }
        log.debug("Signalling background sweeper to shut down.");
        daemon.interrupt();
        try {
            daemon.join();
            daemon = null;
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private static class SweepTransaction extends UnmodifiableTransaction {
        private final long sweepTimestamp;

        SweepTransaction(Transaction delegate, long sweepTimestamp) {
            super(delegate);
            this.sweepTimestamp = sweepTimestamp;
        }

        @Override
        public long getTimestamp() {
            return sweepTimestamp;
        }

        @Override
        public TransactionReadSentinelBehavior getReadSentinelBehavior() {
            return TransactionReadSentinelBehavior.IGNORE;
        }
    }
}
