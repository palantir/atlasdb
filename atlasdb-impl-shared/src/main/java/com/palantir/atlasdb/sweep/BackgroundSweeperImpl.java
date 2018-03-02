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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
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
    private final SweepProgressStore sweepProgressStore;
    private final SweepTableFactory tableFactory;
    private final BackgroundSweeperPerformanceLogger sweepPerfLogger;
    private final SweepMetrics sweepMetrics;


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
            SweepProgressStore sweepProgressStore,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            SweepMetrics sweepMetrics) {
        this.txManager = txManager;
        this.kvs = kvs;
        this.sweepRunner = sweepRunner;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.sweepRowBatchSize = sweepBatchSize;
        this.sweepCellBatchSize = sweepCellBatchSize;
        this.sweepProgressStore = sweepProgressStore;
        this.tableFactory = tableFactory;
        this.sweepPerfLogger = sweepPerfLogger;
        this.sweepMetrics = sweepMetrics;
    }

    public static BackgroundSweeperImpl create(
            LockAwareTransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<Integer> sweepBatchSize,
            Supplier<Integer> sweepCellBatchSize,
            SweepProgressStore sweepProgressStore,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger) {

        SweepMetrics sweepMetrics = SweepMetrics.create();
        return new BackgroundSweeperImpl(txManager,
                kvs,
                sweepRunner,
                isSweepEnabled,
                sweepPauseMillis,
                sweepBatchSize,
                sweepCellBatchSize,
                sweepProgressStore,
                tableFactory,
                sweepPerfLogger,
                sweepMetrics);
    }

    @Override
    public synchronized void runInBackground() {
        Preconditions.checkState(daemon == null);
        daemon = new Thread(this);
        daemon.setDaemon(true);
        daemon.setName("BackgroundSweeper");
        daemon.start();
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
        java.util.Optional<TableToSweep> tableToSweep = getTableToSweep();
        if (!tableToSweep.isPresent()) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        } else {
            runOnceAndSaveResults(tableToSweep.get());
            return true;
        }
    }

    // there's a bug in older jdk8s around type inference here, don't make the same mistake two of us made
    // and try to lambda refactor this unless you live far enough in the future that this isn't an issue
    private java.util.Optional<TableToSweep> getTableToSweep() {
        return txManager.runTaskWithRetry(
                new TransactionTask<java.util.Optional<TableToSweep>, RuntimeException>() {
                    @Override
                    public java.util.Optional<TableToSweep> execute(Transaction tx) {
                        java.util.Optional<SweepProgress> progress = sweepProgressStore.loadProgress(tx);
                        if (progress.isPresent()) {
                            return java.util.Optional.of(new TableToSweep(progress.get().tableRef(), progress));
                        } else {
                            TableReference nextTable = chooseNextTableToSweep(new SweepTransaction(tx,
                                    sweepRunner.getSweepTimestamp(SweepStrategy.CONSERVATIVE)));
                            if (nextTable != null) {
                                return java.util.Optional.of(new TableToSweep(nextTable, java.util.Optional.empty()));
                            } else {
                                return java.util.Optional.empty();
                            }
                        }
                    }
                });
    }

    void runOnceAndSaveResults(TableToSweep tableToSweep) {
        int rowBatchSize = Math.max(1, (int) (sweepRowBatchSize.get() * batchSizeMultiplier));
        int cellBatchSize = sweepCellBatchSize.get();
        Stopwatch watch = Stopwatch.createStarted();
        sweepMetrics.registerMetricsIfNecessary(tableToSweep.getTableRef());
        try {
            SweepResults results = sweepRunner.run(
                    tableToSweep.getTableRef(),
                    rowBatchSize,
                    cellBatchSize,
                    tableToSweep.getStartRow());
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.debug("Swept {} unique cells from {} starting at {}"
                            + " and performed {} deletions in {} ms"
                            + " up to timestamp {}.",
                    results.getCellsExamined(), tableToSweep.getTableRef().getQualifiedName(),
                    tableToSweep.getStartRow() == null ? "0" : PtBytes.encodeHexString(tableToSweep.getStartRow()),
                    results.getCellsDeleted(), elapsedMillis, results.getSweptTimestamp());
            sweepPerfLogger.logSweepResults(
                    SweepPerformanceResults.builder()
                            .sweepResults(results)
                            .tableName(tableToSweep.getTableRef().getQualifiedName())
                            .elapsedMillis(elapsedMillis)
                            .build());
            saveSweepResults(tableToSweep, results);
        } catch (RuntimeException e) {
            // Error logged at a higher log level above.
            log.debug("Failed to sweep {} with row batch size {} and cell batch size {} starting from row {}",
                    tableToSweep.getTableRef().getQualifiedName(),
                    rowBatchSize,
                    cellBatchSize,
                    tableToSweep.getStartRow() == null ? "0" : PtBytes.encodeHexString(tableToSweep.getStartRow()));
            throw e;
        }
    }

    @Nullable
    private TableReference chooseNextTableToSweep(SweepTransaction tx) {
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
        TableReference tableRef = oldGetTableToSweep(tx, allTables, oldPriorities, newPrioritiesByTableName);
        if (tableRef == null) {
            return null;
        }

        log.debug("Now starting to sweep {}.", tableRef);
        return tableRef;
    }

    @Nullable
    private TableReference oldGetTableToSweep(SweepTransaction tx,
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

    private void saveSweepResults(final TableToSweep tableToSweep,
            final SweepResults currentIteration) {
        final long cellsDeleted = tableToSweep.getStaleValuesDeletedPreviously() + currentIteration.getCellsDeleted();
        final long cellsExamined = tableToSweep.getCellsExaminedPreviously() + currentIteration.getCellsExamined();
        final long minimumSweptTimestamp = Math.min(
                tableToSweep.getPreviousMinimumSweptTimestamp().orElse(Long.MAX_VALUE),
                currentIteration.getSweptTimestamp());
        final SweepResults results = SweepResults.builder()
                .cellsDeleted(cellsDeleted)
                .cellsExamined(cellsExamined)
                .sweptTimestamp(minimumSweptTimestamp)
                .nextStartRow(currentIteration.getNextStartRow())
                .build();
        if (currentIteration.getNextStartRow().isPresent()) {
            saveIntermediateSweepResults(tableToSweep, results);
            return;
        }

        saveFinalSweepResults(tableToSweep, results);

        log.debug("Finished sweeping {}, examined {} unique cells, deleted {} cells.",
                tableToSweep.getTableRef().getQualifiedName(), cellsExamined, cellsDeleted);

        sweepProgressStore.clearProgress();
    }

    private void saveIntermediateSweepResults(TableToSweep tableToSweep, SweepResults results) {
        Preconditions.checkArgument(results.getNextStartRow().isPresent(),
                "Next start row should be present when saving intermediate results!");
        txManager.runTaskWithRetry((TxTask) tx -> {
            if (!tableToSweep.hasPreviousProgress()) {
                // This is the first set of results being written for this table.
                SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(tx);
                SweepPriorityRow priorityRow = SweepPriorityRow.of(
                        tableToSweep.getTableRef().getQualifiedName());
                priorityTable.putWriteCount(priorityRow, 0L);
            }
            SweepProgress newProgress = ImmutableSweepProgress.builder()
                    .tableRef(tableToSweep.getTableRef())
                    .staleValuesDeleted(results.getCellsDeleted())
                    .cellTsPairsExamined(results.getCellsExamined())
                    //noinspection OptionalGetWithoutIsPresent // covered by precondition above
                    .startRow(results.getNextStartRow().get())
                    .minimumSweptTimestamp(results.getSweptTimestamp())
                    .build();
            sweepProgressStore.saveProgress(tx, newProgress);
            return null;
        });
    }

    private void saveFinalSweepResults(TableToSweep tableToSweep, SweepResults sweepResults) {
        txManager.runTaskWithRetry((TxTask) tx -> {
            SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(tx);
            SweepPriorityRow row = SweepPriorityRow.of(tableToSweep.getTableRef().getQualifiedName());
            priorityTable.putCellsDeleted(row, sweepResults.getCellsDeleted());
            priorityTable.putCellsExamined(row, sweepResults.getCellsExamined());
            priorityTable.putLastSweepTime(row, System.currentTimeMillis());
            if (!tableToSweep.hasPreviousProgress()) {
                // This is the first (and only) set of results being written for this table.
                priorityTable.putWriteCount(row, 0L);
                priorityTable.putMinimumSweptTimestamp(row, sweepResults.getSweptTimestamp());
            } else {
                priorityTable.putMinimumSweptTimestamp(row, tableToSweep.getPreviousMinimumSweptTimestamp().orElse(0L));
            }
            return null;
        });

        sweepMetrics.recordMetrics(tableToSweep.getTableRef(), sweepResults);
    }

    /**
     * Check whether the table being swept was dropped. If so, stop sweeping it and move on.
     * @return Whether the table being swept was dropped
     */
    private boolean checkAndRepairTableDrop() {
        try {
            Set<TableReference> tables = kvs.getAllTableNames();
            java.util.Optional<SweepProgress> progress = txManager.runTaskReadOnly(sweepProgressStore::loadProgress);
            if (!progress.isPresent() || tables.contains(progress.get().tableRef())) {
                return false;
            }
            sweepProgressStore.clearProgress();
            return true;
        } catch (RuntimeException e) {
            log.error("Failed to check whether the table being swept was dropped."
                    + " Continuing under the assumption that it wasn't...", e);
            return false;
        }
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
            Thread.currentThread().interrupt();
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
