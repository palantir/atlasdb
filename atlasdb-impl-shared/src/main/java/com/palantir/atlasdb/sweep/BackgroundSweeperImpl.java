/**
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRow;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
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

public class BackgroundSweeperImpl implements BackgroundSweeper {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);
    private final LockAwareTransactionManager txManager;
    private final KeyValueService kvs;
    private final SweepTaskRunner sweepRunner;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final Supplier<Integer> sweepBatchSize;
    private final SweepTableFactory tableFactory;
    private volatile float batchSizeMultiplier = 1.0f;
    private Thread daemon;

    // weights one month of no sweeping with the same priority as about 100000 expected cells to sweep.
    private static final double MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT = 100000.0 / TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS);


    public BackgroundSweeperImpl(LockAwareTransactionManager txManager,
                                 KeyValueService kvs,
                                 SweepTaskRunner sweepRunner,
                                 Supplier<Boolean> isSweepEnabled,
                                 Supplier<Long> sweepPauseMillis,
                                 Supplier<Integer> sweepBatchSize,
                                 SweepTableFactory tableFactory) {
        this.txManager = txManager;
        this.kvs = kvs;
        this.sweepRunner = sweepRunner;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.sweepBatchSize = sweepBatchSize;
        this.tableFactory = tableFactory;
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
                        log.error("The background sweep job failed unexpectedly with a batch size of " +
                                ((int) (batchSizeMultiplier * sweepBatchSize.get())) +
                                ". Attempting to continue with a lower batch size...", e);
                        batchSizeMultiplier = Math.min(batchSizeMultiplier / 2, 1.0f / sweepBatchSize.get());
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

    private boolean runOnce() {
        SweepProgressRowResult progress = txManager.runTaskWithRetry(
                new RuntimeTransactionTask<SweepProgressRowResult>() {
            @Override
            public SweepProgressRowResult execute(Transaction t) {
                SweepProgressTable progressTable = tableFactory.getSweepProgressTable(t);
                SweepProgressRowResult result = progressTable.getRow(SweepProgressRow.of(0)).orNull();
                if (result == null) {
                    result = chooseNextTableToSweep(new SweepTransaction(t, sweepRunner.getSweepTimestamp(SweepStrategy.CONSERVATIVE)));
                }
                return result;
            }
        });
        if (progress == null) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        }
        int batchSize = Math.max(1, (int) (sweepBatchSize.get() * batchSizeMultiplier));
        Stopwatch watch = Stopwatch.createStarted();
        try {
            SweepResults results = sweepRunner.run(progress.getFullTableName(), batchSize, progress.getStartRow());
            log.debug("Swept {} unique cells from {} starting at {} and performed {} deletions in {} ms.",
                    results.getCellsExamined(), progress.getFullTableName(),
                    progress.getStartRow() == null ? "0" : PtBytes.encodeHexString(progress.getStartRow()),
                    results.getCellsDeleted(), watch.elapsed(TimeUnit.MILLISECONDS));
            saveSweepResults(progress, results);
            return true;
        } catch (RuntimeException e) {
            // Error logged at a higher log level above.
            log.debug("Failed to sweep {} with batch size {} starting from row {}", progress.getFullTableName(), batchSize,
                    progress.getStartRow() == null ? "0" : PtBytes.encodeHexString(progress.getStartRow()));
            throw e;
        }
    }

    @Nullable
    private SweepProgressRowResult chooseNextTableToSweep(SweepTransaction t) {
        Set<String> allTables = Sets.difference(kvs.getAllTableNames(), AtlasDbConstants.hiddenTables);
        SweepPriorityTable oldPriorityTable = tableFactory.getSweepPriorityTable(t);
        SweepPriorityTable newPriorityTable = tableFactory.getSweepPriorityTable(t.delegate());

        // We read priorities from the past because we should prioritize based on what the sweeper will
        // actually be able to sweep. We read priorities from the present to make sure we don't repeatedly
        // sweep the same table while waiting for the past to catch up.
        List<SweepPriorityRowResult> oldPriorities = oldPriorityTable.getAllRowsUnordered().immutableCopy();
        List<SweepPriorityRowResult> newPriorities = newPriorityTable.getAllRowsUnordered().immutableCopy();
        Map<String,SweepPriorityRowResult> newPrioritiesByTableName = Maps.uniqueIndex(newPriorities,
                Functions.compose(SweepPriorityRow.getFullTableNameFun(), SweepPriorityRowResult.getRowNameFun()));
        String tableName = getTableToSweep(t, allTables, oldPriorities, newPrioritiesByTableName);
        if (tableName == null) {
            return null;
        }
        RowResult<byte[]> rawResult = RowResult.<byte[]>create(SweepProgressRow.of(0).persistToBytes(),
                ImmutableSortedMap.<byte[], byte[]>orderedBy(UnsignedBytes.lexicographicalComparator())
                    .put(SweepProgressTable.SweepProgressNamedColumn.FULL_TABLE_NAME.getShortName(),
                         SweepProgressTable.FullTableName.of(tableName).persistValue())
                    .build());

        log.debug("Now starting to sweep {}.", tableName);
        return SweepProgressRowResult.of(rawResult);
    }

    @Nullable
    private String getTableToSweep(SweepTransaction t,
                                   Set<String> allTables,
                                   List<SweepPriorityRowResult> oldPriorities,
                                   Map<String, SweepPriorityRowResult> newPrioritiesByTableName) {
        Set<String> unsweptTables = Sets.difference(allTables, newPrioritiesByTableName.keySet());
        if (!unsweptTables.isEmpty()) {
            return Iterables.get(unsweptTables, 0);
        }
        double maxPriority = 0.0;
        String toSweep = null;
        Collection<SweepPriorityRow> toDelete = Lists.newArrayList();
        for (SweepPriorityRowResult oldPriority : oldPriorities) {
            String tableName = oldPriority.getRowName().getFullTableName();
            if (allTables.contains(tableName)) {
                SweepPriorityRowResult newPriority = newPrioritiesByTableName.get(tableName);
                double priority = getSweepPriority(oldPriority, newPriority);
                if (priority > maxPriority) {
                    maxPriority = priority;
                    toSweep = tableName;
                }
            } else {
                toDelete.add(oldPriority.getRowName());
            }
        }

        // Clean up rows for tables that no longer exist.
        tableFactory.getSweepPriorityTable(t.delegate()).delete(toDelete);

        return toSweep;
    }

    private double getSweepPriority(SweepPriorityRowResult oldPriority, SweepPriorityRowResult newPriority) {
        if (AtlasDbConstants.hiddenTables.contains(newPriority.getRowName().getFullTableName())) {
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

        if (writeCount <= 100 + cellsExamined / 100 &&
                TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS) < 180) {
            // Not worth the effort if fewer than 1% of cells are new and we've swept in the last 6 months.
            return 0.0;
        }

        // This ordering function weights one month of no sweeping
        // with the same priority as about 100000 expected cells to sweep.
        return estimatedCellsToSweep + millisSinceSweep * MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT;
    }

    private void saveSweepResults(final SweepProgressRowResult progress,
                                  final SweepResults results) {
        final long cellsDeleted = fromNullable(progress.getCellsDeleted()) + results.getCellsDeleted();
        final long cellsExamined = fromNullable(progress.getCellsExamined()) + results.getCellsExamined();
        if (results.getNextStartRow().isPresent()) {
            saveIntermediateSweepResults(progress, results.getNextStartRow().get(), cellsDeleted, cellsExamined);
            return;
        }

        saveFinalSweepResults(progress, cellsDeleted, cellsExamined);

        log.debug("Finished sweeping {}, examined {} unique cells, deleted {} cells.",
                progress.getFullTableName(), cellsExamined, cellsDeleted);

        if (cellsDeleted > 0) {
            Stopwatch watch = Stopwatch.createStarted();
            kvs.compactInternally(progress.getFullTableName());
            log.debug("Finished performing compactInternally on {} in {} ms.",
                    progress.getFullTableName(), watch.elapsed(TimeUnit.MILLISECONDS));
        }

        // Truncate instead of delete because the progress table contains only
        // a single row that has accumulated many overwrites.
        kvs.truncateTable(tableFactory.getSweepProgressTable(null).getTableName());
    }

    private void saveIntermediateSweepResults(final SweepProgressRowResult progress,
                                              final byte[] nextStartRow,
                                              final long cellsDeleted,
                                              final long cellsExamined) {
        txManager.runTaskWithRetry(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                SweepProgressTable progressTable = tableFactory.getSweepProgressTable(t);
                SweepProgressRow row = SweepProgressRow.of(0);
                progressTable.putFullTableName(row, progress.getFullTableName());
                progressTable.putStartRow(row, nextStartRow);
                progressTable.putCellsDeleted(row, cellsDeleted);
                progressTable.putCellsExamined(row, cellsExamined);
                if (!progress.hasStartRow()) {
                    // This is the first set of results being written for this table.
                    tableFactory.getSweepPriorityTable(t).putWriteCount(
                            SweepPriorityRow.of(progress.getFullTableName()), 0L);
                }
                return null;
            }
        });
    }

    private void saveFinalSweepResults(final SweepProgressRowResult progress,
                                       final long cellsDeleted,
                                       final long cellsExamined) {
        txManager.runTaskWithRetry(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(t);
                SweepPriorityRow row = SweepPriorityRow.of(progress.getFullTableName());
                priorityTable.putCellsDeleted(row, cellsDeleted);
                priorityTable.putCellsExamined(row, cellsExamined);
                priorityTable.putLastSweepTime(row, System.currentTimeMillis());
                if (!progress.hasStartRow()) {
                    // This is the first (and only) set of results being written for this table.
                    priorityTable.putWriteCount(row, 0L);
                }
                return null;
            }
        });
    }

    /**
     * Check whether the table being swept was dropped. If so, stop sweeping it and move on.
     * @return Whether the table being swept was dropped.
     */
    private boolean checkAndRepairTableDrop() {
        try {
            Set<String> tables = kvs.getAllTableNames();
            SweepProgressRowResult result = txManager.runTaskReadOnly(
                    new RuntimeTransactionTask<SweepProgressRowResult>() {
                @Override
                public SweepProgressRowResult execute(Transaction t) {
                    return tableFactory.getSweepProgressTable(t).getRow(SweepProgressRow.of(0L)).orNull();
                }
            });
            if (result == null || tables.contains(result.getFullTableName())) {
                return false;
            }
            kvs.truncateTable(tableFactory.getSweepProgressTable(null).getTableName());
            return true;
        } catch (RuntimeException e) {
            log.error("Failed to check whether the table being swept was dropped. Continuing under the assumption that it wasn't...", e);
            return false;
        }
    }

    private long fromNullable(Long num) {
        return num == null ? 0L : num.longValue();
    }

    private Optional<LockRefreshToken> lockOrRefresh(Optional<LockRefreshToken> previousLocks) throws InterruptedException {
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

        public SweepTransaction(Transaction delegate, long sweepTimestamp) {
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
