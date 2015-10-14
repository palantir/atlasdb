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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRow;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.Throwables;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;

public class BackgroundSweeper implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeper.class);
    private final LockAwareTransactionManager txManager;
    private final KeyValueService kvs;
    private final SweepTaskRunner sweepRunner;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final SweepTableFactory tableFactory;
    private Thread daemon;

    public BackgroundSweeper(LockAwareTransactionManager txManager,
                             KeyValueService kvs,
                             SweepTaskRunner sweepRunner,
                             Supplier<Boolean> isSweepEnabled,
                             Supplier<Long> sweepPauseMillis,
                             SweepTableFactory tableFactory) {
        this.txManager = txManager;
        this.kvs = kvs;
        this.sweepRunner = sweepRunner;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.tableFactory = tableFactory;
    }

    public synchronized void runInBackground() {
        daemon = new Thread(this);
        daemon.setDaemon(true);
        daemon.setName("BackgroundSweeper");
        daemon.start();
    }

    @Override
    public void run() {
        Optional<HeldLocksToken> locks = Optional.absent();
        try {
            // Wait a while before starting so short lived clis don't try to sweep.
            Thread.sleep(20 * (1000 + sweepPauseMillis.get()));
            log.info("Starting background sweeper.");
            while (true) {
                boolean sweptSuccessfully = false;
                try {
                    if (isSweepEnabled.get()) {
                        locks = lockOrRefresh(locks);
                        if (locks.isPresent()) {
                            sweptSuccessfully = txManager.runTaskWithLocksWithRetry(
                                    ImmutableList.of(locks.get()),
                                    Suppliers.<LockRequest>ofInstance(null),
                                    LockAwareTransactionTasks.asLockAware(new RuntimeTransactionTask<Boolean>() {
                                @Override
                                public Boolean execute(Transaction t) {
                                    return runOnce(t);
                                }
                            }));
                        } else {
                            log.debug("Skipping sweep because sweep is running elsewhere.");
                        }
                    } else {
                        log.debug("Skipping sweep because it is currently disabled.");
                    }
                } catch (InsufficientConsistencyException e) {
                    log.warn("Could not sweep because not all nodes of the database are online.", e);
                } catch (RuntimeException e) {
                    log.error("The background sweep job failed unexpectedly. Attempting to continue regardless...", e);
                }
                if (sweptSuccessfully) {
                    Thread.sleep(sweepPauseMillis.get());
                } else {
                    Thread.sleep(20 * (1000 + sweepPauseMillis.get()));
                }
            }
        } catch (InterruptedException e) {
            log.info("Shutting down background sweeper.");
        } finally {
            if (locks.isPresent()) {
                txManager.getLockService().unlock(locks.get().getLockRefreshToken());
            }
        }
    }

    private boolean runOnce(Transaction t) {
        SweepProgressTable progressTable = tableFactory.getSweepProgressTable(t);
        Optional<SweepProgressRowResult> optProgress = progressTable.getRow(SweepProgressRow.of(0));
        if (!optProgress.isPresent()) {
            optProgress = chooseNextTableToSweep(t);
        }
        if (!optProgress.isPresent()) {
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        }
        SweepProgressRowResult progress = optProgress.get();
        Stopwatch watch = Stopwatch.createStarted();
        SweepResults results = sweepRunner.run(progress.getFullTableName(), progress.getStartRow());
        log.debug("Swept {} unique cells from {} and performed {} deletions in {} ms.",
                results.getCellsExamined(), progress.getFullTableName(), results.getCellsDeleted(), watch.elapsed(TimeUnit.MILLISECONDS));
        saveSweepResults(t, progress, results);
        return true;
    }

    private Optional<SweepProgressRowResult> chooseNextTableToSweep(Transaction t) {
        Set<String> allTables = Sets.difference(kvs.getAllTableNames(), AtlasDbConstants.hiddenTables);
        SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(t);
        List<SweepPriorityRowResult> results = priorityTable.getAllRowsUnordered().immutableCopy();
        Set<String> unsweptTables = Sets.difference(allTables, ImmutableSet.copyOf(Iterables.transform(results,
                Functions.compose(SweepPriorityRow.getFullTableNameFun(), SweepPriorityRowResult.getRowNameFun()))));
        Optional<String> optTableName = getTableToSweep(unsweptTables, results);
        if (!optTableName.isPresent()) {
            return Optional.absent();
        }
        String tableName = optTableName.get();
        RowResult<byte[]> rawResult = RowResult.<byte[]>create(SweepProgressRow.of(0).persistToBytes(),
                ImmutableSortedMap.<byte[], byte[]>orderedBy(UnsignedBytes.lexicographicalComparator())
                    .put(SweepProgressTable.SweepProgressNamedColumn.FULL_TABLE_NAME.getShortName(),
                         SweepProgressTable.FullTableName.of(tableName).persistValue())
                    .build());

        // We zero out the write count when we start sweeping a table, then reduce
        // it when we finish sweeping to account for new writes made during sweep that
        // were likely swept along the way.
        priorityTable.putWriteCount(SweepPriorityRow.of(tableName), 0L);

        log.info("Now starting to sweep {}.", tableName);
        return Optional.of(SweepProgressRowResult.of(rawResult));
    }

    private Optional<String> getTableToSweep(Set<String> unsweptTables, List<SweepPriorityRowResult> results) {
        if (!unsweptTables.isEmpty()) {
            return Optional.of(Iterables.get(unsweptTables, 0));
        }
        double maxPriority = 0.0;
        String tableName = null;
        for (SweepPriorityRowResult result : results) {
            double priority = getSweepPriority(result);
            if (priority > maxPriority) {
                maxPriority = priority;
                tableName = result.getRowName().getFullTableName();
            }
        }
        return Optional.fromNullable(tableName);
    }

    private double getSweepPriority(SweepPriorityRowResult result) {
        if (AtlasDbConstants.hiddenTables.contains(result.getRowName().getFullTableName())) {
            // Never sweep hidden tables
            return 0.0;
        }
        if (!result.hasLastSweepTime()) {
            // Highest priority if we've never swept it before
            return Double.MAX_VALUE;
        }
        long cellsDeleted = Math.max(1, result.getCellsDeleted());
        long cellsExamined = Math.max(1, result.getCellsExamined());
        long writeCount = Math.max(1, getOldWriteCount(result.getRowName()));
        double previousEfficacy = 1.0 * cellsDeleted / cellsExamined;
        double estimatedCellsToSweep = previousEfficacy * writeCount;
        long millisSinceSweep = System.currentTimeMillis() - result.getLastSweepTime();

        if (writeCount < cellsExamined / 100 && TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS) < 180) {
            // Not worth the effort if fewer than 1% of cells are new and we've swept in the last 6 months.
            return 0.0;
        }

        // This ordering function weights one month of no sweeping
        // with the same priority as about 100000 expected cells to sweep.
        return estimatedCellsToSweep + millisSinceSweep / 25;
    }

    // We don't want the current write count to the table, we want the write count as it was at the
    // current unreadable timestamp, aka, the number of new writes we can actually see while sweeping.
    private long getOldWriteCount(SweepPriorityRow priorityRow) {
        String tableName = priorityRow.getFullTableName();
        byte[] row = priorityRow.persistToBytes();
        byte[] col = SweepPriorityNamedColumn.WRITE_COUNT.getShortName();
        long timestamp = sweepRunner.getSweepTimestamp(tableName);
        Map<Cell, Value> oldResult = kvs.get(tableName, ImmutableMap.of(Cell.create(row, col), timestamp));
        if (oldResult.isEmpty()) {
            // Only happens if there were no writes at this point in the past.
            return 0;
        }
        Value value = Iterables.getOnlyElement(oldResult.values());
        if (value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP) {
            // We swept the sweep table. This is fine though. If we wait, the sweep timestamp will advance
            // past a point that hasn't been swept, and the table will have a higher priority of being swept
            // than the sweep table.
            return 0;
        }
        return SweepPriorityTable.WriteCount.BYTES_HYDRATOR.hydrateFromBytes(value.getContents()).getValue();
    }

    private void saveSweepResults(Transaction t, SweepProgressRowResult progress, SweepResults results) {
        long cellsDeleted = getCellsDeleted(progress, results);
        long cellsExamined = getCellsExamined(progress, results);
        if (results.getNextStartRow().isPresent()) {
            SweepProgressTable progressTable = tableFactory.getSweepProgressTable(t);
            SweepProgressRow row = SweepProgressRow.of(0);
            progressTable.putFullTableName(row, progress.getFullTableName());
            progressTable.putStartRow(row, results.getNextStartRow().get());
            progressTable.putCellsDeleted(row, cellsDeleted);
            progressTable.putCellsExamined(row, cellsExamined);
        } else {
            SweepPriorityTable priorityTable = tableFactory.getSweepPriorityTable(t);
            SweepPriorityRow row = SweepPriorityRow.of(progress.getFullTableName());
            priorityTable.putCellsDeleted(row, cellsDeleted);
            priorityTable.putCellsExamined(row, cellsExamined);
            priorityTable.putLastSweepTime(row, System.currentTimeMillis());

            // Estimate that half of the new writes that occurred while sweeping got swept.
            Long currentWriteCount = priorityTable.getWriteCounts(ImmutableList.of(row)).get(row);
            long oldWriteCount = getOldWriteCount(row);
            if (currentWriteCount != null && currentWriteCount > oldWriteCount) {
                priorityTable.putWriteCount(row, currentWriteCount - oldWriteCount / 2);
            }

            log.info("Finished sweeping {}, examined {} unique cells, deleted {} cells.",
                    progress.getFullTableName(), cellsExamined, cellsDeleted);

            if (cellsDeleted > 0) {
                Stopwatch watch = Stopwatch.createStarted();
                kvs.compactInternally(progress.getFullTableName());
                log.info("Finished performing compactInternally on {} in {} ms.",
                        progress.getFullTableName(), watch.elapsed(TimeUnit.MILLISECONDS));
            }

            // Truncate instead of delete because the progress table contains only
            // a single row that has accumulated many overwrites.
            kvs.truncateTable(tableFactory.getSweepProgressTable(t).getTableName());
        }
    }

    private long getCellsDeleted(SweepProgressRowResult progress, SweepResults results) {
        if (progress.hasCellsDeleted()) {
            return progress.getCellsDeleted() + results.getCellsDeleted();
        } else {
            return results.getCellsDeleted();
        }
    }

    private long getCellsExamined(SweepProgressRowResult progress, SweepResults results) {
        if (progress.hasCellsExamined()) {
            return progress.getCellsExamined() + results.getCellsExamined();
        } else {
            return results.getCellsExamined();
        }
    }

    private Optional<HeldLocksToken> lockOrRefresh(Optional<HeldLocksToken> previousLocks) throws InterruptedException {
        if (previousLocks.isPresent()) {
            LockRefreshToken refreshToken = previousLocks.get().getLockRefreshToken();
            Set<LockRefreshToken> refreshedTokens = txManager.getLockService()
                    .refreshLockRefreshTokens(ImmutableList.of(refreshToken));
            if (refreshedTokens.isEmpty()) {
                return Optional.absent();
            } else {
                return previousLocks;
            }
        } else {
            LockDescriptor lock = StringLockDescriptor.of("atlasdb sweep");
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE)).doNotBlock().build();
            HeldLocksToken response = txManager.getLockService().lockAndGetHeldLocksAnonymously(request);
            if (response != null) {
                return Optional.of(response);
            } else {
                return Optional.absent();
            }
        }
    }

    public synchronized void shutdown() {
        if (daemon == null) {
            return;
        }
        log.info("Signalling background sweeper to shut down.");
        daemon.interrupt();
        try {
            daemon.join();
            daemon = null;
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
