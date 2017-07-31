/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.RemoteLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public final class BackgroundSweeperImpl implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);
    private final RemoteLockService lockService;
    private final NextTableToSweepProvider nextTableToSweepProvider;
    private final Supplier<Boolean> isSweepEnabled;
    private final SpecificTableSweeper specificTableSweeper;
    private final int numberOfConcurrentSweeps;

    static volatile double batchSizeMultiplier = 1.0;

    @VisibleForTesting
    BackgroundSweeperImpl(
            RemoteLockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            Supplier<Boolean> isSweepEnabled,
            SpecificTableSweeper specificTableSweeper,
            int numberOfConcurrentSweeps) {
        this.lockService = lockService;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.isSweepEnabled = isSweepEnabled;
        this.specificTableSweeper = specificTableSweeper;
        this.numberOfConcurrentSweeps = numberOfConcurrentSweeps;
    }

    public static BackgroundSweeperImpl create(
            Supplier<Boolean> isSweepEnabled,
            SpecificTableSweeper specificTableSweeper,
            int numberOfConcurrentSweeps) {
        NextTableToSweepProvider nextTableToSweepProvider = new NextTableToSweepProviderImpl(
                specificTableSweeper.getKvs(), specificTableSweeper.getSweepPriorityStore());
        return new BackgroundSweeperImpl(
                specificTableSweeper.getTxManager().getLockService(),
                nextTableToSweepProvider,
                isSweepEnabled,
                specificTableSweeper,
                numberOfConcurrentSweeps);
    }

    @Override
    public void run() {
        try (SweepLocks locks = createSweepLocks()) {
            // Wait a while before starting so short lived clis don't try to sweep.
            log.info("Starting background sweeper.");
            while (true) {
                if (isSweepEnabled.get()) {
                    grabLocksAndRun(locks);
                }
            }
        } catch (InterruptedException e) {
            log.warn("Shutting down background sweeper. Please restart the service to rerun background sweep.");
        }
    }

    private void grabLocksAndRun(SweepLocks locks) throws InterruptedException {
        boolean sweptSuccessfully = false;
        try {
            if (locks.lockOrRefreshSweepLease()) {
                sweptSuccessfully = runOnce();
            } else {
                log.debug("Skipping sweep because sweep is running elsewhere.");
            }
        } catch (InsufficientConsistencyException e) {
            log.warn("Could not sweep because not all nodes of the database are online.", e);
        } catch (RuntimeException e) {
            specificTableSweeper.getSweepMetrics().sweepError();
            if (checkAndRepairTableDrop()) {
                log.info("The table being swept by the background sweeper was dropped, moving on...");
            } else {
                SweepBatchConfig lastBatchConfig = getAdjustedBatchConfig();
                log.warn("The background sweep job failed unexpectedly with batch config {}."
                                + " Attempting to continue with a lower batch size...",
                        SafeArg.of("cell batch size", lastBatchConfig),
                        e);
                // Cut batch size in half, always sweep at least one row (we round down).
                batchSizeMultiplier = Math.max(batchSizeMultiplier / 2, 1.5 / lastBatchConfig.candidateBatchSize());
            }
        }

        if (sweptSuccessfully) {
            batchSizeMultiplier = Math.min(1.0, batchSizeMultiplier * 1.01);
        }
    }

    @VisibleForTesting
    boolean runOnce() {
        Optional<TableToSweep> tableToSweep = getTableToSweep();
        if (!tableToSweep.isPresent()) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        } else {
            specificTableSweeper.runOnceForTable(tableToSweep.get(), Optional.empty(), true);
            return true;
        }
    }

    private SweepBatchConfig getAdjustedBatchConfig() {
        SweepBatchConfig baseConfig = specificTableSweeper.getSweepBatchConfig().get();
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(adjustBatchParameter(baseConfig.maxCellTsPairsToExamine()))
                .candidateBatchSize(adjustBatchParameter(baseConfig.candidateBatchSize()))
                .deleteBatchSize(adjustBatchParameter(baseConfig.deleteBatchSize()))
                .build();
    }

    static int adjustBatchParameter(int parameterValue) {
        return Math.max(1, (int) (batchSizeMultiplier * parameterValue));
    }

    private Optional<TableToSweep> getTableToSweep() {
        return specificTableSweeper.getTxManager().runTaskWithRetry(
                new TransactionTask<Optional<TableToSweep>, RuntimeException>() {
                    @Override
                    public Optional<TableToSweep> execute(Transaction tx) {
                        Optional<SweepProgress> progress = specificTableSweeper.getSweepProgressStore().loadProgress(
                                tx);
                        if (progress.isPresent()) {
                            return Optional.of(new TableToSweep(progress.get().tableRef(), progress.get()));
                        } else {
                            Optional<TableReference> nextTable = nextTableToSweepProvider.chooseNextTableToSweep(
                                    tx, specificTableSweeper.getSweepRunner().getConservativeSweepTimestamp());
                            if (nextTable.isPresent()) {
                                log.debug("Now starting to sweep next table.", UnsafeArg.of("table name", nextTable));
                                return Optional.of(new TableToSweep(nextTable.get(), null));
                            } else {
                                return Optional.empty();
                            }
                        }
                    }
                });
    }

    /**
     * Check whether the table being swept was dropped. If so, stop sweeping it and move on.
     *
     * @return Whether the table being swept was dropped
     */
    boolean checkAndRepairTableDrop() {
        try {
            Set<TableReference> tables = specificTableSweeper.getKvs().getAllTableNames();
            Optional<SweepProgress> progress = specificTableSweeper.getTxManager().runTaskReadOnly(
                    specificTableSweeper.getSweepProgressStore()::loadProgress);
            if (!progress.isPresent() || tables.contains(progress.get().tableRef())) {
                return false;
            } else {
                specificTableSweeper.getSweepProgressStore().clearProgress();
                return true;
            }
        } catch (RuntimeException e) {
            log.error("Failed to check whether the table being swept was dropped."
                    + " Continuing under the assumption that it wasn't...", e);
            return false;
        }
    }

    @VisibleForTesting
    SweepLocks createSweepLocks() {
        return new SweepLocks(lockService, numberOfConcurrentSweeps);
    }
}
