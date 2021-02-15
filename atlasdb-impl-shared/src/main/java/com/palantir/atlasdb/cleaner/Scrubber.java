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
package com.palantir.atlasdb.cleaner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.UniformRowNamePartitioner;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.Maps2;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scrubs individuals cells on-demand.
 *
 * The goal of the scrubber is to keep the most recently committed value committed before the
 * immutableTimestamp (or unReadable timestamp) and remove all old values written before this one.
 *
 * @author jweel
 */
@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class Scrubber {
    private static final Logger log = LoggerFactory.getLogger(Scrubber.class);
    private static final int MAX_RETRY_ATTEMPTS = 100;
    private static final int RETRY_SLEEP_INTERVAL_IN_MILLIS = 1000;
    private static final int MAX_DELETES_IN_BATCH = 10_000;

    private final ScheduledExecutorService service =
            PTExecutors.newSingleThreadScheduledExecutor(new NamedThreadFactory("scrubber", true /* daemon */));

    @GuardedBy("this")
    private boolean scrubTaskLaunched = false;

    private final KeyValueService keyValueService;
    private final ScrubberStore scrubberStore;
    private final Supplier<Long> backgroundScrubFrequencyMillisSupplier;
    private final Supplier<Boolean> isScrubEnabled;

    private final Supplier<Long> immutableTimestampSupplier;
    private final Supplier<Long> unreadableTimestampSupplier;
    private final TransactionService transactionService;
    private final Collection<Follower> followers;
    private final MetricsManager metricsManager;
    private final boolean aggressiveScrub;
    private final Supplier<Integer> batchSizeSupplier;
    private final int threadCount;
    private final int readThreadCount;
    private final ExecutorService readerExec;
    private final ExecutorService exec;

    private static final String SCRUBBER_THREAD_PREFIX = "AtlasScrubber";

    // Keep track of threads spawned by scrub, so we don't starve when
    // running scrub for followers.
    private ExecutorInheritableThreadLocal<Boolean> inScrubThread = new ExecutorInheritableThreadLocal<Boolean>() {
        @Override
        public Boolean initialValue() {
            return false;
        }
    };

    public static Scrubber create(
            KeyValueService keyValueService,
            ScrubberStore scrubberStore,
            Supplier<Long> backgroundScrubFrequencyMillisSupplier,
            Supplier<Boolean> isScrubEnabled,
            Supplier<Long> unreadableTimestampSupplier,
            Supplier<Long> immutableTimestampSupplier,
            TransactionService transactionService,
            boolean aggressiveScrub,
            Supplier<Integer> batchSizeSupplier,
            int threadCount,
            int readThreadCount,
            Collection<Follower> followers,
            MetricsManager metricsManager) {
        Scrubber scrubber = new Scrubber(
                keyValueService,
                scrubberStore,
                backgroundScrubFrequencyMillisSupplier,
                isScrubEnabled,
                unreadableTimestampSupplier,
                immutableTimestampSupplier,
                transactionService,
                aggressiveScrub,
                batchSizeSupplier,
                threadCount,
                readThreadCount,
                followers,
                metricsManager);
        return scrubber;
    }

    private Scrubber(
            KeyValueService keyValueService,
            ScrubberStore scrubberStore,
            Supplier<Long> backgroundScrubFrequencyMillisSupplier,
            Supplier<Boolean> isScrubEnabled,
            Supplier<Long> unreadableTimestampSupplier,
            Supplier<Long> immutableTimestampSupplier,
            TransactionService transactionService,
            final boolean aggressiveScrub,
            Supplier<Integer> batchSizeSupplier,
            int threadCount,
            int readThreadCount,
            Collection<Follower> followers,
            MetricsManager metricsManager) {
        this.keyValueService = keyValueService;
        this.scrubberStore = scrubberStore;
        this.backgroundScrubFrequencyMillisSupplier = backgroundScrubFrequencyMillisSupplier;
        this.isScrubEnabled = isScrubEnabled;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
        this.transactionService = transactionService;
        this.aggressiveScrub = aggressiveScrub;
        this.batchSizeSupplier = batchSizeSupplier;
        this.threadCount = threadCount;
        this.readThreadCount = readThreadCount;
        this.followers = followers;
        this.metricsManager = metricsManager;

        this.readerExec = PTExecutors.newFixedThreadPool(readThreadCount, SCRUBBER_THREAD_PREFIX);
        this.exec = PTExecutors.newFixedThreadPool(threadCount, SCRUBBER_THREAD_PREFIX);
    }

    public boolean isInitialized() {
        return keyValueService.isInitialized() && scrubberStore.isInitialized();
    }

    /**
     * The background scrub task has the same semantics as a "CONSERVATIVE_HARD_DELETE" transaction,
     * specifically, it will wait until all transactions that may be affected by scrub have completed
     * or timed out.  Note that whether or not the background task is "CONSERVATIVE" depends NOT on the
     * frequency of the background task, but on the "maxScrubTimestamp"
     */
    private synchronized void launchBackgroundScrubTask(final TransactionManager txManager) {
        if (scrubTaskLaunched) {
            throw new SafeIllegalStateException("Background scrub task has already been launched");
        }
        Runnable scrubTask = () -> {
            int numberOfAttempts = 0;
            while (numberOfAttempts < MAX_RETRY_ATTEMPTS) {
                try {
                    runBackgroundScrubTask(txManager);

                    long sleepDuration = backgroundScrubFrequencyMillisSupplier.get();
                    log.debug("Sleeping {} millis until next execution of scrub task", sleepDuration);
                    Thread.sleep(sleepDuration);
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable t) { // (authorized)
                    if (Thread.interrupted()) {
                        break;
                    }
                    log.error(
                            "Encountered the following error during background scrub task," + " but continuing anyway",
                            t);
                    numberOfAttempts++;
                    lazyWriteMetric(AtlasDbMetricNames.SCRUB_RETRIES, 1);
                    try {
                        Thread.sleep(RETRY_SLEEP_INTERVAL_IN_MILLIS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        };
        service.schedule(scrubTask, 0, TimeUnit.MILLISECONDS);
        scrubTaskLaunched = true;
    }

    @VisibleForTesting
    void runBackgroundScrubTask(final TransactionManager txManager) {
        log.debug("Starting scrub task");

        // Warning: Let T be the hard delete transaction that triggered a scrub, and let S be its
        // start timestamp.  If the locks for T happen to time out right after T checks that its
        // locks are held but right before T writes its commit timestamp (extremely rare case), AND
        // the unreadable timestamp is greater than S, then the scrub task could actually roll back
        // the hard delete transaction (forcing it to abort or retry).  Note that this doesn't affect
        // correctness, but could be an annoying edge cause that causes hard delete to take longer
        // than it otherwise would have.
        Long immutableTimestamp = immutableTimestampSupplier.get();
        Long unreadableTimestamp = unreadableTimestampSupplier.get();
        final long maxScrubTimestamp =
                aggressiveScrub ? immutableTimestamp : Math.min(unreadableTimestamp, immutableTimestamp);
        log.debug(
                "Scrub task immutableTimestamp: {}, unreadableTimestamp: {}, maxScrubTimestamp: {}",
                immutableTimestamp,
                unreadableTimestamp,
                maxScrubTimestamp);
        final int batchSize = (int) Math.ceil(batchSizeSupplier.get() * ((double) threadCount / readThreadCount));

        List<byte[]> rangeBoundaries = new ArrayList<>();
        rangeBoundaries.add(PtBytes.EMPTY_BYTE_ARRAY);
        if (readThreadCount > 1) {
            // This will actually partition into the closest higher power of 2 number of ranges.
            rangeBoundaries.addAll(Ordering.from(UnsignedBytes.lexicographicalComparator())
                    .sortedCopy(new UniformRowNamePartitioner(ValueType.BLOB).getPartitions(readThreadCount - 1)));
        }
        rangeBoundaries.add(PtBytes.EMPTY_BYTE_ARRAY);

        List<Future<Void>> readerFutures = new ArrayList<>();
        final AtomicInteger totalCellsRead = new AtomicInteger(0);
        for (int i = 0; i < rangeBoundaries.size() - 1; i++) {
            final byte[] startRow = rangeBoundaries.get(i);
            final byte[] endRow = rangeBoundaries.get(i + 1);
            readerFutures.add(readerExec.submit(() -> {
                BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> scrubQueue =
                        scrubberStore.getBatchingVisitableScrubQueue(maxScrubTimestamp, startRow, endRow);
                scrubQueue.batchAccept(batchSize, batch -> {
                    for (SortedMap<Long, Multimap<TableReference, Cell>> cells : batch) {
                        // We may actually get more cells than the batch size. The batch size is used
                        // for pulling off the scrub queue, and a single entry in the scrub queue may
                        // match multiple tables. These will get broken down into smaller batches later
                        // on when we actually do deletes.
                        int numCellsRead = scrubSomeCells(cells, txManager, maxScrubTimestamp);
                        int totalRead = totalCellsRead.addAndGet(numCellsRead);
                        log.debug(
                                "Scrub task processed {} cells in a batch, total {} processed so far.",
                                numCellsRead,
                                totalRead);
                        if (!isScrubEnabled.get()) {
                            log.debug("Stopping scrub for banned hours.");
                            break;
                        }
                    }
                    return isScrubEnabled.get();
                });
                return null;
            }));
        }

        for (Future<Void> readerFuture : readerFutures) {
            Futures.getUnchecked(readerFuture);
        }

        log.debug(
                "Scrub background task running at timestamp {} processed a total of {} cells",
                maxScrubTimestamp,
                totalCellsRead.get());

        log.debug("Finished scrub task");
    }

    /* package */ void scrubImmediately(
            final TransactionManager txManager,
            final Multimap<TableReference, Cell> tableNameToCell,
            final long scrubTimestamp,
            final long commitTimestamp) {
        log.debug("Scrubbing a total of {} cells immediately.", tableNameToCell.size());

        // Note that if the background scrub thread is also running at the same time, it will try to scrub
        // the same cells as the current thread (since these cells were queued for scrubbing right before
        // the hard delete transaction committed; while this is unfortunate (because it means we will be
        // doing more work than necessary), the behavior is still correct
        long nextImmutableTimestamp;
        while ((nextImmutableTimestamp = immutableTimestampSupplier.get()) < commitTimestamp) {
            try {
                log.debug(
                        "Sleeping because immutable timestamp {} has not advanced to at least commit timestamp {}",
                        nextImmutableTimestamp,
                        commitTimestamp);
                Thread.sleep(AtlasDbConstants.SCRUBBER_RETRY_DELAY_MILLIS);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for immutableTimestamp to advance past commitTimestamp", e);
            }
        }

        List<Future<Void>> scrubFutures = new ArrayList<>();
        for (List<Map.Entry<TableReference, Cell>> batch :
                Iterables.partition(tableNameToCell.entries(), batchSizeSupplier.get())) {
            final Multimap<TableReference, Cell> batchMultimap = HashMultimap.create();
            for (Map.Entry<TableReference, Cell> e : batch) {
                batchMultimap.put(e.getKey(), e.getValue());
            }

            final Callable<Void> c = () -> {
                log.debug("Scrubbing {} cells immediately.", batchMultimap.size());
                // Here we don't need to check scrub timestamps because we guarantee that scrubImmediately is called
                // AFTER the transaction commits
                scrubCells(txManager, batchMultimap, scrubTimestamp, TransactionType.AGGRESSIVE_HARD_DELETE);
                log.debug("Completed scrub immediately.");
                return null;
            };
            if (!inScrubThread.get()) {
                scrubFutures.add(exec.submit(() -> {
                    inScrubThread.set(true);
                    c.call();
                    return null;
                }));
            } else {
                try {
                    c.call();
                } catch (Exception e) {
                    throw Throwables.throwUncheckedException(e);
                }
            }
        }

        for (Future<Void> future : scrubFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        }
    }

    // Warning: scrubTimestamp MUST be the start timestamp of the hard delete transaction that triggers
    // the scrubbing; we need this start timestamp to check whether the hard delete transaction was
    // actually committed before we do any scrubbing
    /* package */ void queueCellsForScrubbing(Multimap<Cell, TableReference> cellToTableRefs, long scrubTimestamp) {
        if (cellToTableRefs.isEmpty()) {
            return;
        }
        scrubberStore.queueCellsForScrubbing(cellToTableRefs, scrubTimestamp, batchSizeSupplier.get());
        lazyWriteMetric(AtlasDbMetricNames.ENQUEUED_CELLS, cellToTableRefs.size());
    }

    private long getCommitTimestampRollBackIfNecessary(
            long startTimestamp, Multimap<TableReference, Cell> tableNameToCell) {
        Long commitTimestamp = transactionService.get(startTimestamp);
        if (commitTimestamp == null) {
            // Roll back this transaction (note that rolling back arbitrary transactions
            // can never cause correctness issues, only liveness issues)
            try {
                transactionService.putUnlessExists(startTimestamp, TransactionConstants.FAILED_COMMIT_TS);
            } catch (KeyAlreadyExistsException e) {
                String msg = "Could not roll back transaction with start timestamp " + startTimestamp + "; either"
                        + " it was already rolled back (by a different transaction), or it committed successfully"
                        + " before we could roll it back.";
                log.error(
                        "This isn't a bug but it should be very infrequent. {}",
                        msg,
                        new TransactionFailedRetriableException(msg, e));
            }
            commitTimestamp = transactionService.get(startTimestamp);
        }
        if (commitTimestamp == null) {
            throw new RuntimeException("expected commit timestamp to be non-null for startTs: " + startTimestamp);
        }
        if (commitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
            for (TableReference table : tableNameToCell.keySet()) {
                Map<Cell, Long> toDelete = Maps2.createConstantValueMap(tableNameToCell.get(table), startTimestamp);
                keyValueService.delete(table, Multimaps.forMap(toDelete));
            }
        }
        return commitTimestamp;
    }

    /**
     * Scrubs some cells.
     *
     * @return number of cells read from _scrub table
     */
    private int scrubSomeCells(
            SortedMap<Long, Multimap<TableReference, Cell>> scrubTimestampToTableNameToCell,
            TransactionManager txManager,
            long maxScrubTimestamp) {
        log.trace("Attempting to scrub cells: {}", scrubTimestampToTableNameToCell);

        if (log.isDebugEnabled()) {
            int numCells = 0;
            Set<TableReference> tables = new HashSet<>();
            for (Multimap<TableReference, Cell> v : scrubTimestampToTableNameToCell.values()) {
                tables.addAll(v.keySet());
                numCells += v.size();
            }
            log.debug("Attempting to scrub {} cells from tables {}", numCells, tables);
        }

        if (scrubTimestampToTableNameToCell.size() == 0) {
            return 0; // No cells left to scrub
        }

        int numCellsReadFromScrubTable = 0;
        List<Future<Void>> scrubFutures = new ArrayList<>();
        Map<TableReference, Multimap<Cell, Long>> failedWrites = new HashMap<>();

        for (Map.Entry<Long, Multimap<TableReference, Cell>> entry : scrubTimestampToTableNameToCell.entrySet()) {
            final long scrubTimestamp = entry.getKey();
            final Multimap<TableReference, Cell> tableNameToCell = entry.getValue();

            numCellsReadFromScrubTable += tableNameToCell.size();

            // This is CRITICAL; don't scrub if the hard delete transaction didn't actually finish
            // (we still remove it from the _scrub table with the call to markCellsAsScrubbed though),
            // or else we could cause permanent data loss if the hard delete transaction failed after
            // queuing cells to scrub but before successfully committing
            long commitTimestamp = getCommitTimestampRollBackIfNecessary(scrubTimestamp, tableNameToCell);
            if (commitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                for (Map.Entry<TableReference, Collection<Cell>> cells :
                        tableNameToCell.asMap().entrySet()) {
                    Multimap<Cell, Long> failedCells = failedWrites.get(cells.getKey());
                    if (failedCells == null) {
                        failedCells = ArrayListMultimap.create(cells.getValue().size(), 2);
                        failedWrites.put(cells.getKey(), failedCells);
                    }
                    for (Cell cell : cells.getValue()) {
                        failedCells.put(cell, scrubTimestamp);
                    }
                }
            } else if (commitTimestamp < maxScrubTimestamp) {
                for (final List<Map.Entry<TableReference, Cell>> batch :
                        Iterables.partition(tableNameToCell.entries(), batchSizeSupplier.get())) {
                    final Multimap<TableReference, Cell> batchMultimap = HashMultimap.create();
                    for (Map.Entry<TableReference, Cell> e : batch) {
                        batchMultimap.put(e.getKey(), e.getValue());
                    }
                    scrubFutures.add(exec.submit(() -> {
                        scrubCells(
                                txManager,
                                batchMultimap,
                                scrubTimestamp,
                                aggressiveScrub ? TransactionType.AGGRESSIVE_HARD_DELETE : TransactionType.HARD_DELETE);
                        return null;
                    }));
                }
            }
            // else {
            //     We cannot scrub this yet because not all transactions can read this value.
            // }
        }

        for (Future<Void> future : scrubFutures) {
            Futures.getUnchecked(future);
        }

        if (!failedWrites.isEmpty()) {
            scrubberStore.markCellsAsScrubbed(failedWrites, batchSizeSupplier.get());
        }

        log.trace("Finished scrubbing cells: {}", scrubTimestampToTableNameToCell);

        if (log.isDebugEnabled()) {
            Set<TableReference> tables = new HashSet<>();
            for (Multimap<TableReference, Cell> v : scrubTimestampToTableNameToCell.values()) {
                tables.addAll(v.keySet());
            }
            long minTimestamp = Collections.min(scrubTimestampToTableNameToCell.keySet());
            long maxTimestamp = Collections.max(scrubTimestampToTableNameToCell.keySet());
            log.debug(
                    "Finished scrubbing {} cells at {} timestamps ({}...{}) from tables {}",
                    numCellsReadFromScrubTable,
                    scrubTimestampToTableNameToCell.size(),
                    minTimestamp,
                    maxTimestamp,
                    tables);
        }

        return numCellsReadFromScrubTable;
    }

    private void scrubCells(
            TransactionManager txManager,
            Multimap<TableReference, Cell> tableNameToCells,
            long scrubTimestamp,
            Transaction.TransactionType transactionType) {
        Map<TableReference, Multimap<Cell, Long>> allCellsToMarkScrubbed =
                Maps.newHashMapWithExpectedSize(tableNameToCells.keySet().size());
        for (Map.Entry<TableReference, Collection<Cell>> entry :
                tableNameToCells.asMap().entrySet()) {
            TableReference tableRef = entry.getKey();
            log.debug(
                    "Attempting to immediately scrub {} cells from table {}",
                    entry.getValue().size(),
                    tableRef);
            for (List<Cell> cells : Iterables.partition(entry.getValue(), batchSizeSupplier.get())) {
                Multimap<Cell, Long> allTimestamps =
                        keyValueService.getAllTimestamps(tableRef, ImmutableSet.copyOf(cells), scrubTimestamp);
                Multimap<Cell, Long> timestampsToDelete =
                        Multimaps.filterValues(allTimestamps, v -> !v.equals(Value.INVALID_VALUE_TIMESTAMP));

                // If transactionType == TransactionType.AGGRESSIVE_HARD_DELETE this might
                // force other transactions to abort or retry
                deleteCellsAtTimestamps(txManager, tableRef, timestampsToDelete, transactionType);

                Multimap<Cell, Long> cellsToMarkScrubbed = HashMultimap.create(allTimestamps);
                for (Cell cell : cells) {
                    cellsToMarkScrubbed.put(cell, scrubTimestamp);
                }
                allCellsToMarkScrubbed.put(tableRef, cellsToMarkScrubbed);
            }
            log.debug(
                    "Immediately scrubbed {} cells from table {}",
                    entry.getValue().size(),
                    tableRef);
        }
        scrubberStore.markCellsAsScrubbed(allCellsToMarkScrubbed, batchSizeSupplier.get());
        lazyWriteMetric(AtlasDbMetricNames.SCRUBBED_CELLS, allCellsToMarkScrubbed.size());
    }

    private void deleteCellsAtTimestamps(
            TransactionManager txManager,
            TableReference tableRef,
            Multimap<Cell, Long> cellToTimestamp,
            Transaction.TransactionType transactionType) {
        if (!cellToTimestamp.isEmpty()) {
            for (Follower follower : followers) {
                follower.run(txManager, tableRef, cellToTimestamp.keySet(), transactionType);
            }
            keyValueService.addGarbageCollectionSentinelValues(tableRef, cellToTimestamp.keySet());
            for (List<Map.Entry<Cell, Long>> batch :
                    Iterables.partition(cellToTimestamp.entries(), MAX_DELETES_IN_BATCH)) {
                ImmutableMultimap.Builder<Cell, Long> builder = ImmutableMultimap.builder();
                batch.forEach(builder::put);
                keyValueService.delete(tableRef, builder.build());
                lazyWriteMetric(AtlasDbMetricNames.DELETED_CELLS, batch.size());
            }
        }
    }

    private void lazyWriteMetric(String name, long value) {
        metricsManager.registerOrGetMeter(Scrubber.class, name).mark(value);
    }

    public long getUnreadableTimestamp() {
        return unreadableTimestampSupplier.get();
    }

    public void start(TransactionManager txManager) {
        launchBackgroundScrubTask(txManager);
    }

    public void shutdown() {
        exec.shutdown();
        readerExec.shutdown();
        service.shutdownNow();
        boolean shutdown = false;
        try {
            shutdown = service.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down the scrubber. This shouldn't happen.");
            Thread.currentThread().interrupt();
        }
        if (!shutdown) {
            log.error("Failed to shutdown scrubber in a timely manner. The scrubber may attempt"
                    + " to access a key value service after the key value service closes. This shouldn't"
                    + " cause any problems, but may result in some scary looking error messages.");
        }
    }
}
