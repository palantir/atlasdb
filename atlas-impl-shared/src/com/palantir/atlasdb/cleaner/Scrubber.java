// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.cleaner;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.Maps2;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

/**
 * Scrubs individuals cells on-demand.
 *
 * The goal of the scrubber is to keep the most recently committed value committed before the
 * immutableTimestamp (or unReadable timestamp) and remove all old values written before this one.
 *
 * @author jweel
 */
// TODO (ejin): Rename 'scrubbing' to 'purging' and 'sweeping' to 'garbage collection'?
// TODO (ejin): Remove Vineet's GC framework so that Jaap's is canonical?
public final class Scrubber {
    private static final Logger log = LoggerFactory.getLogger(Scrubber.class);
    private static final int AGGRESSIVE_SCRUB_FREQUENCY_IN_MILLIS = 5 * 60 * 1000; // 5 min
    private static final int MAX_RETRY_ATTEMPTS = 100;
    private static final int RETRY_SLEEP_INTERVAL_IN_MILLIS = 1000;

    private final ScheduledExecutorService service = PTExecutors.newSingleThreadScheduledExecutor();
    @GuardedBy("this") private boolean scrubTaskLaunched = false;

    private final KeyValueService keyValueService;
    private final ScrubberStore scrubberStore;
    private final Supplier<Long> backgroundScrubFrequencyMillisSupplier;

    private final Supplier<Long> immutableTimestampSupplier;
    private final Supplier<Long> unreadableTimestampSupplier;
    private final TransactionService transactionService;
    private final Collection<Follower> followers;
    private final boolean aggressiveScrub;
    private final Supplier<Integer> batchSizeSupplier;
    private final int threadCount;
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

    public static Scrubber create(KeyValueService keyValueService,
                                  ScrubberStore scrubberStore,
                                  Supplier<Long> backgroundScrubFrequencyMillisSupplier,
                                  Supplier<Long> unreadableTimestampSupplier,
                                  Supplier<Long> immutableTimestampSupplier,
                                  TransactionService transactionService,
                                  boolean aggressiveScrub,
                                  Supplier<Integer> batchSizeSupplier,
                                  int threadCount,
                                  Collection<Follower> followers) {
        Scrubber scrubber = new Scrubber(
                keyValueService,
                scrubberStore,
                backgroundScrubFrequencyMillisSupplier,
                unreadableTimestampSupplier,
                immutableTimestampSupplier,
                transactionService,
                aggressiveScrub,
                batchSizeSupplier,
                threadCount,
                followers);
        return scrubber;
    }

    private Scrubber(KeyValueService keyValueService,
                     ScrubberStore scrubberStore,
                     Supplier<Long> backgroundScrubFrequencyMillisSupplier,
                     Supplier<Long> unreadableTimestampSupplier,
                     Supplier<Long> immutableTimestampSupplier,
                     TransactionService transactionService,
                     final boolean aggressiveScrub,
                     Supplier<Integer> batchSizeSupplier,
                     int threadCount,
                     Collection<Follower> followers) {
        this.keyValueService = keyValueService;
        this.scrubberStore = scrubberStore;
        this.backgroundScrubFrequencyMillisSupplier = backgroundScrubFrequencyMillisSupplier;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
        this.transactionService = transactionService;
        this.aggressiveScrub = aggressiveScrub;
        this.batchSizeSupplier = batchSizeSupplier;
        this.threadCount = threadCount;
        this.followers = followers;
        NamedThreadFactory threadFactory = new NamedThreadFactory(SCRUBBER_THREAD_PREFIX, true);
        this.exec = PTExecutors.newFixedThreadPool(threadCount, threadFactory);
    }

    /**
     * The background scrub task has the same semantics as a "CONSERVATIVE_HARD_DELETE" transaction,
     * specifically, it will wait until all transactions that may be affected by scrub have completed
     * or timed out.  Note that whether or not the background task is "CONSERVATIVE" depends NOT on the
     * frequency of the background task, but on the "maxScrubTimestamp"
     */
    private synchronized void launchBackgroundScrubTask(final TransactionManager txManager) {
        if (scrubTaskLaunched) {
            throw new IllegalStateException("Background scrub task has already been launched");
        }
        Runnable scrubTask = new Runnable() {
            @Override
            public void run() {
                int numberOfAttempts = 0;
                while (numberOfAttempts < MAX_RETRY_ATTEMPTS) {
                    try {
                        log.info("Starting scrub task");
                        while (true) {
                            // Warning: Let T be the hard delete transaction that triggered a scrub, and let S be its
                            // start timestamp.  If the locks for T happen to time out right after T checks that its
                            // locks are held but right before T writes its commit timestamp (extremely rare case), AND
                            // the unreadable timestamp is greater than S, then the scrub task could actually roll back
                            // the hard delete transaction (forcing it to abort or retry).  Note that this doesn't affect
                            // correctness, but could be an annoying edge cause that causes hard delete to take longer
                            // than it otherwise would have.
                            Long immutableTimestamp = immutableTimestampSupplier.get();
                            Long unreadableTimestamp = unreadableTimestampSupplier.get();
                            final long maxScrubTimestamp = aggressiveScrub ? immutableTimestamp :
                                    Math.min(unreadableTimestamp, immutableTimestamp);
                            if (log.isInfoEnabled()) {
                                log.info("Scrub task immutableTimestamp: " + immutableTimestamp
                                        + ", unreadableTimestamp: " + unreadableTimestamp
                                        + ", min: " + maxScrubTimestamp);
                            }
                            int numCellsReadFromScrubTable = scrubSomeCells(txManager, threadCount * batchSizeSupplier.get(), maxScrubTimestamp);
                            if (numCellsReadFromScrubTable == 0) {
                                break;
                            }
                            if (log.isInfoEnabled()) {
                                log.info("Scrub task scrubbed " + numCellsReadFromScrubTable + " cells");
                            }
                        }
                        log.info("Finished scrub task");

                        long sleepDuration = backgroundScrubFrequencyMillisSupplier.get();
                        log.info(String.format("Sleeping %d millis until next execution of scrub task", sleepDuration));
                        Thread.sleep(sleepDuration);
                    } catch (InterruptedException e) {
                        if (service.isShutdown()) {
                            break;
                        } else {
                            log.error("Interrupted unexpectedly during background scrub task, but continuing anyway", e);
                        }
                    } catch (Throwable t) { // (authorized)
                        log.error("Encountered the following error during background scrub task, but continuing anyway", t);
                        numberOfAttempts++;
                        try {
                            Thread.sleep(RETRY_SLEEP_INTERVAL_IN_MILLIS);
                        } catch (InterruptedException e) {
                            log.error("Interrupted while waiting to retry, but continuing anyway.", e);
                            // Restore interrupt
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        };
        service.schedule(scrubTask, 0, TimeUnit.MILLISECONDS);
        scrubTaskLaunched = true;
    }

    /* package */ void scrubImmediately(final TransactionManager txManager,
                                        final Multimap<String, Cell> tableNameToCell,
                                        final long scrubTimestamp,
                                        final long commitTimestamp) {
        if (log.isInfoEnabled()) {
            log.info("Scrubbing a total of " + tableNameToCell.size() + " cells immediately.");
        }

        // Note that if the background scrub thread is also running at the same time, it will try to scrub
        // the same cells as the current thread (since these cells were queued for scrubbing right before
        // the hard delete transaction committed; while this is unfortunate (because it means we will be
        // doing more work than necessary), the behavior is still correct
        long nextImmutableTimestamp;
        while ((nextImmutableTimestamp = immutableTimestampSupplier.get()) < commitTimestamp) {
            try {
                if (log.isInfoEnabled()) {
                    log.info(String.format(
                            "Sleeping because immutable timestamp %d has not advanced to at least commit timestamp %d",
                            nextImmutableTimestamp,
                            commitTimestamp));
                }
                Thread.sleep(AtlasDbConstants.SCRUBBER_RETRY_DELAY_MILLIS);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for immutableTimestamp to advance past commitTimestamp", e);
            }
        }

        List<Future<Void>> scrubFutures = Lists.newArrayList();
        for (List<Entry<String, Cell>> batch : Iterables.partition(tableNameToCell.entries(), batchSizeSupplier.get())) {
            final Multimap<String, Cell> batchMultimap = HashMultimap.create();
            for (Entry<String, Cell> e : batch) {
                batchMultimap.put(e.getKey(), e.getValue());
            }

            final Callable<Void> c =new Callable<Void>()  {
                @Override
                public Void call() throws Exception {
                    if (log.isInfoEnabled()) {
                        log.info("Scrubbing " + batchMultimap.size() + " cells immediately.");
                    }

                    // Here we don't need to check scrub timestamps because we guarantee that scrubImmediately is called
                    // AFTER the transaction commits
                    scrubCells(txManager, batchMultimap, scrubTimestamp, TransactionType.AGGRESSIVE_HARD_DELETE);

                    Multimap<Cell, Long> cellToScrubTimestamp = HashMultimap.create();

                    cellToScrubTimestamp = Multimaps.invertFrom(
                            Multimaps.index(batchMultimap.values(), Functions.constant(scrubTimestamp)),
                            cellToScrubTimestamp);

                    scrubberStore.markCellsAsScrubbed(cellToScrubTimestamp, batchSizeSupplier.get());

                    if (log.isInfoEnabled()) {
                        log.info("Completed scrub immediately.");
                    }
                    return null;
                }
            };
            if (!inScrubThread.get()) {
                scrubFutures.add(exec.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        inScrubThread.set(true);
                        c.call();
                        return null;
                    }}));
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
    /* package */ void queueCellsForScrubbing(Multimap<String, Cell> tableNameToCell, long scrubTimestamp) {
        if (tableNameToCell.isEmpty()) {
            return;
        }
        scrubberStore.queueCellsForScrubbing(tableNameToCell, scrubTimestamp, batchSizeSupplier.get());
    }

    private long getCommitTimestampRollBackIfNecessary(long startTimestamp,
                                                       Multimap<String, Cell> tableNameToCell) {
        Long commitTimestamp = transactionService.get(startTimestamp);
        if (commitTimestamp == null) {
            // Roll back this transaction (note that rolling back arbitrary transactions
            // can never cause correctness issues, only liveness issues)
            try {
                transactionService.putUnlessExists(startTimestamp, TransactionConstants.FAILED_COMMIT_TS);
            } catch (KeyAlreadyExistsException e) {
                String msg = "Could not roll back transaction with start timestamp " + startTimestamp + "; either" +
                        " it was already rolled back (by a different transaction), or it committed successfully" +
                        " before we could roll it back.";
                log.error("This isn't a bug but it should be very infrequent. " + msg,
                        new TransactionFailedRetriableException(msg, e));
            }
            commitTimestamp = transactionService.get(startTimestamp);
        }
        if (commitTimestamp == null) {
            throw new RuntimeException("expected commit timestamp to be non-null for startTs: " + startTimestamp);
        }
        if (commitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
            for (String table : tableNameToCell.keySet()) {
                Map<Cell, Long> toDelete = Maps2.createConstantValueMap(tableNameToCell.get(table), startTimestamp);
                keyValueService.delete(table, Multimaps.forMap(toDelete));
            }
        }
        return commitTimestamp;
    }

    /**
     * @return number of cells read from _scrub table
     */
    private int scrubSomeCells(final TransactionManager txManager, int maxCellsToScrub, long maxScrubTimestamp) {
        SortedMap<Long, Multimap<String, Cell>> scrubTimestampToTableNameToCell =
            scrubberStore.getCellsToScrub(maxCellsToScrub, maxScrubTimestamp);


        // Don't call expensive toString() if trace logging is off
        if (log.isTraceEnabled()) {
            log.trace("Attempting to scrub cells: " + scrubTimestampToTableNameToCell);
        }

        if (log.isInfoEnabled()) {
            Set<String> tables = Sets.newHashSet();
            for (Multimap<String, Cell> v : scrubTimestampToTableNameToCell.values()) {
                tables.addAll(v.keySet());
            }
            log.info("Attempting to scrub " + scrubTimestampToTableNameToCell.size() + " cells from tables " + tables);
        }

        if (scrubTimestampToTableNameToCell.size() == 0) {
            return 0; // No cells left to scrub
        }

        Multimap<Long, Cell> toRemoveFromScrubQueue = HashMultimap.create();

        int numCellsReadFromScrubTable = 0;
        for (Map.Entry<Long, Multimap<String, Cell>> entry : scrubTimestampToTableNameToCell.entrySet()) {
            final long scrubTimestamp = entry.getKey();
            final Multimap<String, Cell> tableNameToCell = entry.getValue();

            numCellsReadFromScrubTable += tableNameToCell.size();

            long commitTimestamp = getCommitTimestampRollBackIfNecessary(scrubTimestamp, tableNameToCell);
            if (commitTimestamp >= maxScrubTimestamp) {
                // We cannot scrub this yet because not all transactions can read this value.
                continue;
            } else if (commitTimestamp != TransactionConstants.FAILED_COMMIT_TS) {
                // This is CRITICAL; don't scrub if the hard delete transaction didn't actually finish
                // (we still remove it from the _scrub table with the call to markCellsAsScrubbed though),
                // or else we could cause permanent data loss if the hard delete transaction failed after
                // queuing cells to scrub but before successfully committing
                List<Future<Void>> scrubFutures = Lists.newArrayList();
                for (final List<Entry<String, Cell>> batch : Iterables.partition(tableNameToCell.entries(), batchSizeSupplier.get())) {
                    final Multimap<String, Cell> batchMultimap = HashMultimap.create();
                    for (Entry<String, Cell> e : batch) {
                        batchMultimap.put(e.getKey(), e.getValue());
                    }
                    scrubFutures.add(exec.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            scrubCells(txManager, batchMultimap,
                                    scrubTimestamp,
                                    aggressiveScrub ? TransactionType.AGGRESSIVE_HARD_DELETE : TransactionType.HARD_DELETE);
                            return null;
                        }}));
                }
                for (Future<Void> future : scrubFutures) {
                    Futures.getUnchecked(future);
                }
            }
            toRemoveFromScrubQueue.putAll(scrubTimestamp, tableNameToCell.values());
        }

        Multimap<Cell, Long> cellToScrubTimestamp = HashMultimap.create();
        scrubberStore.markCellsAsScrubbed(Multimaps.invertFrom(toRemoveFromScrubQueue, cellToScrubTimestamp), batchSizeSupplier.get());

        if (log.isTraceEnabled()) {
            log.trace("Finished scrubbing cells: " + scrubTimestampToTableNameToCell);
        }

        if (log.isInfoEnabled()) {
            Set<String> tables = Sets.newHashSet();
            for (Multimap<String, Cell> v : scrubTimestampToTableNameToCell.values()) {
                tables.addAll(v.keySet());
            }
            log.info("Finished scrubbing " + scrubTimestampToTableNameToCell.size() + " cells from tables " + tables);
        }

        return numCellsReadFromScrubTable;
    }

    private void scrubCells(TransactionManager txManager,
                            Multimap<String, Cell> tableNameToCells,
                            long scrubTimestamp,
                            Transaction.TransactionType transactionType) {
        for (Entry<String, Collection<Cell>> entry : tableNameToCells.asMap().entrySet()) {
            String tableName = entry.getKey();
            if (log.isInfoEnabled()) {
                log.info("Attempting to immediately scrub " + entry.getValue().size() + " cells from table " + tableName);
            }
            for (List<Cell> cells : Iterables.partition(entry.getValue(), batchSizeSupplier.get())) {
                Multimap<Cell, Long> timestampsToDelete = HashMultimap.create(
                        keyValueService.getAllTimestamps(tableName, ImmutableSet.copyOf(cells), scrubTimestamp));
                for (Cell cell : ImmutableList.copyOf(timestampsToDelete.keySet())) {
                    // Don't scrub garbage collection sentinels
                    timestampsToDelete.remove(cell, Value.INVALID_VALUE_TIMESTAMP);
                }
                // If transactionType == TransactionType.AGGRESSIVE_HARD_DELETE this might
                // force other transactions to abort or retry
                deleteCellsAtTimestamps(txManager, tableName, timestampsToDelete, transactionType);
            }
            if (log.isInfoEnabled()) {
                log.info("Immediately scrubbed " + entry.getValue().size() + " cells from table " + tableName);
            }
        }
    }

    private void deleteCellsAtTimestamps(TransactionManager txManager,
                                         String tableName,
                                         Multimap<Cell, Long> cellToTimestamp,
                                         Transaction.TransactionType transactionType) {
        if (!cellToTimestamp.isEmpty()) {
            for (Follower follower : followers) {
                follower.run(txManager, tableName, cellToTimestamp.keySet(), transactionType);
            }
            keyValueService.addGarbageCollectionSentinelValues(
                    tableName,
                    cellToTimestamp.keySet());
            keyValueService.delete(tableName, cellToTimestamp);
        }
    }

    public long getUnreadableTimestamp() {
        return unreadableTimestampSupplier.get();
    }

    public void start(TransactionManager txManager) {
        launchBackgroundScrubTask(txManager);
    }

    public void shutdown() {
        service.shutdownNow();
    }
}
