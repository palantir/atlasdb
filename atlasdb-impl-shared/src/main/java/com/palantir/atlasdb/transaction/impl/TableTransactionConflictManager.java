/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.concurrent.GuardedBy;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * An advisory implementation of
 * <a href="https://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2009/Papers/p492-fekete.pdf">
 *     Making Snapshot Isolation Serializable</a>.
 * Hypothesis - read write sets are non-conflicting enough at the level of a whole table that we can avoid
 * significant numbers of conflicts by performing more elaborate conflict checking at this level.
 *
 * Idea here is to provide this as an advisory thing which can be run on all AtlasDB nodes in a high performance
 * fashion in order to see whether making this kind of change part of the AtlasDB commit protocol can result
 * in significant improvements.
 *
 * Example of hypothesis from internal use case:
 * Imagine I have a data table, with values that are frequently overwritten. A cleanup task deletes the values
 * if they have not been written in the last 30 days. This cleanup task will conflict with any values that are
 * being frequently overwritten, but this does not affect serializability.
 */
public final class TableTransactionConflictManager {
    private static final int OVERHEADS = 40;
    private static final int MAX_MEMORY = 10_000_000;
    private static final Logger log = LoggerFactory.getLogger(TableTransactionConflictManager.class);

    @GuardedBy("this")
    private final NavigableMap<Long, TransactionState> runningTransactions = new TreeMap<>();
    @GuardedBy("this")
    private final SetMultimap<TableReference, Long> lastUpdated = HashMultimap.create();
    @GuardedBy("this")
    private final SetMultimap<TableReference, Long> reads = HashMultimap.create();

    enum Status {
        NO_READ_WRITE_CONFLICTS, CANNOT_ADVISE, ERROR;
    }

    @Value.Immutable
    interface TransactionState {
        TransactionState NEW = ImmutableTransactionState.builder()
                .inConflict(false)
                .outConflict(false)
                .build();

        Optional<Long> commitTimestamp();
        Set<TableReference> writeSet();
        Set<TableReference> readSet();
        boolean inConflict();
        boolean outConflict();

        TransactionState withCommitTimestamp(Optional<Long> commitTimestamp);
        TransactionState withWriteSet(Iterable<? extends TableReference> writeSet);
        TransactionState withReadSet(Iterable<? extends TableReference> readSet);
        TransactionState withInConflict(boolean inConflict);
        TransactionState withOutConflict(boolean outConflict);
    }

    private long bytesUsed() {
        return runningTransactions.values().stream()
                .mapToInt(txn -> OVERHEADS + 30 * (txn.readSet().size() + txn.writeSet().size()))
                .asLongStream()
                .sum();
    }

    private Optional<Long> getFirstRunningTransaction() {
        return Maps.filterValues(runningTransactions, state -> !state.commitTimestamp().isPresent())
                .keySet().stream().findFirst();
    }

    private void cleanUp() {
        Optional<Long> maybeMinRunning = getFirstRunningTransaction();
        if (!maybeMinRunning.isPresent() || bytesUsed() >= MAX_MEMORY) {
            runningTransactions.clear();
            lastUpdated.clear();
            reads.clear();
            return;
        }
        long minRunning = maybeMinRunning.get();
        Iterator<Map.Entry<Long, TransactionState>> iterator = runningTransactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, TransactionState> current = iterator.next();
            Long transactionTimestamp = current.getKey();
            if (minRunning < transactionTimestamp) {
                return;
            }
            TransactionState state = current.getValue();
            if (state.commitTimestamp().map(commitTs -> minRunning < commitTs).orElse(true)) {
                return;
            }
            state.readSet().forEach(table -> reads.remove(table, transactionTimestamp));
            state.writeSet().forEach(table -> lastUpdated.remove(table, transactionTimestamp));
            // cleanup last updated
            iterator.remove();
        }
    }

    interface RunningTransaction {
        Status commit(long commitTimestamp, Set<TableReference> tablesRead, Set<TableReference> tablesWritten);
        void abort();
        void abortIfNotComplete();
    }

    public enum NoOpRunningTransaction implements RunningTransaction {
        INSTANCE;

        @Override
        public Status commit(long commitTimestamp, Set<TableReference> tablesRead,
                Set<TableReference> tablesWritten) {
            log.info("Did not expect commit from a no-op transaction");
            return Status.CANNOT_ADVISE;
        }

        @Override
        public void abort() {}

        @Override
        public void abortIfNotComplete() {}
    }

    public synchronized RunningTransaction startTransaction(long startTimestamp) {
        runningTransactions.put(startTimestamp, TransactionState.NEW);
        return new RunningTransaction() {
            @Override
            public Status commit(
                    long commitTimestamp, Set<TableReference> tablesRead, Set<TableReference> tablesWritten) {
                return commitTransaction(startTimestamp, commitTimestamp, tablesRead, tablesWritten);
            }

            @Override
            public void abort() {
                abortTransaction(startTimestamp);
            }

            @Override
            public void abortIfNotComplete() {
                abortTransactionIfNotComplete(startTimestamp);
            }
        };
    }

    private synchronized void abortTransaction(long startTimestamp) {
        runningTransactions.remove(startTimestamp);
        cleanUp();
    }

    private synchronized void abortTransactionIfNotComplete(long startTimestamp) {
        TransactionState state = runningTransactions.get(startTimestamp);
        if (state != null && !state.commitTimestamp().isPresent()) {
            runningTransactions.remove(startTimestamp);
        }
    }

    private synchronized Status commitTransaction(
            long startTimestamp,
            long commitTimestamp,
            Set<TableReference> tablesRead,
            Set<TableReference> tablesWritten) {
        try {
            if (!runningTransactions.containsKey(startTimestamp)) {
                cleanUp();
                return Status.ERROR;
            }
            for (TableReference write : tablesWritten) {
                registerWriteConflicts(startTimestamp, write);
            }

            for (TableReference read : tablesRead) {
                if (isConflictingRead(startTimestamp, read)) {
                    runningTransactions.remove(startTimestamp);
                    cleanUp();
                    return Status.CANNOT_ADVISE;
                }
            }

            TransactionState state = runningTransactions.get(startTimestamp);
            if (state.inConflict() && state.outConflict()) {
                runningTransactions.remove(startTimestamp);
                cleanUp();
                return Status.CANNOT_ADVISE;
            }

            runningTransactions.put(startTimestamp, state.withWriteSet(tablesWritten)
                    .withReadSet(tablesRead)
                    .withCommitTimestamp(Optional.of(commitTimestamp)));
            tablesWritten.forEach(write -> lastUpdated.put(write, startTimestamp));
            tablesRead.forEach(read -> reads.put(read, startTimestamp));
            cleanUp();
            return Status.NO_READ_WRITE_CONFLICTS;
        } catch (RuntimeException e) {
            log.warn("Error thrown while processing conflicts", e);
            return Status.ERROR;
        }
    }

    private boolean isConflictingRead(long startTimestamp, TableReference table) {
        TransactionState ourState = runningTransactions.get(startTimestamp);
        Set<Long> otherWriteStartTimestamps = lastUpdated.get(table);
        for (Long otherStartTimestamp : otherWriteStartTimestamps) {
            if (otherStartTimestamp == startTimestamp) {
                continue;
            }
            TransactionState otherState = runningTransactions.get(otherStartTimestamp);
            if (otherState.commitTimestamp().get() > startTimestamp && otherState.outConflict()) {
                return true;
            } else {
                runningTransactions.put(otherStartTimestamp, otherState.withInConflict(true));
                ourState = ourState.withOutConflict(true);
                runningTransactions.put(startTimestamp, ourState);
            }
        }
        return false;
    }

    private void registerWriteConflicts(long startTimestamp, TableReference table) {
        Set<Long> readStartTimestamps = reads.get(table);
        for (Long readStartTimestamp : readStartTimestamps) {
            if (readStartTimestamp == startTimestamp) {
                continue;
            }
            TransactionState readState = runningTransactions.get(readStartTimestamp);
            if (readState.commitTimestamp().get() > startTimestamp && readState.inConflict()) {
                continue;
            } else {
                runningTransactions.put(readStartTimestamp, readState.withOutConflict(true));
                runningTransactions.compute(startTimestamp, (key, value) -> value.withInConflict(true));
            }
        }
    }
}
