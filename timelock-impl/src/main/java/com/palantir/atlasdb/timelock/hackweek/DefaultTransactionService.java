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

package com.palantir.atlasdb.timelock.hackweek;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.protos.generated.TransactionService.Cell;
import com.palantir.atlasdb.protos.generated.TransactionService.CheckReadConflictsResponse;
import com.palantir.atlasdb.protos.generated.TransactionService.CommitWritesResponse;
import com.palantir.atlasdb.protos.generated.TransactionService.ImmutableTimestamp;
import com.palantir.atlasdb.protos.generated.TransactionService.Table;
import com.palantir.atlasdb.protos.generated.TransactionService.TableCell;
import com.palantir.atlasdb.protos.generated.TransactionService.TableRange;
import com.palantir.atlasdb.protos.generated.TransactionService.Timestamp;
import com.palantir.atlasdb.protos.generated.TransactionService.TimestampRange;
import com.palantir.common.concurrent.PTExecutors;

@NotThreadSafe
public final class DefaultTransactionService implements JamesTransactionService {
    private static final ScheduledExecutorService schedulingExecutor = PTExecutors.newSingleThreadScheduledExecutor();
    private static CheckReadConflictsResponse ABORTED = CheckReadConflictsResponse.newBuilder().build();
    private final NavigableMap<Long, RunningTransactionState> runningTransactions = new TreeMap<>();
    private final Map<Table, NavigableMap<Cell, Set<Long>>> lastUpdated = new HashMap<>();
    private final Map<Table, NavigableMap<Cell, Set<Long>>> reads = new HashMap<>();
    private final Map<Long, SettableFuture<?>> notifications = new HashMap<>();

    private final NavigableMap<Long, List<TableCell>> writes = new TreeMap<>();

    private static final RunningTransactionState EMPTY = new RunningTransactionState();

    private static class RunningTransactionState {
        boolean doneCommitting = false;
        Long commitTimestamp = null;
        List<TableCell> writes = Collections.emptyList();
        List<TableCell> reads = Collections.emptyList();
        boolean inConflict = false;
        boolean outConflict = false;
    }

    private long timestamp = 0;

    private OptionalLong getFirstRunningTransaction() {
        return runningTransactions.entrySet().stream()
                .filter(entry -> entry.getValue().commitTimestamp == null)
                .mapToLong(Map.Entry::getKey)
                .findFirst();
    }

    private void cleanUp() {
        OptionalLong maybeMinRunning = getFirstRunningTransaction();
        if (!maybeMinRunning.isPresent()) {
            runningTransactions.clear();
            lastUpdated.clear();
            return;
        }
        long minRunning = maybeMinRunning.getAsLong();

        Iterator<Map.Entry<Long, RunningTransactionState>> iterator = runningTransactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RunningTransactionState> current = iterator.next();
            if (minRunning < current.getKey()) {
                return;
            }
            Long commitTimestamp = current.getValue().commitTimestamp;
            if (commitTimestamp == null || minRunning < commitTimestamp) {
                return;
            }
            clearWrites(current.getKey());
            current.getValue().reads.forEach(write -> getReads(write.getTable()).compute(write.getCell(), (c, timestamps) -> {
                timestamps.remove(current.getKey());
                if (timestamps.isEmpty()) {
                    return null;
                }
                return timestamps;
            }));
            iterator.remove();
        }
    }

    private void clearWrites(Long txnTimestamp) {
        runningTransactions.get(txnTimestamp).writes.forEach(
                write -> getLastUpdated(write.getTable()).compute(write.getCell(), (c, timestamps) -> {
            timestamps.remove(txnTimestamp);
            if (timestamps.isEmpty()) {
                return null;
            }
            return timestamps;
        }));
    }

    @Override
    public ImmutableTimestamp getImmutableTimestamp() {
        OptionalLong firstRunning = getFirstRunningTransaction();
        if (!firstRunning.isPresent()) {
            return ImmutableTimestamp.newBuilder().setTimestamp(timestamp++).build();
        } else {
            return ImmutableTimestamp.newBuilder().setTimestamp(firstRunning.getAsLong()).build();
        }
    }

    @Override
    public Timestamp getFreshTimestamp() {
        return Timestamp.newBuilder().setTimestamp(timestamp++).build();
    }

    @Override
    public TimestampRange startTransactions(long cachedUpTo, long numberOfTransactions) {
        long start = timestamp;
        timestamp += numberOfTransactions;
        LongStream.range(start, timestamp).forEach(timestamp -> runningTransactions.put(timestamp, new RunningTransactionState()));
        return TimestampRange.newBuilder()
                .setLower(start)
                .setUpper(timestamp)
                .setImmutable(getImmutableTimestamp().getTimestamp())
                .addAllCacheUpdates(() -> writes.tailMap(cachedUpTo).values().stream().flatMap(Collection::stream).iterator())
                .build();
    }

    @Override
    public CommitWritesResponse commitWrites(long startTimestamp, List<TableCell> writes) {
        if (!runningTransactions.containsKey(startTimestamp)) {
            return CommitWritesResponse.newBuilder().setContinue(false).build();
        }

        for (TableCell write : writes) {
            if (isConflicting(startTimestamp, write.getTable(), write.getCell())) {
                runningTransactions.remove(startTimestamp);
                return CommitWritesResponse.newBuilder().setContinue(false).build();
            }
        }

        runningTransactions.get(startTimestamp).writes = writes;
        for (TableCell write : writes) {
            getLastUpdated(write.getTable()).compute(write.getCell(), (cell, current) -> {
                Set<Long> result = current == null ? new HashSet<>() : current;
                result.add(startTimestamp);
                return result;
            });
        }
        return CommitWritesResponse.newBuilder().setContinue(true).build();
    }

    @Override
    public CheckReadConflictsResponse checkReadConflicts(
            long startTimestamp, List<TableCell> reads, List<TableRange> ranges) {
        if (!runningTransactions.containsKey(startTimestamp)) {
            return ABORTED;
        }

        for (TableCell read : reads) {
            if (isConflictingRead(startTimestamp, read.getTable(), read.getCell())) {
                clearWrites(startTimestamp);
                runningTransactions.remove(startTimestamp);
                return ABORTED;
            }
        }

        for (TableRange read : ranges) {
            NavigableMap<Cell, Set<Long>> updated = getLastUpdated(read.getTable());
            updated = updated.subMap(read.getStart(), true, read.getEnd(), false);
            if (read.getHasColumnFilter()) {
                Set<ByteString> columnFilter = new HashSet<>(read.getColumnFilterList());
                updated = Maps.filterKeys(updated, c -> columnFilter.contains(c.getColumn()));
            }
            updated = Maps.filterKeys(updated, cell -> isConflictingRead(startTimestamp, read.getTable(), cell));
            if (!updated.isEmpty()) {
                clearWrites(startTimestamp);
                runningTransactions.remove(startTimestamp);
                return ABORTED;
            }
        }

        RunningTransactionState state = runningTransactions.get(startTimestamp);

        if (state.inConflict && state.outConflict) {
            clearWrites(startTimestamp);
            runningTransactions.remove(startTimestamp);
            return ABORTED;
        }

        state.reads = reads;

        reads.forEach(tableCell -> getReads(tableCell.getTable()).compute(tableCell.getCell(), (cell, set) -> {
            Set<Long> result = set == null ? new HashSet<>() : set;
            result.add(startTimestamp);
            return result;
        }));

        long commitTimestamp = timestamp++;
        state.commitTimestamp = commitTimestamp;
        writes.put(commitTimestamp, state.writes);
        cleanUp();
        return CheckReadConflictsResponse.newBuilder().setCommitTimestamp(commitTimestamp).build();
    }

    @Override
    public ListenableFuture<?> waitForCommit(List<Long> startTimestamps) {
        List<ListenableFuture<?>> futures = startTimestamps.stream()
                .filter(runningTransactions::containsKey)
                .filter(ts -> !runningTransactions.get(ts).doneCommitting)
                .map(ts -> {
                    SettableFuture<?> future = SettableFuture.create();
                    notifications.put(ts, future);
                    return future;
                })
                .collect(toList());
        return Futures.withTimeout(Futures.allAsList(futures), 10, TimeUnit.SECONDS, schedulingExecutor);
    }

    @Override
    public void unlock(List<Long> startTimestamps) {
        startTimestamps.forEach(ts -> Optional.ofNullable(runningTransactions.get(ts)).ifPresent(state -> state.doneCommitting = true));
        startTimestamps.forEach(ts -> Optional.ofNullable(notifications.get(ts)).ifPresent(x -> x.set(null)));
        cleanUp();
    }

    private boolean isConflicting(long startTimestamp, long otherStartTimestamp) {
        if (startTimestamp == otherStartTimestamp) {
            return false;
        }
        Long otherCommitTimestamp = runningTransactions.getOrDefault(otherStartTimestamp, EMPTY).commitTimestamp;
        if (otherCommitTimestamp == null) {
            return true;
        } else {
            return startTimestamp < otherCommitTimestamp;
        }
    }

    private boolean isConflictingRead(long startTimestamp, Table table, Cell cell) {
        RunningTransactionState ourState = runningTransactions.get(startTimestamp);
        Set<Long> otherWriteStartTimestamps = getLastUpdated(table).getOrDefault(cell, Collections.emptySet());
        for (Long otherStartTimestamp : otherWriteStartTimestamps) {
            if (otherStartTimestamp == startTimestamp) {
                continue;
            }
            RunningTransactionState otherState = runningTransactions.get(otherStartTimestamp);
            if (otherState.commitTimestamp != null && otherState.commitTimestamp > startTimestamp
                    && otherState.outConflict) {
                return true;
            } else {
                otherState.inConflict = true;
                ourState.outConflict = true;
            }
        }
        return false;
    }

    private boolean isConflicting(long startTimestamp, Table table, Cell cell) {
        Set<Long> readStartTimestamps = getReads(table).getOrDefault(cell, Collections.emptySet());
        for (Long readStartTimestamp : readStartTimestamps) {
            if (readStartTimestamp == startTimestamp) {
                continue;
            }
            RunningTransactionState readState = runningTransactions.get(readStartTimestamp);
            if (readState.commitTimestamp > startTimestamp && readState.inConflict) {
                return true;
            } else {
                readState.outConflict = true;
                runningTransactions.get(startTimestamp).inConflict = true;
            }
        }

        Set<Long> otherWriteStartTimestamps = getLastUpdated(table).getOrDefault(cell, Collections.emptySet());
        return otherWriteStartTimestamps.stream()
                .anyMatch(otherWriteStartTimestamp -> isConflicting(startTimestamp, otherWriteStartTimestamp));
    }

    private NavigableMap<Cell, Set<Long>> getLastUpdated(Table table) {
        return lastUpdated.computeIfAbsent(table, k -> new TreeMap<>(
                Comparator.comparing(Cell::getRow, ByteStringComparator.INSTANCE)
                        .thenComparing(Cell::getColumn, ByteStringComparator.INSTANCE)));
    }

    private NavigableMap<Cell, Set<Long>> getReads(Table table) {
        return reads.computeIfAbsent(table, k -> new TreeMap<>(
                Comparator.comparing(Cell::getRow, ByteStringComparator.INSTANCE)
                        .thenComparing(Cell::getColumn, ByteStringComparator.INSTANCE)));
    }

    // can avoid this by using similar tricks as UnsignedBytes; this is for simplicity
    // zero length comes last
    private enum ByteStringComparator implements Comparator<ByteString> {
        INSTANCE;

        @Override
        public int compare(ByteString o1, ByteString o2) {
            if (o1.size() == 0) {
                return o2.size() == 0 ? 0 : 1;
            } else if (o2.size() == 0) {
                return -1;
            }
            return UnsignedBytes.lexicographicalComparator().compare(o1.toByteArray(), o2.toByteArray());
        }
    }
}
