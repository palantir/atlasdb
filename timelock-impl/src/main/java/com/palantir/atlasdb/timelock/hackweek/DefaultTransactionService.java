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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.UnsignedBytes;
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

@NotThreadSafe
public final class DefaultTransactionService implements JamesTransactionService {
    private static CheckReadConflictsResponse ABORTED = CheckReadConflictsResponse.newBuilder().build();
    private final Map<Long, Long> commitTimestamps = new HashMap<>();
    private final NavigableMap<Long, List<TableCell>> writesByTransaction = new TreeMap<>();
    private final RangeSet<Long> runningTransactions = TreeRangeSet.create();
    private final Map<Table, NavigableMap<Cell, Long>> lastUpdated = new HashMap<>();

    private long timestamp = 0;

    private void cleanUp() {
        Iterator<Map.Entry<Long, List<TableCell>>> iterator = writesByTransaction.entrySet().iterator();
        Range<Long> liveRange = runningTransactions.span();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<TableCell>> current = iterator.next();
            if (liveRange.contains(current.getKey())) {
                return;
            }
            Long commitTimestamp = commitTimestamps.get(current.getKey());
            if (commitTimestamp != null && liveRange.contains(commitTimestamp)) {
                return;
            }
            iterator.remove();
            commitTimestamps.remove(current.getKey());
            current.getValue().forEach(write ->
                    getLastUpdated(write.getTable()).remove(write.getCell(), current.getKey()));
        }
    }

    @Override
    public ImmutableTimestamp getImmutableTimestamp() {
        if (runningTransactions.isEmpty()) {
            return ImmutableTimestamp.newBuilder().setTimestamp(timestamp).build();
        } else {
            return ImmutableTimestamp.newBuilder().setTimestamp(runningTransactions.span().lowerEndpoint()).build();
        }
    }

    @Override
    public Timestamp getFreshTimestamp() {
        return Timestamp.newBuilder().setTimestamp(timestamp++).build();
    }

    @Override
    public TimestampRange startTransactions(long numberOfTransactions) {
        long start = timestamp;
        timestamp += numberOfTransactions;
        runningTransactions.add(Range.closedOpen(start, timestamp));
        return TimestampRange.newBuilder()
                .setLower(start)
                .setUpper(timestamp)
                .setImmutable(getImmutableTimestamp().getTimestamp())
                .build();
    }

    @Override
    public CommitWritesResponse commitWrites(long startTimestamp, List<TableCell> writes) {
        if (!runningTransactions.contains(startTimestamp)) {
            return CommitWritesResponse.newBuilder().setContinue(false).build();
        }

        for (TableCell write : writes) {
            if (isConflicting(startTimestamp, write.getTable(), write.getCell())) {
                runningTransactions.remove(Range.singleton(startTimestamp));
                return CommitWritesResponse.newBuilder().setContinue(false).build();
            }
        }

        writesByTransaction.put(startTimestamp, writes);
        for (TableCell write : writes) {
            getLastUpdated(write.getTable()).put(write.getCell(), startTimestamp);
        }
        return CommitWritesResponse.newBuilder().setContinue(true).build();
    }

    @Override
    public CheckReadConflictsResponse checkReadConflicts(
            long startTimestamp, List<TableCell> reads, List<TableRange> ranges) {
        if (!runningTransactions.contains(startTimestamp)) {
            return ABORTED;
        }

        for (TableCell read : reads) {
            if (isConflicting(startTimestamp, read.getTable(), read.getCell())) {
                runningTransactions.remove(Range.singleton(startTimestamp));
                return ABORTED;
            }
        }

        for (TableRange read : ranges) {
            NavigableMap<Cell, Long> updated = getLastUpdated(read.getTable());
            updated = updated.subMap(read.getStart(), true, read.getEnd(), false);
            if (read.getHasColumnFilter()) {
                Set<ByteString> columnFilter = new HashSet<>(read.getColumnFilterList());
                updated = Maps.filterKeys(updated, c -> columnFilter.contains(c.getColumn()));
            }
            updated = Maps.filterValues(updated, timestamp -> isConflicting(startTimestamp, timestamp));
            if (!updated.isEmpty()) {
                runningTransactions.remove(Range.singleton(startTimestamp));
                return ABORTED;
            }
        }
        long commitTimestamp = timestamp++;
        commitTimestamps.put(startTimestamp, commitTimestamp);
        cleanUp();
        return CheckReadConflictsResponse.newBuilder().setCommitTimestamp(commitTimestamp).build();
    }

    @Override
    public void unlock(List<Long> startTimestamps) {
        startTimestamps.stream().map(Range::singleton).forEach(runningTransactions::remove);
    }

    private boolean isConflicting(long startTimestamp, long otherStartTimestamp) {
        Long otherCommitTimestamp = commitTimestamps.get(otherStartTimestamp);
        if (otherCommitTimestamp == null) {
            return startTimestamp < otherStartTimestamp;
        } else {
            return startTimestamp < otherCommitTimestamp;
        }
    }

    private boolean isConflicting(long startTimestamp, Table table, Cell cell) {
        Long otherWriteStartTimestamp = getLastUpdated(table).get(cell);
        if (otherWriteStartTimestamp != null) {
            return isConflicting(startTimestamp, otherWriteStartTimestamp);
        }
        return false;
    }

    private NavigableMap<Cell, Long> getLastUpdated(Table table) {
        return lastUpdated.computeIfAbsent(table, k -> new TreeMap<>(
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
