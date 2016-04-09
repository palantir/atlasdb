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
package com.palantir.atlasdb.table.common;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.annotation.Inclusive;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.lock.LockRefreshToken;

public class TableTasks {
    private static final Logger log = LoggerFactory.getLogger(TableTasks.class);

    public static class CopyStats {
        private final AtomicLong rowsCopied;
        private final AtomicLong cellsCopied;

        public CopyStats(AtomicLong rowsCopied,
                         AtomicLong cellsCopied) {
            this.rowsCopied = rowsCopied;
            this.cellsCopied = cellsCopied;
        }
    }

    private static class PartialCopyStats {
        private long rowsCopied = 0;
        private long cellsCopied = 0;
    }

    private static interface CopyTask {
        PartialCopyStats call(RangeRequest request, MutableRange range) throws InterruptedException;
    }

    public static void copy(final TransactionManager txManager,
                            ExecutorService exec,
                            final TableReference srcTable,
                            final TableReference dstTable,
                            int batchSize,
                            int threadCount,
                            @Output CopyStats stats) throws InterruptedException {
        copyExternal(exec, srcTable, dstTable, batchSize, threadCount, stats, new CopyTask() {
            @Override
            public PartialCopyStats call(final RangeRequest request, final MutableRange range) {
                return txManager.runTaskWithRetry(new RuntimeTransactionTask<PartialCopyStats>() {
                    @Override
                    public PartialCopyStats execute(final Transaction t) {
                        return copyInternal(t, srcTable, dstTable, request, range);
                    }
                });
            }
        });
    }

    public static void copy(final TransactionManager txManager,
                            ExecutorService exec,
                            final Iterable<LockRefreshToken> lockTokens,
                            final TableReference srcTable,
                            final TableReference dstTable,
                            int batchSize,
                            int threadCount,
                            @Output CopyStats stats) throws InterruptedException {
        copyExternal(exec, srcTable, dstTable, batchSize, threadCount, stats, new CopyTask() {
            @Override
            public PartialCopyStats call(final RangeRequest request, final MutableRange range) throws InterruptedException {
                return txManager.runTaskWithRetry(
                        new RuntimeTransactionTask<PartialCopyStats>() {
                            @Override
                            public PartialCopyStats execute(Transaction t) {
                                return copyInternal(t, srcTable, dstTable, request, range);
                            }
                        });
            }
        });
    }

    public static void copyExternal(ExecutorService exec,
                                    final TableReference srcTable,
                                    final TableReference dstTable,
                                    int batchSize,
                                    int threadCount,
                                    final CopyStats stats,
                                    final CopyTask task) throws InterruptedException {
        BlockingWorkerPool pool = new BlockingWorkerPool(exec, threadCount);
        for (final MutableRange range : getRanges(threadCount, batchSize)) {
            pool.submitTask(new Runnable() {
                @Override
                public void run() {
                    do {
                        final RangeRequest request = range.getRangeRequest();
                        try {
                            long startTime = System.currentTimeMillis();
                            PartialCopyStats partialStats = task.call(request, range);
                            stats.rowsCopied.addAndGet(partialStats.rowsCopied);
                            stats.cellsCopied.addAndGet(partialStats.cellsCopied);
                            log.info("Copied {} rows, {} cells from {} to {} in {} ms.",
                                    partialStats.rowsCopied,
                                    partialStats.cellsCopied,
                                    srcTable,
                                    dstTable,
                                    System.currentTimeMillis() - startTime);
                        } catch (InterruptedException e) {
                            throw Throwables.rewrapAndThrowUncheckedException(e);
                        }
                    } while (!range.isComplete());
                }
            });
        }
        pool.waitForSubmittedTasks();
    }

    private static PartialCopyStats copyInternal(final Transaction t,
                                                 final TableReference srcTable,
                                                 final TableReference dstTable,
                                                 RangeRequest request,
                                                 final MutableRange range) {
        final PartialCopyStats stats = new PartialCopyStats();
        boolean isEmpty = t.getRange(srcTable, request).batchAccept(range.getBatchSize(),
                new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
            @Override
            public boolean visit(List<RowResult<byte[]>> batch) {
                Map<Cell, byte[]> entries = Maps.newHashMapWithExpectedSize(batch.size());
                for (RowResult<byte[]> result : batch) {
                    for (Entry<Cell, byte[]> entry : result.getCells()) {
                        entries.put(entry.getKey(), entry.getValue());
                    }
                }
                if (batch.size() < range.getBatchSize()) {
                    range.setStartRow(null);
                } else {
                    byte[] lastRow = batch.get(batch.size() - 1).getRowName();
                    range.setStartRow(RangeRequests.nextLexicographicName(lastRow));
                }
                t.put(dstTable, entries);
                stats.rowsCopied = batch.size();
                stats.cellsCopied = entries.size();
                return false;
            }
        });
        if (isEmpty) {
            range.setStartRow(null);
        }
        return stats;
    }

    public static long estimateSize(Transaction t,
                                    TableReference table,
                                    final int batchSize,
                                    final Function<byte[], byte[]> uniformizer) {
        final AtomicLong estimate = new AtomicLong();
        t.getRange(table, RangeRequest.all()).batchAccept(batchSize,
                new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
            @Override
            public boolean visit(List<RowResult<byte[]>> batch) {
                if (batch.size() < batchSize) {
                    estimate.set(batch.size());
                } else {
                    byte[] row = uniformizer.apply(batch.get(batchSize - 1).getRowName());
                    estimate.set(BigInteger.valueOf(2)
                        .pow(row.length * 8)
                        .multiply(BigInteger.valueOf(batchSize))
                        .divide(new BigInteger(1, row))
                        .longValue());
                }
                return false;
            }
        });
        return estimate.get();
    }

    public static class DiffStats {
        private final AtomicLong rowsOnlyInSource;
        private final AtomicLong rowsPartiallyInCommon;
        private final AtomicLong rowsCompletelyInCommon;
        private final AtomicLong rowsVisited;
        private final AtomicLong cellsOnlyInSource;
        private final AtomicLong cellsInCommon;

        public DiffStats(AtomicLong rowsOnlyInSource,
                         AtomicLong rowsPartiallyInCommon,
                         AtomicLong rowsCompletelyInCommon,
                         AtomicLong rowsVisited,
                         AtomicLong cellsOnlyInSource,
                         AtomicLong cellsInCommon) {
            this.rowsOnlyInSource = rowsOnlyInSource;
            this.rowsPartiallyInCommon = rowsPartiallyInCommon;
            this.rowsCompletelyInCommon = rowsCompletelyInCommon;
            this.rowsVisited = rowsVisited;
            this.cellsOnlyInSource = cellsOnlyInSource;
            this.cellsInCommon = cellsInCommon;
        }
    }

    public static class PartialDiffStats {
        private long rowsOnlyInSource;
        private long rowsPartiallyInCommon;
        private long rowsCompletelyInCommon;
        private long rowsVisited;
        private long cellsOnlyInSource;
        private long cellsInCommon;
    }

    private static interface DiffTask {
        PartialDiffStats call(RangeRequest request, MutableRange range, DiffStrategy strategy) throws InterruptedException;
    }

    public static interface DiffVisitor {
        void visit(Transaction t, Iterator<Cell> partialDiff);
    }

    public static void diff(final TransactionManager txManager,
                            ExecutorService exec,
                            final TableReference plusTable,
                            final TableReference minusTable,
                            int batchSize,
                            int threadCount,
                            @Output DiffStats stats,
                            final DiffVisitor visitor) throws InterruptedException {
        diffExternal(txManager, exec, plusTable, minusTable, batchSize, threadCount, stats, new DiffTask() {
            @Override
            public PartialDiffStats call(final RangeRequest request, final MutableRange range, final DiffStrategy strategy)
                    throws InterruptedException {
                return txManager.runTaskWithRetry(new RuntimeTransactionTask<PartialDiffStats>() {
                    @Override
                    public PartialDiffStats execute(final Transaction t) {
                        return diffInternal(t, plusTable, minusTable, request, range, strategy, visitor);
                    }
                });
            }
        });
    }

    public static void diff(final TransactionManager txManager,
                            ExecutorService exec,
                            final Iterable<LockRefreshToken> lockTokens,
                            final TableReference plusTable,
                            final TableReference minusTable,
                            final int batchSize,
                            int threadCount,
                            @Output DiffStats stats,
                            final DiffVisitor visitor) throws InterruptedException {
        diffExternal(txManager, exec, plusTable, minusTable, batchSize, threadCount, stats, new DiffTask() {
            @Override
            public PartialDiffStats call(final RangeRequest request, final MutableRange range, final DiffStrategy strategy)
                    throws InterruptedException {
                return txManager.runTaskWithRetry(
                        new RuntimeTransactionTask<PartialDiffStats>() {
                            @Override
                            public PartialDiffStats execute(Transaction t) {
                                return diffInternal(t, plusTable, minusTable, request, range, strategy, visitor);
                            }

                            @Override
                            public String toString() {
                                return "diff(" + request + ',' + strategy + ')';
                            }
                        });
            }
        });
    }

    private static void diffExternal(final TransactionManager txManager,
                                     ExecutorService exec,
                                     final TableReference plusTable,
                                     final TableReference minusTable,
                                     final int batchSize,
                                     int threadCount,
                                     final DiffStats stats,
                                     final DiffTask task) throws InterruptedException {
        final DiffStrategy strategy = getDiffStrategy(txManager, plusTable, minusTable, batchSize);
        BlockingWorkerPool pool = new BlockingWorkerPool(exec, threadCount);
        for (final MutableRange range : getRanges(threadCount, batchSize)) {
            pool.submitTask(new Runnable() {
                @Override
                public void run() {
                    do {
                        final RangeRequest request = range.getRangeRequest();
                        try {
                            long startTime = System.currentTimeMillis();
                            PartialDiffStats partialStats = task.call(request, range, strategy);
                            stats.rowsOnlyInSource.addAndGet(partialStats.rowsOnlyInSource);
                            stats.rowsPartiallyInCommon.addAndGet(partialStats.rowsPartiallyInCommon);
                            stats.rowsCompletelyInCommon.addAndGet(partialStats.rowsCompletelyInCommon);
                            stats.rowsVisited.addAndGet(partialStats.rowsVisited);
                            stats.cellsOnlyInSource.addAndGet(partialStats.cellsOnlyInSource);
                            stats.cellsInCommon.addAndGet(partialStats.cellsInCommon);
                            if (log.isInfoEnabled()) {
                                log.info("Processed diff of " +
                                        "{} rows " +
                                        "{} rows only in source " +
                                        "{} rows partially in common " +
                                        "{} rows completely in common " +
                                        "{} cells only in source " +
                                        "{} cells in common " +
                                        "between {} and {} in {} ms.",
                                        partialStats.rowsVisited,
                                        partialStats.rowsOnlyInSource,
                                        partialStats.rowsPartiallyInCommon,
                                        partialStats.rowsCompletelyInCommon,
                                        partialStats.cellsOnlyInSource,
                                        partialStats.cellsInCommon,
                                        plusTable,
                                        minusTable,
                                        System.currentTimeMillis() - startTime);
                            }
                        } catch (InterruptedException e) {
                            throw Throwables.rewrapAndThrowUncheckedException(e);
                        }
                    } while (!range.isComplete());
                }
            });
        }
        pool.waitForSubmittedTasks();
    }

    private static DiffStrategy getDiffStrategy(TransactionManager txManager,
                                                final TableReference plusTable,
                                                final TableReference minusTable,
                                                final int batchSize) {
        final DiffStrategy strategy = txManager.runTaskWithRetry(
                new RuntimeTransactionTask<DiffStrategy>() {
            @Override
            public DiffStrategy execute(final Transaction t) {
                long minusSize = estimateSize(t, minusTable, batchSize, Functions.<byte[]>identity());
                long plusSize = estimateSize(t, plusTable, batchSize, Functions.<byte[]>identity());
                return minusSize > 4 * plusSize ? DiffStrategy.ROWS : DiffStrategy.RANGE;
            }
        });
        return strategy;
    }

    private static PartialDiffStats diffInternal(final Transaction t,
                                                 TableReference plusTable,
                                                 final TableReference minusTable,
                                                 final RangeRequest request,
                                                 final MutableRange range,
                                                 final DiffStrategy strategy,
                                                 final DiffVisitor visitor) {
        final PartialDiffStats partialStats = new PartialDiffStats();
        boolean isEmpty = t.getRange(plusTable, request).batchAccept(range.getBatchSize(),
                new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
            @Override
            public boolean visit(List<RowResult<byte[]>> batch) {
                partialStats.rowsOnlyInSource = 0;
                partialStats.rowsPartiallyInCommon = 0;
                partialStats.rowsCompletelyInCommon = 0;
                partialStats.rowsVisited = 0;
                partialStats.cellsOnlyInSource = 0;
                partialStats.cellsInCommon = 0;
                byte[] lastRow = batch.get(batch.size() - 1).getRowName();
                if (batch.size() < range.getBatchSize()) {
                    range.setStartRow(null);
                } else {
                    range.setStartRow(RangeRequests.nextLexicographicName(lastRow));
                }
                Iterable<RowResult<byte[]>> toRemove;
                if (strategy == DiffStrategy.RANGE) {
                    toRemove = BatchingVisitables.visitWhile(
                            t.getRange(minusTable, request), lessThan(lastRow)).immutableCopy();
                } else {
                    toRemove = t.getRows(minusTable,
                            Iterables.transform(batch, RowResult.<byte[]>getRowNameFun()),
                            ColumnSelection.all()).values();
                }
                visitor.visit(t, diffInternal(asCells(batch), asCells(toRemove), partialStats));
                partialStats.rowsVisited += batch.size();
                return false;
            }
        });
        if (isEmpty) {
            range.setStartRow(null);
        }
        return partialStats;
    }

    private static Iterable<Cell> asCells(final Iterable<RowResult<byte[]>> results) {
        return new Iterable<Cell>() {
            @Override
            public Iterator<Cell> iterator() {
                return new AbstractIterator<Cell>() {
                    private final Iterator<RowResult<byte[]>> outerIter = results.iterator();
                    private byte[] row = null;
                    private Iterator<byte[]> innerIter = null;
                    @Override
                    protected Cell computeNext() {
                        while (true) {
                            if (innerIter != null && innerIter.hasNext()) {
                                byte[] col = innerIter.next();
                                return Cell.create(row, col);
                            }
                            if (!outerIter.hasNext()) {
                                return endOfData();
                            }
                            RowResult<byte[]> result = outerIter.next();
                            row = result.getRowName();
                            innerIter = result.getColumns().keySet().iterator();
                        }
                    }
                };
            }
        };
    }

    private static Iterator<Cell> diffInternal(final Iterable<Cell> plus,
                                               final Iterable<Cell> minus,
                                               final PartialDiffStats partialStats) {
        return new AbstractIterator<Cell>() {
            private final Iterator<Cell> plusIter = plus.iterator();
            private final Iterator<Cell> minusIter = minus.iterator();
            private byte[] currKey;
            private boolean keyOnlyInSource;
            private boolean keyInCommon;
            private Cell currPlus;
            private Cell currMinus;
            @Override
            protected Cell computeNext() {
                currPlus = null;
                while (true) {
                    if (currPlus == null) {
                        if (!plusIter.hasNext()) {
                            recordStats();
                            return endOfData();
                        }
                        currPlus = plusIter.next();
                    }
                    if (!matches(currPlus, currKey)) {
                        recordStats();
                        keyOnlyInSource = false;
                        keyInCommon = false;
                    }
                    currKey = currPlus.getRowName();
                    if (currMinus == null) {
                        if (!minusIter.hasNext()) {
                            return onlyInSource();
                        }
                        currMinus = minusIter.next();
                    }
                    int comparison = currPlus.compareTo(currMinus);
                    if (comparison < 0) {
                        return onlyInSource();
                    } else if (comparison > 0) {
                        currMinus = null;
                    } else {
                        keyInCommon = true;
                        partialStats.cellsInCommon++;
                        currPlus = null;
                        currMinus = null;
                    }
                }
            }

            private Cell onlyInSource() {
                keyOnlyInSource = true;
                partialStats.cellsOnlyInSource++;
                return currPlus;
            }

            private void recordStats() {
                if (!keyOnlyInSource) {
                    partialStats.rowsCompletelyInCommon++;
                } else if (keyInCommon) {
                    partialStats.rowsPartiallyInCommon++;
                } else {
                    partialStats.rowsOnlyInSource++;
                }
            }

            private boolean matches(Cell cell, byte[] key) {
                return key == null || UnsignedBytes.lexicographicalComparator().compare(cell.getRowName(), key) == 0;
            }
        };
    }

    private static Predicate<RowResult<byte[]>> lessThan(@Inclusive final byte[] max) {
        return new Predicate<RowResult<byte[]>>() {
            @Override
            public boolean apply(RowResult<byte[]> bytes) {
                return UnsignedBytes.lexicographicalComparator()
                        .compare(bytes.getRowName(), max) <= 0;
            }
        };
    }

    private enum DiffStrategy {
        RANGE, ROWS;
    }

    private static Iterable<MutableRange> getRanges(int threadCount, int batchSize) {
        if (threadCount == 1) {
            return ImmutableList.of(new MutableRange(new byte[0], new byte[0], batchSize));
        }
        Preconditions.checkState(threadCount > 0);
        threadCount = Math.min(threadCount, 256);
        byte step = (byte) (256 / threadCount);
        byte curr = step;
        Collection<MutableRange> ranges = Lists.newArrayListWithCapacity(threadCount);
        ranges.add(new MutableRange(new byte[0], new byte[] {step}, batchSize));
        for (int i = 1; i < threadCount - 1; i++) {
            byte next = (byte) (curr + step);
            ranges.add(new MutableRange(new byte[] {curr}, new byte[] {next}, batchSize));
            curr = next;
        }
        ranges.add(new MutableRange(new byte[] {curr}, new byte[0], batchSize));
        return ranges;
    }

    private static class MutableRange {
        private byte[] startRow;
        private final byte[] endRow;
        private final int batchSize;

        public MutableRange(byte[] startRow, byte[] endRow, int batchSize) {
            this.startRow = Preconditions.checkNotNull(startRow);
            this.endRow = Preconditions.checkNotNull(endRow);
            this.batchSize = batchSize;
        }

        public void setStartRow(byte[] startRow) {
            this.startRow = startRow;
        }

        public RangeRequest getRangeRequest() {
            return RangeRequest.builder().startRowInclusive(startRow).endRowExclusive(endRow).build();
        }

        public int getBatchSize() {
            return batchSize;
        }

        public boolean isComplete() {
            return startRow == null;
        }
    }
}
