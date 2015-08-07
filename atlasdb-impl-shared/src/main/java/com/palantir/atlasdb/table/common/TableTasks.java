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
import com.google.common.base.Suppliers;
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
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.annotation.Inclusive;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;

public class TableTasks {
    private static final Logger log = LoggerFactory.getLogger(TableTasks.class);

    private static interface CopyTask {
        long call(RangeRequest request, MutableRange range) throws InterruptedException;
    }

    public static void copy(final TransactionManager txManager,
                            ExecutorService exec,
                            final String srcTable,
                            final String dstTable,
                            int batchSize,
                            int threadCount,
                            @Output AtomicLong counter) throws InterruptedException {
        copyExternal(exec, srcTable, dstTable, batchSize, threadCount, counter, new CopyTask() {
            @Override
            public long call(final RangeRequest request, final MutableRange range) {
                return txManager.runTaskWithRetry(new RuntimeTransactionTask<Long>() {
                    @Override
                    public Long execute(final Transaction t) {
                        return copyInternal(t, srcTable, dstTable, request, range);
                    }
                });
            }
        });
    }

    public static void copy(final LockAwareTransactionManager txManager,
                            ExecutorService exec,
                            final Iterable<LockRefreshToken> lockTokens,
                            final String srcTable,
                            final String dstTable,
                            int batchSize,
                            int threadCount,
                            @Output AtomicLong counter) throws InterruptedException {
        copyExternal(exec, srcTable, dstTable, batchSize, threadCount, counter, new CopyTask() {
            @Override
            public long call(final RangeRequest request, final MutableRange range) throws InterruptedException {
                return txManager.runTaskWithLocksWithRetry(lockTokens, Suppliers.<LockRequest>ofInstance(null),
                        new LockAwareTransactionTask<Long, RuntimeException>() {
                    @Override
                    public Long execute(Transaction t, Iterable<LockRefreshToken> heldLocks) {
                        return copyInternal(t, srcTable, dstTable, request, range);
                    }
                });
            }
        });
    }

    public static void copyExternal(ExecutorService exec,
                                    final String srcTable,
                                    final String dstTable,
                                    int batchSize,
                                    int threadCount,
                                    final AtomicLong counter,
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
                            long numCopied = task.call(request, range);
                            counter.addAndGet(numCopied);
                            log.info("Copied {} rows from {} to {} in {} ms.",
                                    numCopied,
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

    private static long copyInternal(final Transaction t,
                                     final String srcTable,
                                     final String dstTable,
                                     RangeRequest request,
                                     final MutableRange range) {
        final AtomicLong numCopied = new AtomicLong();
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
                numCopied.set(entries.size());
                return false;
            }
        });
        if (isEmpty) {
            range.setStartRow(null);
        }
        return numCopied.get();
    }

    public static long estimateSize(Transaction t,
                                    String table,
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
        private final AtomicLong itemsOnlyInSource;
        private final AtomicLong itemsInCommon;

        public DiffStats(AtomicLong itemsOnlyInSource,
                         AtomicLong itemsInCommon) {
            this.itemsOnlyInSource = itemsOnlyInSource;
            this.itemsInCommon = itemsInCommon;
        }
    }

    public static interface DiffVisitor {
        void visit(Transaction t, Iterator<Cell> partialDiff);
    }

    public static void diff(final TransactionManager txManager,
                            ExecutorService exec,
                            final String plusTable,
                            final String minusTable,
                            int batchSize,
                            int threadCount,
                            @Output DiffStats counter,
                            final DiffVisitor visitor) throws InterruptedException {
        diffExternal(txManager, exec, plusTable, minusTable, batchSize, threadCount, counter, new DiffTask() {
            @Override
            public DiffStats call(final RangeRequest request, final MutableRange range, final DiffStrategy strategy)
                    throws InterruptedException {
                return txManager.runTaskWithRetry(new RuntimeTransactionTask<DiffStats>() {
                    @Override
                    public DiffStats execute(final Transaction t) {
                        return diffInternal(t, plusTable, minusTable, request, range, strategy, visitor);
                    }
                });
            }
        });
    }

    public static void diff(final LockAwareTransactionManager txManager,
                            ExecutorService exec,
                            final Iterable<LockRefreshToken> lockTokens,
                            final String plusTable,
                            final String minusTable,
                            final int batchSize,
                            int threadCount,
                            @Output DiffStats counter,
                            final DiffVisitor visitor) throws InterruptedException {
        diffExternal(txManager, exec, plusTable, minusTable, batchSize, threadCount, counter, new DiffTask() {
            @Override
            public DiffStats call(final RangeRequest request, final MutableRange range, final DiffStrategy strategy)
                    throws InterruptedException {
                return txManager.runTaskWithLocksWithRetry(lockTokens, Suppliers.<LockRequest>ofInstance(null),
                        new LockAwareTransactionTask<DiffStats, RuntimeException>() {
                    @Override
                    public DiffStats execute(Transaction t, Iterable<LockRefreshToken> heldLocks) {
                        return diffInternal(t, plusTable, minusTable, request, range, strategy, visitor);
                    }
                });
            }
        });
    }

    private static interface DiffTask {
        DiffStats call(RangeRequest request, MutableRange range, DiffStrategy strategy) throws InterruptedException;
    }

    private static void diffExternal(final TransactionManager txManager,
                                     ExecutorService exec,
                                     final String plusTable,
                                     final String minusTable,
                                     final int batchSize,
                                     int threadCount,
                                     final DiffStats counter,
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
                            DiffStats partialStats = task.call(request, range, strategy);
                            counter.itemsInCommon.addAndGet(partialStats.itemsInCommon.get());
                            counter.itemsOnlyInSource.addAndGet(partialStats.itemsOnlyInSource.get());
                            log.info("Processed diff of {} rows only in source and {} rows in common between {} and {} in {} ms.",
                                    partialStats.itemsInCommon,
                                    partialStats.itemsOnlyInSource,
                                    plusTable,
                                    minusTable,
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

    private static DiffStrategy getDiffStrategy(TransactionManager txManager,
                                                final String plusTable,
                                                final String minusTable,
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

    private static DiffStats diffInternal(final Transaction t,
                                          String plusTable,
                                          final String minusTable,
                                          final RangeRequest request,
                                          final MutableRange range,
                                          final DiffStrategy strategy,
                                          final DiffVisitor visitor) {
        final DiffStats partialStats = new DiffStats(new AtomicLong(), new AtomicLong());
        boolean isEmpty = t.getRange(plusTable, request).batchAccept(range.getBatchSize(),
                new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
            @Override
            public boolean visit(List<RowResult<byte[]>> batch) {
                partialStats.itemsInCommon.set(0);
                partialStats.itemsOnlyInSource.set(0);
                byte[] lastRow = batch.get(batch.size() - 1).getRowName();
                if (batch.size() < batch.size()) {
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
                                               final DiffStats partialStats) {
        return new AbstractIterator<Cell>() {
            private final Iterator<Cell> plusIter = plus.iterator();
            private final Iterator<Cell> minusIter = minus.iterator();
            private Cell currPlus = null;
            private Cell currMinus = null;
            @Override
            protected Cell computeNext() {
                currPlus = null;
                while (true) {
                    if (currPlus == null) {
                        if (!plusIter.hasNext()) {
                            return endOfData();
                        }
                        currPlus = plusIter.next();
                    }
                    if (currMinus == null) {
                        if (!minusIter.hasNext()) {
                            partialStats.itemsOnlyInSource.incrementAndGet();
                            return currPlus;
                        }
                        currMinus = minusIter.next();
                    }
                    int comparison = currPlus.compareTo(currMinus);
                    if (comparison < 0) {
                        partialStats.itemsOnlyInSource.incrementAndGet();
                        return currPlus;
                    } else if (comparison > 0) {
                        currMinus = null;
                    } else {
                        partialStats.itemsInCommon.incrementAndGet();
                        currPlus = null;
                        currMinus = null;
                    }
                }
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
