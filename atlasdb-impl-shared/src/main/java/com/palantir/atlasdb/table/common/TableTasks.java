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
package com.palantir.atlasdb.table.common;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.annotation.Inclusive;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.lock.LockRefreshToken;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public final class TableTasks {
    private static final SafeLogger log = SafeLoggerFactory.get(TableTasks.class);

    private TableTasks() {
        // Utility class
    }

    private interface InterruptibleRangeTask {
        void execute(MutableRange range) throws InterruptedException;
    }

    public static void copy(
            TransactionManager txManager,
            ExecutorService exec,
            TableReference srcTable,
            TableReference dstTable,
            int batchSize,
            int threadCount,
            @Output CopyStats stats)
            throws InterruptedException {
        copyExternal(
                exec,
                srcTable,
                dstTable,
                batchSize,
                threadCount,
                stats,
                (request, range) ->
                        txManager.runTaskWithRetry(tx -> copyInternal(tx, srcTable, dstTable, request, range)));
    }

    public static void copy(
            TransactionManager txManager,
            ExecutorService exec,
            Iterable<LockRefreshToken> _lockTokens,
            final TableReference srcTable,
            final TableReference dstTable,
            int batchSize,
            int threadCount,
            @Output CopyStats stats)
            throws InterruptedException {
        copyExternal(
                exec,
                srcTable,
                dstTable,
                batchSize,
                threadCount,
                stats,
                (request, range) ->
                        txManager.runTaskWithRetry(tx -> copyInternal(tx, srcTable, dstTable, request, range)));
    }

    public static void copyExternal(
            ExecutorService exec,
            final TableReference srcTable,
            final TableReference dstTable,
            int batchSize,
            int threadCount,
            final CopyStats stats,
            final CopyTask task)
            throws InterruptedException {
        new InterruptibleRangeExecutor(exec, batchSize, threadCount)
                .executeTask("copy", range -> executeCopyTask(srcTable, dstTable, stats, task, range));
    }

    private static void executeCopyTask(
            TableReference srcTable, TableReference dstTable, CopyStats stats, CopyTask task, MutableRange range)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        PartialCopyStats partialStats = task.call(range.getRangeRequest(), range);
        stats.rowsCopied.addAndGet(partialStats.rowsCopied);
        stats.cellsCopied.addAndGet(partialStats.cellsCopied);
        log.info(
                "Copied {} rows, {} cells from {} to {} in {} ms.",
                SafeArg.of("rowsCopied", partialStats.rowsCopied),
                SafeArg.of("cellsCopied", partialStats.cellsCopied),
                LoggingArgs.tableRef("srcTable", srcTable),
                LoggingArgs.tableRef("dstTable", dstTable),
                SafeArg.of("timeTaken", System.currentTimeMillis() - startTime));
    }

    private static PartialCopyStats copyInternal(
            final Transaction transaction,
            final TableReference srcTable,
            final TableReference dstTable,
            RangeRequest request,
            final MutableRange range) {
        final PartialCopyStats stats = new PartialCopyStats();
        boolean isEmpty = transaction.getRange(srcTable, request).batchAccept(range.getBatchSize(), batch -> {
            Map<Cell, byte[]> entries = Maps.newHashMapWithExpectedSize(batch.size());
            for (RowResult<byte[]> result : batch) {
                for (Map.Entry<Cell, byte[]> entry : result.getCells()) {
                    entries.put(entry.getKey(), entry.getValue());
                }
            }
            if (batch.size() < range.getBatchSize()) {
                range.setStartRow(null);
            } else {
                byte[] lastRow = batch.get(batch.size() - 1).getRowName();
                range.setStartRow(RangeRequests.nextLexicographicName(lastRow));
            }
            transaction.put(dstTable, entries);
            stats.rowsCopied = batch.size();
            stats.cellsCopied = entries.size();
            return false;
        });
        if (isEmpty) {
            range.setStartRow(null);
        }
        return stats;
    }

    public static long estimateSize(
            Transaction transaction, TableReference table, int batchSize, Function<byte[], byte[]> uniformizer) {
        final AtomicLong estimate = new AtomicLong();
        transaction.getRange(table, RangeRequest.all()).batchAccept(batchSize, batch -> {
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
        });
        return estimate.get();
    }

    public static void diff(
            final TransactionManager txManager,
            ExecutorService exec,
            final TableReference plusTable,
            final TableReference minusTable,
            int batchSize,
            int threadCount,
            @Output DiffStats stats,
            final DiffVisitor visitor)
            throws InterruptedException {
        DiffStrategy diffStrategy =
                txManager.runTaskWithRetry(t -> getDiffStrategy(t, plusTable, minusTable, batchSize));
        diffExternal(
                diffStrategy,
                exec,
                plusTable,
                minusTable,
                batchSize,
                threadCount,
                stats,
                (request, range, strategy) -> txManager.runTaskWithRetry(
                        t -> diffInternal(t, plusTable, minusTable, request, range, strategy, visitor)));
    }

    /**
     * Deprecated.
     * @deprecated Use {@link #diff(TransactionManager, ExecutorService, TableReference, TableReference,
     *             int, int, DiffStats, DiffVisitor)} instead
     */
    @Deprecated
    @SuppressWarnings("InlineMeSuggester")
    public static void diff(
            final TransactionManager txManager,
            ExecutorService exec,
            final Iterable<LockRefreshToken> _lockTokens,
            final TableReference plusTable,
            final TableReference minusTable,
            final int batchSize,
            int threadCount,
            @Output DiffStats stats,
            final DiffVisitor visitor)
            throws InterruptedException {
        diff(txManager, exec, plusTable, minusTable, batchSize, threadCount, stats, visitor);
    }

    private static void diffExternal(
            final DiffStrategy strategy,
            ExecutorService exec,
            final TableReference plusTable,
            final TableReference minusTable,
            final int batchSize,
            int threadCount,
            final DiffStats stats,
            final DiffTask task)
            throws InterruptedException {
        new InterruptibleRangeExecutor(exec, batchSize, threadCount)
                .executeTask("diff", range -> executeDiffTask(strategy, plusTable, minusTable, stats, task, range));
    }

    private static void executeDiffTask(
            DiffStrategy strategy,
            TableReference plusTable,
            TableReference minusTable,
            DiffStats stats,
            DiffTask task,
            MutableRange range)
            throws InterruptedException {
        final RangeRequest request = range.getRangeRequest();
        long startTime = System.currentTimeMillis();
        PartialDiffStats partialStats = task.call(request, range, strategy);
        stats.rowsOnlyInSource.addAndGet(partialStats.rowsOnlyInSource);
        stats.rowsPartiallyInCommon.addAndGet(partialStats.rowsPartiallyInCommon);
        stats.rowsCompletelyInCommon.addAndGet(partialStats.rowsCompletelyInCommon);
        stats.rowsVisited.addAndGet(partialStats.rowsVisited);
        stats.cellsOnlyInSource.addAndGet(partialStats.cellsOnlyInSource);
        stats.cellsInCommon.addAndGet(partialStats.cellsInCommon);
        if (log.isInfoEnabled()) {
            log.info(
                    "Processed diff of "
                            + "{} rows "
                            + "{} rows only in source "
                            + "{} rows partially in common "
                            + "{} rows completely in common "
                            + "{} cells only in source "
                            + "{} cells in common "
                            + "between {} and {} in {} ms.",
                    SafeArg.of("rowsVisited", partialStats.rowsVisited),
                    SafeArg.of("rowsOnlyInSource", partialStats.rowsOnlyInSource),
                    SafeArg.of("rowsPartiallyInCommon", partialStats.rowsPartiallyInCommon),
                    SafeArg.of("rowsCompletelyInCommon", partialStats.rowsCompletelyInCommon),
                    SafeArg.of("cellsOnlyInSource", partialStats.cellsOnlyInSource),
                    SafeArg.of("cellsInCommon", partialStats.cellsInCommon),
                    LoggingArgs.tableRef("plusTable", plusTable),
                    LoggingArgs.tableRef("minusTable", minusTable),
                    SafeArg.of("timeTaken", System.currentTimeMillis() - startTime));
        }
    }

    private static DiffStrategy getDiffStrategy(
            Transaction tx, TableReference plusTable, TableReference minusTable, int batchSize) {
        long minusSize = estimateSize(tx, minusTable, batchSize, Functions.identity());
        long plusSize = estimateSize(tx, plusTable, batchSize, Functions.identity());
        return minusSize > 4 * plusSize ? DiffStrategy.ROWS : DiffStrategy.RANGE;
    }

    private static PartialDiffStats diffInternal(
            final Transaction tx,
            TableReference plusTable,
            final TableReference minusTable,
            final RangeRequest request,
            final MutableRange range,
            final DiffStrategy strategy,
            final DiffVisitor visitor) {
        final PartialDiffStats partialStats = new PartialDiffStats();
        boolean isEmpty = tx.getRange(plusTable, request).batchAccept(range.getBatchSize(), batch -> {
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
                toRemove = BatchingVisitables.visitWhile(tx.getRange(minusTable, request), lessThan(lastRow))
                        .immutableCopy();
            } else {
                toRemove = tx.getRows(
                                minusTable, Lists.transform(batch, RowResult.getRowNameFun()), ColumnSelection.all())
                        .values();
            }
            visitor.visit(tx, diffInternal(asCells(batch), asCells(toRemove), partialStats));
            partialStats.rowsVisited += batch.size();
            return false;
        });
        if (isEmpty) {
            range.setStartRow(null);
        }
        return partialStats;
    }

    private static Iterator<Cell> diffInternal(
            final Iterable<Cell> plus, final Iterable<Cell> minus, final PartialDiffStats partialStats) {
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

    private static Iterable<Cell> asCells(final Iterable<RowResult<byte[]>> results) {
        return () -> new AbstractIterator<Cell>() {
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

    private static Predicate<RowResult<byte[]>> lessThan(@Inclusive final byte[] max) {
        return bytes -> UnsignedBytes.lexicographicalComparator().compare(bytes.getRowName(), max) <= 0;
    }

    private static Iterable<MutableRange> getRanges(int givenThreadCount, int batchSize) {
        int threadCount = Math.min(givenThreadCount, 256);

        Preconditions.checkState(threadCount > 0, "threadCount must be positive");
        if (threadCount == 1) {
            return ImmutableList.of(new MutableRange(new byte[0], new byte[0], batchSize));
        }

        byte step = (byte) (256 / threadCount);
        byte curr = step;
        Collection<MutableRange> ranges = new ArrayList<>(threadCount);
        ranges.add(new MutableRange(new byte[0], new byte[] {step}, batchSize));
        for (int i = 1; i < threadCount - 1; i++) {
            byte next = (byte) (curr + step);
            ranges.add(new MutableRange(new byte[] {curr}, new byte[] {next}, batchSize));
            curr = next;
        }
        ranges.add(new MutableRange(new byte[] {curr}, new byte[0], batchSize));
        return ranges;
    }

    public interface DiffVisitor {
        void visit(Transaction transaction, Iterator<Cell> partialDiff);
    }

    public static class DiffStats {
        private final AtomicLong rowsOnlyInSource;
        private final AtomicLong rowsPartiallyInCommon;
        private final AtomicLong rowsCompletelyInCommon;
        private final AtomicLong rowsVisited;
        private final AtomicLong cellsOnlyInSource;
        private final AtomicLong cellsInCommon;

        public DiffStats(
                AtomicLong rowsOnlyInSource,
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

    public static class CopyStats {
        private final AtomicLong rowsCopied;
        private final AtomicLong cellsCopied;

        public CopyStats(AtomicLong rowsCopied, AtomicLong cellsCopied) {
            this.rowsCopied = rowsCopied;
            this.cellsCopied = cellsCopied;
        }
    }

    private enum DiffStrategy {
        RANGE,
        ROWS;
    }

    private interface DiffTask {
        PartialDiffStats call(RangeRequest request, MutableRange range, DiffStrategy strategy)
                throws InterruptedException;
    }

    private interface CopyTask {
        PartialCopyStats call(RangeRequest request, MutableRange range) throws InterruptedException;
    }

    private static final class PartialCopyStats {
        private long rowsCopied = 0;
        private long cellsCopied = 0;
    }

    private static class InterruptibleRangeExecutor {
        private final ExecutorService exec;
        private final int batchSize;
        private final int threadCount;

        InterruptibleRangeExecutor(ExecutorService exec, int batchSize, int threadCount) {
            this.exec = exec;
            this.batchSize = batchSize;
            this.threadCount = threadCount;
        }

        void executeTask(String taskName, InterruptibleRangeTask task) throws InterruptedException {
            BlockingWorkerPool pool = new BlockingWorkerPool(exec, threadCount);
            for (final MutableRange range : getRanges(threadCount, batchSize)) {
                if (Thread.currentThread().isInterrupted()) {
                    log.info(
                            "Thread interrupted. Cancelling {} of range {}",
                            SafeArg.of("taskName", taskName),
                            UnsafeArg.of("range", range));
                    return;
                }
                pool.submitTask(() -> {
                    do {
                        if (Thread.currentThread().isInterrupted()) {
                            log.info(
                                    "Thread interrupted. Cancelling {} of range {}",
                                    SafeArg.of("taskName", taskName),
                                    UnsafeArg.of("range", range));
                            break;
                        }
                        try {
                            task.execute(range);
                        } catch (InterruptedException e) {
                            throw Throwables.rewrapAndThrowUncheckedException(e);
                        }
                    } while (!range.isComplete());
                });
            }
            pool.waitForSubmittedTasks();
        }
    }
}
