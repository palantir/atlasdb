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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.lock.HeldLocksToken;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class RangeVisitor {
    private static final SafeLogger log = SafeLoggerFactory.get(RangeVisitor.class);
    private final TransactionManager txManager;
    private final TableReference tableRef;
    private byte[] startRow = new byte[0];
    private byte[] endRow = new byte[0];
    private int batchSize = 1000;
    private int threadCount = 1;
    private ExecutorService exec = MoreExecutors.newDirectExecutorService();
    private Iterable<HeldLocksToken> lockTokens = ImmutableList.of();
    private AtomicLong counter = new AtomicLong();

    public RangeVisitor(TransactionManager txManager, TableReference tableRef) {
        this.txManager = txManager;
        this.tableRef = tableRef;
    }

    public RangeVisitor setStartRowInclusive(byte[] newStartRow) {
        this.startRow = newStartRow;
        return this;
    }

    public RangeVisitor setEndRowExclusive(byte[] newEndRow) {
        this.endRow = newEndRow;
        return this;
    }

    public RangeVisitor setRangePrefix(byte[] prefix) {
        this.startRow = prefix;
        this.endRow = RangeRequests.createEndNameForPrefixScan(prefix);
        return this;
    }

    public RangeVisitor setBatchSize(int newBatchSize) {
        this.batchSize = newBatchSize;
        return this;
    }

    public RangeVisitor setExecutor(ExecutorService newExec) {
        this.exec = newExec;
        return this;
    }

    public RangeVisitor setThreadCount(int newThreadCount) {
        this.threadCount = newThreadCount;
        return this;
    }

    public RangeVisitor setLockTokens(Iterable<HeldLocksToken> newLockTokens) {
        this.lockTokens = newLockTokens;
        return this;
    }

    public RangeVisitor setProgressCounter(AtomicLong newCounter) {
        this.counter = newCounter;
        return this;
    }

    public long visit(final Visitor visitor) throws InterruptedException {
        BlockingWorkerPool pool = new BlockingWorkerPool(exec, threadCount);
        for (final MutableRange range : getRanges()) {
            pool.submitTask(() -> visitRange(visitor, range));
        }
        pool.waitForSubmittedTasks();
        return counter.get();
    }

    private void visitRange(Visitor visitor, MutableRange range) {
        do {
            final RangeRequest request = range.getRangeRequest();
            try {
                long startTime = System.currentTimeMillis();
                long numVisited = txManager.runTaskWithLocksWithRetry(
                        lockTokens, Suppliers.ofInstance(null), new LockAwareTransactionTask<Long, RuntimeException>() {
                            @Override
                            public Long execute(Transaction tx, Iterable<HeldLocksToken> heldLocks) {
                                return visitInternal(tx, visitor, request, range);
                            }

                            @Override
                            public String toString() {
                                return "visitRange(" + request + ")";
                            }
                        });
                counter.addAndGet(numVisited);
                log.info(
                        "Visited {} rows from {} in {} ms.",
                        SafeArg.of("numVisisted", numVisited),
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of("duration", System.currentTimeMillis() - startTime));
            } catch (InterruptedException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        } while (!range.isComplete());
    }

    private long visitInternal(Transaction tx, Visitor visitor, RangeRequest request, MutableRange range) {
        final AtomicLong numVisited = new AtomicLong();
        boolean isEmpty = tx.getRange(tableRef, request).batchAccept(range.getBatchSize(), batch -> {
            visitor.visit(tx, batch);
            if (batch.size() < range.getBatchSize()) {
                range.setStartRow(null);
            } else {
                byte[] lastRow = batch.get(batch.size() - 1).getRowName();
                range.setStartRow(RangeRequests.nextLexicographicName(lastRow));
            }
            numVisited.set(batch.size());
            return false;
        });
        if (isEmpty) {
            range.setStartRow(null);
        }
        return numVisited.get();
    }

    private Iterable<MutableRange> getRanges() {
        if (threadCount == 1) {
            return ImmutableList.of(new MutableRange(startRow, endRow, batchSize));
        }
        int length = Math.max(startRow.length, endRow.length);
        length = Math.max(1, length);
        byte[] expandedStartRow = Arrays.copyOf(startRow, length);
        byte[] expandedEndRow = Arrays.copyOf(endRow, length);
        for (int i = endRow.length; i < length; i++) {
            expandedEndRow[i] = (byte) 0xff;
        }
        BigInteger startNum = new BigInteger(1, expandedStartRow);
        BigInteger endNum = new BigInteger(1, expandedEndRow);
        BigInteger step = endNum.subtract(startNum).divide(BigInteger.valueOf(threadCount));
        BigInteger curr = startNum.add(step);
        Collection<MutableRange> ranges = new ArrayList<>(threadCount);
        ranges.add(new MutableRange(startRow, toBytes(curr, length), batchSize));
        for (int i = 1; i < threadCount - 1; i++) {
            BigInteger next = curr.add(step);
            ranges.add(new MutableRange(toBytes(curr, length), toBytes(next, length), batchSize));
            curr = next;
        }
        ranges.add(new MutableRange(toBytes(curr, length), endRow, batchSize));
        return ranges;
    }

    private static byte[] toBytes(BigInteger num, int length) {
        byte[] rawBytes = num.toByteArray();
        byte[] paddedBytes = new byte[length];
        System.arraycopy(
                rawBytes,
                Math.max(0, rawBytes.length - length),
                paddedBytes,
                Math.max(0, length - rawBytes.length),
                length);
        return paddedBytes;
    }

    public interface Visitor {
        void visit(Transaction tx, List<RowResult<byte[]>> batch);
    }
}
