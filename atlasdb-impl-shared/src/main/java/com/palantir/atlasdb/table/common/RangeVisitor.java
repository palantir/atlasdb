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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;

public class RangeVisitor {
    private static final Logger log = LoggerFactory.getLogger(RangeVisitor.class);
    private final LockAwareTransactionManager txManager;
    private final TableReference tableRef;
    private byte[] startRow = new byte[0];
    private byte[] endRow = new byte[0];
    private int batchSize = 1000;
    private int threadCount = 1;
    private ExecutorService exec = MoreExecutors.newDirectExecutorService();
    private Iterable<HeldLocksToken> lockTokens = ImmutableList.of();
    private AtomicLong counter = new AtomicLong();

    public RangeVisitor(LockAwareTransactionManager txManager,
                        TableReference tableRef) {
        this.txManager = txManager;
        this.tableRef = tableRef;
    }

    public RangeVisitor setStartRowInclusive(byte[] startRow) {
        this.startRow = startRow;
        return this;
    }

    public RangeVisitor setEndRowExclusive(byte[] endRow) {
        this.endRow = endRow;
        return this;
    }

    public RangeVisitor setRangePrefix(byte[] prefix) {
        this.startRow = prefix;
        this.endRow = RangeRequests.createEndNameForPrefixScan(prefix);
        return this;
    }

    public RangeVisitor setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public RangeVisitor setExecutor(ExecutorService exec) {
        this.exec = exec;
        return this;
    }

    public RangeVisitor setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    public RangeVisitor setLockTokens(Iterable<HeldLocksToken> lockTokens) {
        this.lockTokens = lockTokens;
        return this;
    }

    public RangeVisitor setProgressCounter(AtomicLong counter) {
        this.counter = counter;
        return this;
    }

    public long visit(final Visitor visitor) throws InterruptedException {
        BlockingWorkerPool pool = new BlockingWorkerPool(exec, threadCount);
        for (final MutableRange range : getRanges()) {
            pool.submitTask(new Runnable() {
                @Override
                public void run() {
                    visitRange(visitor, range);
                }
            });
        }
        pool.waitForSubmittedTasks();
        return counter.get();
    }

    private void visitRange(final Visitor visitor,
                            final MutableRange range) {
        do {
            final RangeRequest request = range.getRangeRequest();
            try {
                long startTime = System.currentTimeMillis();
                long numVisited = txManager.runTaskWithLocksWithRetry(lockTokens,
                        Suppliers.<LockRequest> ofInstance(null),
                        new LockAwareTransactionTask<Long, RuntimeException>() {
                            @Override
                            public Long execute(Transaction t,
                                                Iterable<HeldLocksToken> heldLocks) {
                                return visitInternal(t, visitor, request, range);
                            }

                            @Override
                            public String toString() {
                                return "visitRange(" + request + ")";
                            }
                        });
                counter.addAndGet(numVisited);
                log.info("Visited {} rows from {} in {} ms.",
                        numVisited,
                        tableRef.getQualifiedName(),
                        System.currentTimeMillis() - startTime);
            } catch (InterruptedException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        } while (!range.isComplete());
    }

    private long visitInternal(final Transaction t,
                               final Visitor visitor,
                               RangeRequest request,
                               final MutableRange range) {
        final AtomicLong numVisited = new AtomicLong();
        boolean isEmpty = t.getRange(tableRef, request).batchAccept(range.getBatchSize(),
                new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
                    @Override
                    public boolean visit(List<RowResult<byte[]>> batch) {
                        visitor.visit(t, batch);
                        if (batch.size() < range.getBatchSize()) {
                            range.setStartRow(null);
                        } else {
                            byte[] lastRow = batch.get(batch.size() - 1).getRowName();
                            range.setStartRow(RangeRequests.nextLexicographicName(lastRow));
                        }
                        numVisited.set(batch.size());
                        return false;
                    }
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
        Collection<MutableRange> ranges = Lists.newArrayListWithCapacity(threadCount);
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
        System.arraycopy(rawBytes, Math.max(0, rawBytes.length - length), paddedBytes, Math.max(0, length - rawBytes.length), length);
        return paddedBytes;
    }

    public static interface Visitor {
        void visit(Transaction t, List<RowResult<byte[]>> batch);
    }
}
