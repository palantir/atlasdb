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
package com.palantir.atlasdb.shell;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.Nullable;

import org.jruby.RubyProc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.UniformRowNamePartitioner;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

/**
 * This class wraps a more Ruby-friendly interface around {@link Transaction}
 */
final public class AtlasShellTransactionAdapter {
    private final Transaction transaction;
    private final AtlasShellRuby atlasShellRuby;

    // For parallel range requests
    private static final int CHUNKS_PER_THREAD = 100;
    private static final int MAX_ALLOWED_THREADS = 16;

    public AtlasShellTransactionAdapter(Transaction transaction, AtlasShellRuby atlasShellRuby) {
        assert transaction.isUncommitted();
        this.atlasShellRuby = atlasShellRuby;
        this.transaction = transaction;
    }

    public void each(String tableName,
                     @Nullable byte[] startRowInclusive,
                     @Nullable byte[] endRowExclusive,
                     @Nullable String[] columns,
                     int batchSize,
                     final RubyProc rubyProc) {
        RangeRequest.Builder rangeRequestBuilder = RangeRequest.builder();
        if (columns != null && columns.length > 0) {
            List<byte[]> columnsToRetain = Lists.newArrayList();
            for (String column : columns) {
                columnsToRetain.add(column.getBytes());
            }
            rangeRequestBuilder.retainColumns(columnsToRetain);
        }
        if (startRowInclusive != null) {
            rangeRequestBuilder.startRowInclusive(startRowInclusive);
        }
        if (endRowExclusive != null) {
            rangeRequestBuilder.endRowExclusive(endRowExclusive);
        }
        RangeRequest rangeRequest = rangeRequestBuilder.build();
        BatchingVisitable<RowResult<byte[]>> batchingVisitable = transaction.getRange(
                tableName,
                rangeRequest);
        final AtlasShellInterruptListener interruptListener = new AtlasShellInterruptListener();
        AbortingVisitor<RowResult<byte[]>, RuntimeException> abortingVisitor = new AbortingVisitor<RowResult<byte[]>, RuntimeException>() {
            @Override
            public boolean visit(RowResult<byte[]> item) throws RuntimeException {
                // Recall that if the visit() method returns false, then the AbortingVisitor will terminate
                if (interruptListener.isInterrupted()) {
                    throw new AtlasShellInterruptException("The current operation was interrupted (Ctrl+C cancels" +
                            " the current operation; use Ctrl+X instead to copy text without canceling the current" +
                            " operation)");
                    // TODO (ejin): Add a reasonable way to indicate where the execution left off
                } else {
                    return (Boolean) atlasShellRuby.call(rubyProc, "call", new Object[] { item });
                }
            }
        };
        AbortingVisitor<Iterable<RowResult<byte[]>>, RuntimeException> batchingAbortingVisitor = AbortingVisitors.batching(abortingVisitor);
        atlasShellRuby.getInterruptCallback().registerInterruptListener(interruptListener);
        try {
            batchingVisitable.batchAccept(batchSize, batchingAbortingVisitor);
        } finally {
            atlasShellRuby.getInterruptCallback().unregisterInterruptListener(interruptListener);
        }
    }

    public void eachPrefixParallel(final String tableName,
                                   byte[] startRowInclusive,
                                   byte[] endRowExclusive,
                                   @Nullable final String[] columns,
                                   final int batchSize,
                                   int numThreads,
                                   ValueType valueType,
                                   final RubyProc rubyProc) throws ExecutionException, InterruptedException {
        Preconditions.checkArgument(numThreads > 0);
        numThreads = Math.min(numThreads, MAX_ALLOWED_THREADS);

        Preconditions.checkArgument(isPrefixRange(startRowInclusive, endRowExclusive), "eachPrefixParallel must be called with a prefix range");

        List<byte[]> partitionSuffixes = new UniformRowNamePartitioner(valueType).getPartitions(numThreads * CHUNKS_PER_THREAD);
        List<byte[]> sortedPartitionSuffixes = Ordering.from(UnsignedBytes.lexicographicalComparator()).sortedCopy(partitionSuffixes);
        final List<byte[]> partitions = Lists.newArrayList();
        for (int i = 0; i < sortedPartitionSuffixes.size(); i++) {
            partitions.add(Bytes.concat(startRowInclusive, sortedPartitionSuffixes.get(i)));
        }
        partitions.add(endRowExclusive);

        // TODO (ejin): Should we have one executor per call, or just one for the class?
        // Note that if we have just one executor for the class, then it might not make
        // sense to have the parameter 'numThreads' anymore
        ThreadPoolExecutor executor = PTExecutors.newFixedThreadPool(
                numThreads,
                new NamedThreadFactory("Parallel range request for table: " + tableName));

        List<Future<Void>> futures = Lists.newArrayList();

        for (int i = 0; i < partitions.size() - 1; i++) {
            final byte[] chunkStartInclusive = partitions.get(i);
            final byte[] chunkEndExclusive = partitions.get(i + 1);
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    each(
                            tableName,
                            chunkStartInclusive,
                            chunkEndExclusive,
                            columns,
                            batchSize,
                            rubyProc);
                    return null;
                }
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }
        executor.shutdown();
    }

    public boolean isPrefixRange(byte[] startRowInclusive, byte[] endRowExclusive) {
        if (startRowInclusive == null && endRowExclusive == null) {
            // We're okay if there's a 'null' prefix
            return true;
        } else if (startRowInclusive == null || endRowExclusive == null) {
            return false;
        } else {
            // Ensure that this actually is a prefix range
            return Arrays.equals(
                    endRowExclusive, RangeRequests.createEndNameForPrefixScan(startRowInclusive));
        }
    }

    public void delete(String tableName, byte[] row, byte[][] columns) {
        Set<Cell> cells = Sets.newHashSet();
        for (byte[] column : columns) {
            Cell cell = Cell.create(row, column);
            cells.add(cell);
        }
        transaction.delete(tableName, cells);
    }

    public void put(String tableName, byte[] row, Map<byte[], byte[]> columnToValue) {
        Map<Cell, byte[]> cellToValue = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> columnAndValue : columnToValue.entrySet()) {
            byte[] column = columnAndValue.getKey();
            byte[] value = columnAndValue.getValue();
            Cell cell = Cell.create(row, column);
            cellToValue.put(cell, value);
        }
        transaction.put(tableName, cellToValue);
    }

    public void commit() {
        transaction.commit();
    }

    public void abort() {
        transaction.abort();
    }

    boolean isUncommitted() {
        return transaction.isUncommitted();
    }

    long getTimestamp() {
        return transaction.getTimestamp();
    }
}
