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
package com.palantir.atlasdb.transaction.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.BatchingVisitable;

/**
 * Provides the methods for a transaction with the key-value store.
 * @see TransactionManager
 */
public interface Transaction {

    @Idempotent
    SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection);

    @Idempotent
    Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection);

    @Idempotent
    Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int batchHint);

    @Idempotent
    Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells);

    /**
     * Creates a visitable that scans the provided range.
     *
     * @param tableRef the table to scan
     * @param rangeRequest the range of rows and columns to scan
     * @return an array of <code>RowResult</code> objects representing the range
     */
    @Idempotent
    BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest);

    /**
     * Creates a visitable that scans the provided range.
     * <p>
     * To get good performance out of this method, it is important to specify
     * the batch hint in each RangeRequest. If this isn't done then this method may do more work
     * than you need and will be slower than it needs to be.  If the batchHint isn't specified it
     * will default to 1 for the first page in each range.
     *
     * @deprecated Should use either {@link #getRanges(TableReference, Iterable, int, BiFunction)} or
     * {@link #getRangesLazy(TableReference, Iterable)} to ensure you are using an appropriate level
     * of concurrency for your specific workflow.
     */
    @Idempotent
    @Deprecated
    Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests);

    /**
     * Creates unvisited visitibles that scan the provided ranges and then applies the provided visitableProcessor
     * function with concurrency specified by the concurrencyLevel parameter.
     */
    @Idempotent
    <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor);

    /**
     * Same as {@link #getRanges(TableReference, Iterable, int, BiFunction)} but uses the default concurrency
     * value specified by {@link KeyValueServiceConfig#defaultGetRangesConcurrency()}.
     */
    @Idempotent
    <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor);

    /**
     * Returns visitibles that scan the provided ranges. This does no pre-fetching so visiting the resulting
     * visitibles will incur database reads on first access.
     */
    @Idempotent
    Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests);

    /**
     * Puts values into the key-value store. If you put a null or the empty byte array, then
     * this is treated like a delete to the store.
     * @param tableRef the table into which to put the values
     * @param values the values to append to the table
     */
    @Idempotent
    void put(TableReference tableRef, Map<Cell, byte[]> values);

    /**
     * Deletes values from the key-value store.
     * @param tableRef the table from which to delete the values
     * @param keys the set of cells to delete from the store
     */
    @Idempotent
    void delete(TableReference tableRef, Set<Cell> keys);

    @Idempotent
    TransactionType getTransactionType();

    @Idempotent
    void setTransactionType(TransactionType transactionType);

    enum TransactionType {
        DEFAULT,

        /**
         * Hard delete transactions are different from regular transactions because they
         * must queue cells for "scrubbing" on every cell that's modified or deleted.
         * (i.e. not just write a value at the latest timestamp, but also clean up values at older timestamps)
         */
        HARD_DELETE,

        /**
         * In addition to queuing cells for "scrubbing", we also:
         * - (a) Scrub earlier than we would have otherwise, even at the cost of possibly
         *       causing open transactions to abort, and
         * - (b) Block until the scrub is complete.
         */
        AGGRESSIVE_HARD_DELETE
    }

    /**
     * Aborts the transaction. Can be called repeatedly.
     * <p>
     * Cannot be called after a call to {@link #commit()}. Check {@link #isUncommitted()} to be sure.
     */
    @Idempotent
    void abort();

    /**
     * Commits the transaction. Can be called repeatedly, but will only commit the first time.
     * Cannot be called after a call to {@link #abort()}.
     * Check {@link #isAborted()} before calling commit if unsure.
     * <p>
     * This method checks for any data conflicts.  This can happen if someone else is
     * concurrently writing to the same {@link Cell}s.
     * <p>
     * NOTE: only cell-level conflicts are detected.  If another transaction writes to the same row
     * but a different cell, then no conflict occurs.
     * @throws TransactionConflictException if this transaction had a conflict with another running transaction
     * @throws TransactionCommitFailedException if this transaction failed at commit time.  If this exception
     * is thrown, then this transaction may have been committed, but most likely not.
     */
    @Idempotent
    void commit() throws TransactionFailedException;

    @Idempotent
    void commit(TransactionService transactionService) throws TransactionFailedException;

    /**
     * Gets whether the transaction has been aborted.
     *
     * @return <code>true</code> if <code>abort()</code> has been called, otherwise <code>false</code>
     */
    @Idempotent
    boolean isAborted();

    /**
     * Gets whether the transaction has not been committed.
     *
     * @return <code>true</code> if neither <code>commit()</code> or <code>abort()</code> have been called,
     *         otherwise <code>false</code>
     */
    @Idempotent
    boolean isUncommitted();

    /**
     * Gets the timestamp the current transaction is running at.
     */
    @Idempotent
    long getTimestamp();

    /**
     * Determines what the transaction should do if it reads a sentinel timestamp which
     * indicates it may be performing an inconsistent read.
     */
    TransactionReadSentinelBehavior getReadSentinelBehavior();

    /**
     * Informs the transaction that a particular table has been written to.
     */
    void useTable(TableReference tableRef, ConstraintCheckable table);
}
