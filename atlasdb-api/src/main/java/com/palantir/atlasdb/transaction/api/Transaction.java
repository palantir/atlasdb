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

import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Provides the methods for a transaction with the key-value store.
 *
 * In general: users may assume that if maps (including sorted maps) keyed on byte[] are returned to the user,
 * they may be accessed via any byte array that is equivalent in terms of
 * {@link java.util.Arrays#equals(byte[], byte[])}.
 *
 * Throughout this class, methods that return structures that may not perform all of their loading upfront (batching
 * visitables, iterables, streams and futures, for instance) _must_ be used strictly within the scope of the
 * transaction. Concretely, this means that results must be retrieved before exiting the transaction - in the case of
 * streams and iterators, this means that you must collect the results before exiting; in the case of futures, you must
 * await all asynchronous calls before returning from the transaction.
 *
 * @see TransactionManager
 */
public interface Transaction {

    /**
     * Returns a mapping of rows to {@link RowResult}s within {@code tableRef} for the specified {@code rows}, loading
     * columns according to the provided {@link ColumnSelection}. Duplicate rows are permitted (but there will be just
     * one key-value pair for that row in the returned {@link NavigableMap}).
     *
     * The returned {@link NavigableMap} is sorted on the byte order of row keys; the ordering of the input parameter
     * {@code rows} is irrelevant.
     *
     * If there are rows with no cells matching the provided {@link ColumnSelection}, they will not be present in the
     * {@link Map#keySet()} of the output map at all. This accounts for writes and deletes done in this transaction:
     * a row written to in this transaction will be present, and a row which is deleted in this transaction will be
     * absent.
     *
     * @param tableRef table to load rows from
     * @param rows rows to be loaded
     * @param columnSelection columns to load from the given rows
     * @return a mapping of rows to the columns matching the provided column selection
     */
    @Idempotent
    NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection);

    /**
     * Returns a mapping of requested {@code rows} to corresponding columns from the queried table.
     * Only columns matching the provided predicate will be returned, and the single predicate provided applies across
     * all of the rows. Users should provide unique rows: behaviour is undefined if this is not the case.
     *
     * The returned {@link BatchingVisitable}s are guaranteed to return cells matching the predicate. These are sorted
     * by column on byte ordering.
     *
     * It is guaranteed that the {@link Map#keySet()} of the returned map has a corresponding element for each of the
     * input {@code rows}, even if there are rows where no columns match the predicate.
     *
     * Batching visitables must be used strictly within the scope of the transaction.
     *
     * @param tableRef table to load values from
     * @param rows unique rows to apply the column range selection to
     * @param columnRangeSelection range of columns and batch size to load for each of the rows provided
     * @return a mapping of rows to cells matching the predicate in the row, following the ordering outlined above
     */
    @Idempotent
    Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection);

    /**
     * Returns a single iterator over the cell-value pairs in {@code tableRef} for the specified {@code rows}, where the
     * columns fall within the provided {@link ColumnRangeSelection}. The single provided {@link ColumnRangeSelection}
     * applies to all of the rows.
     *
     * The returned iterator is guaranteed to return cell-value pairs in a lexicographic ordering over rows and columns
     * where rows are sorted according to the provided {@code rows} {@link Iterable} and then columns on byte ordering.
     * If the {@link Iterable} does not have a stable ordering (i.e. iteration order can change across iterators
     * returned) then the returned iterator is sorted lexicographically with columns sorted on byte ordering, but
     * the ordering of rows is undefined.
     *
     * Iterators must be used strictly within the scope of the transaction.
     *
     * @param tableRef table to load values from
     * @param rows unique rows to apply the column range selection to
     * @param columnRangeSelection range of columns to load for each of the rows provided
     * @param batchHint number of columns that should be loaded from the underlying database at once
     * @return an iterator over cell-value pairs, guaranteed to follow the ordering outlined above
     * @throws IllegalArgumentException if {@code rows} contains duplicates
     */
    @Idempotent
    Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, ColumnRangeSelection columnRangeSelection, int batchHint);

    /**
     * Returns a mapping of rows to {@link Iterator}s over cell-value pairs within {@code tableRef} for the specified
     * {@code rows}, where the columns fall within the provided {@link BatchColumnRangeSelection}. The single provided
     * {@link BatchColumnRangeSelection} applies to all of the rows.
     *
     * The returned iterators are guaranteed to return cells matching the predicate. These are sorted by column on
     * byte ordering.
     *
     * It is guaranteed that the {@link Map#keySet()} of the returned map has a corresponding element for each of the
     * input {@code rows}, even if there are rows where no columns match the predicate.
     *
     * Iterators must be used strictly within the scope of the transaction.
     *
     * @param tableRef table to load values from
     * @param rows unique rows to apply the column range selection to
     * @param columnRangeSelection range of columns and batch size to load for each of the rows provided
     * @return a mapping of rows to cells matching the predicate in the row, following the ordering outlined above
     * @throws IllegalArgumentException if {@code rows} contains duplicates
     */
    @Idempotent
    Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> getRowsColumnRangeIterator(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection);

    /**
     * Returns an iterator over cell-value pairs within {@code tableRef} for the specified {@code rows}, where the
     * columns fall within the provided  {@link BatchColumnRangeSelection}.The single provided
     * {@link BatchColumnRangeSelection} applies to all of the rows. The cells for a row appear exactly once even if
     * the same row is included multiple times in {@code rows}.
     *
     * The returned iterator is guaranteed to contain cells sorted first in lexicographical order of column on byte
     * ordering, then in order of row, where rows are sorted according to the provided {@code rows} {@link Iterable}.
     * If the {@link Iterable} does not have a stable ordering (i.e. iteration order can change across iterators
     * returned) then the returned iterator is sorted lexicographically with columns sorted on byte ordering, but the
     * ordering of rows is undefined. In case of duplicate rows, the ordering is based on the first occurrence of
     * the row.
     *
     * Iterators must be used strictly within the scope of the transaction.
     *
     * @param tableRef table to load values from
     * @param rows unique rows to apply column range selection to
     * @param batchColumnRangeSelection range of columns and batch size to load for all rows provided
     * @return an iterator over cell-value pairs, guaranteed to follow the ordering outlined above
     */
    @Idempotent
    Iterator<Map.Entry<Cell, byte[]>> getSortedColumns(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection);
    /**
     * Gets the values associated for each cell in {@code cells} from table specified by {@code tableRef}.
     *
     * @param tableRef the table from which to get the values
     * @param cells the cells for which we want to get the values
     * @return a {@link Map} from {@link Cell} to {@code byte[]} representing cell/value pairs
     */
    @Idempotent
    Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells);

    /**
     * Gets the values associated for each cell in {@code cells} from table specified by {@code tableRef}. It is not
     * guaranteed that the actual implementations are in fact asynchronous.
     *
     * The future must be used strictly within the scope of the transaction.
     *
     * @param tableRef the table from which to get the values
     * @param cells the cells for which we want to get the values
     * @return a {@link Map} from {@link Cell} to {@code byte[]} representing cell/value pairs
     */
    @Idempotent
    ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells);

    /**
     * Creates a visitable that scans the provided range.
     *
     * Batching visitables must be used strictly within the scope of the transaction.
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
     * Batching visitables must be used strictly within the scope of the transaction.
     *
     * @deprecated Should use either {@link #getRanges(TableReference, Iterable, int, BiFunction)} or
     * {@link #getRangesLazy(TableReference, Iterable)} to ensure you are using an appropriate level
     * of concurrency for your specific workflow.
     */
    @Idempotent
    @Deprecated
    Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests);

    /**
     * Creates unvisited visitables that scan the provided ranges and then applies the provided visitableProcessor
     * function with concurrency specified by the concurrencyLevel parameter.
     *
     * It is guaranteed that the range requests seen by the provided visitable processor are equal to the provided
     * iterable of range requests, though no guarantees are made on the order they are encountered in.
     *
     * Streams must be read within the scope of the transaction.
     */
    @Idempotent
    <T> Stream<T> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor);

    /**
     * Same as {@link #getRanges(TableReference, Iterable, int, BiFunction)} but uses the default concurrency
     * value specified by {@link KeyValueServiceConfig#defaultGetRangesConcurrency()}.
     */
    @Idempotent
    <T> Stream<T> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor);

    /**
     * Same as {@link #getRanges(TableReference, Iterable, int, BiFunction)}. However, additionally allows for the
     * specification of additional parameters specific to {@link GetRangesQuery}.
     */
    @Idempotent
    <T> Stream<T> getRanges(GetRangesQuery<T> getRangesQuery);

    /**
     * Returns visitibles that scan the provided ranges. This does no pre-fetching so visiting the resulting
     * visitibles will incur database reads on first access.
     *
     * Streams and visitables must be read within the scope of this transaction.
     */
    @Idempotent
    Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests);

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

    /**
     * Registers a callback that will be called after the AtlasDB client perceives a successful commit of a transaction.
     *
     * We guarantee that if the provided callback runs, the transaction has definitely committed successfully.
     * The converse is NOT true: it is possible that the callback is NOT called even if the transaction was successful.
     *
     * The semantics of callbacks are as follows:
     * <ul>
     *     <li>Callbacks must be registered before a transaction commits or aborts; registration of a callback once
     *     either of the above is true will throw an exception.</li>
     *     <li>Callbacks will run serially in the order they were registered.</li>
     *     <li>Exceptions thrown from any callback will be propagated immediately, and cause callbacks registered
     *     later to not run.</li>
     *     <li>Exceptions thrown from any callback will not change the result of the transaction.</li>
     * </ul>
     */
    void onSuccess(Runnable callback);

    /**
     * Disables read-write conflict checking for this table for the duration of this transaction only.
     *
     * This method should be called before any reads are done on this table.
     */
    @Idempotent
    default void disableReadWriteConflictChecking(TableReference tableRef) {
        throw new UnsupportedOperationException();
    }

    /**
     * Marks this table as involved in this transaction, without actually reading any rows.
     */
    @Idempotent
    default void markTableInvolved(TableReference tableRef) {
        throw new UnsupportedOperationException();
    }

    @Idempotent
    List<byte[]> getRowKeysInRange(
            TableReference tableRef, byte[] startRowInclusive, byte[] endRowInclusive, int maxResults);
}
