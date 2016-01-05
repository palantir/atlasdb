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
package com.palantir.atlasdb.transaction.api;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.BatchingVisitable;

/**
 * Provides the methods for a transaction with the key-value store.
 * @see TransactionManager
 */
public interface Transaction {

    @Idempotent
    SortedMap<byte[], RowResult<byte[]>> getRows(String tableName, Iterable<byte[]> rows,
                                                 ColumnSelection columnSelection);

    @Idempotent
    Map<Cell, byte[]> get(String tableName, Set<Cell> cells);

    /**
     * Creates a visitable that scans the provided range.
     *
     * @param tableName the table to scan
     * @param rangeRequest the range of rows and columns to scan
     * @return an array of <code>RowResult</code> objects representing the range
     */
    @Idempotent
    BatchingVisitable<RowResult<byte[]>> getRange(String tableName, RangeRequest rangeRequest);

    /**
     * Creates a visitable that scans the provided range.
     * <p>
     * To get good performance out of this method, it is important to specify
     * the batch hint in each RangeRequest. If this isn't done then this method may do more work
     * than you need and will be slower than it needs to be.  If the batchHint isn't specified it
     * will default to 1 for the first page in each range.
     */
    @Idempotent
    Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(String tableName, Iterable<RangeRequest> rangeRequests);

    /**
     * Puts values into the key-value store. If you put a null or the empty byte array, then
     * this is treated like a delete to the store.
     * @param tableName the table into which to put the values
     * @param values the values to append to the table
     */
    @Idempotent
    void put(String tableName, Map<Cell, byte[]> values);

    /**
     * Deletes values from the key-value store.
     * @param tableName the table from which to delete the values
     * @param keys the set of cells to delete from the store
     */
    @Idempotent
    void delete(String tableName, Set<Cell> keys);

    @Idempotent
    TransactionType getTransactionType();

    @Idempotent
    void setTransactionType(TransactionType transactionType);

    enum TransactionType {
        DEFAULT,

        /**
         * Hard delete transactions are different from regular transactions because they
         * must queue cells for "scrubbing" (i.e. not just write a value at the latest
         * timestamp, but also clean up values at older timestamps) on every cell that's
         * modified or deleted
         */
        HARD_DELETE,

        /**
         * In addition to queuing cells for "scrubbing", we also:
         * - (a) Scrub earlier than we would have otherwise, even at the cost of possibly
         *       causing open transactions to abort, and
         * - (b) Block until the scrub is complete
         */
        AGGRESSIVE_HARD_DELETE;
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
     * @return <code>true</code> if <code>abort()</code> has been called, otherwise <code>false</code>
     */
    @Idempotent
    boolean isAborted();

    /**
     * @return <code>true</code> if neither <code>commit()</code> or <code>abort()</code> have been called, otherwise <code>false</code>
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
    void useTable(String tableName, ConstraintCheckable table);
}
