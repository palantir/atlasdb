/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

public enum ConflictHandler {
    /**
     * If two transactions write to the same cell the one that was committed later will win.  We
     * are ignoring write conflicts in this case.
     */
    IGNORE_ALL(false, false, false),

    /**
     * If two transactions concurrently write to the same cell the one that commits later will
     * throw a {@link TransactionConflictException} and should have details about how this happened.
     */
    RETRY_ON_WRITE_WRITE(false, true, false),

    /**
     * If a transaction writes a new value to a Cell (different than the value read by that
     * transaction), then it will throw if another concurrent transaction wrote to this cell
     * concurrently and committed first.
     * <p>
     * If a transaction is writing the same value to a Cell (same as was read by that transaction),
     * then it will throw in the case where a concurrent transaction has written a different
     * value to this Cell.  A concurrent transaction that just writes the same value will not cause
     * throwing of a {@link TransactionConflictException}, because we want to allow many
     * transactions to do touches concurrently, but they should conflict with a change to this cell.
     * <p>
     * Note: Transactions that only touch a cell still need to grab a write lock for it during commit.
     * For a cell that is frequently touched and infrequently updated (i.e., a read-write lock design
     * with mostly reads), it may make sense to replicate the cell, have each reader touch one
     * replica, and have writers update all replicas.
     * <p>
     * Note: This ConflictHandler has the ABA problem (https://en.wikipedia.org/wiki/ABA_problem).  The easiest
     * way to fix this is to have the value always increase.
     */
    RETRY_ON_VALUE_CHANGED(false, true, false),

    /**
     * This conflict type does everything that {@link #RETRY_ON_WRITE_WRITE} does but additionally detects
     * and retries on read/write conflict.
     * <p>
     * A read/write conflict happens whenever you read a value but a concurrent transaction has written a
     * new differing value and committed before your transaction commits.  This logic only applies to write
     * transactions.  Read transactions are already serializable because they read from a consistent point in time.
     * <p>
     * This should not be used on tables that are expected to have tons of reads because we must keep the whole read
     * set in memory so we can compare it at commit time to ensure that everything we read has not changed.
     */
    SERIALIZABLE(false, true, true),

    /**
     * Same as {@link RETRY_ON_WRITE_WRITE}, but checks for conflicts by locking cells during commit instead
     * of locking rows. Cell locks are more fine-grained, so this will produce less contention at the expense
     * of requiring more locks to be acquired.
     */
    RETRY_ON_WRITE_WRITE_CELL(true, true, false),

    /**
     * Same as {@link SERIALIZABLE}, but checks for conflicts by locking cells during commit instead
     * of locking rows. Cell locks are more fine-grained, so this will produce less contention at the expense
     * of requiring more locks to be acquired.
     */
    SERIALIZABLE_CELL(true, true, true),

    /**
     * This conflict handler is designed to be used by index tables. As any write/write conflict on an index table
     * will necessarily also be a write/write conflict on base table, we do not need to check write/write conflicts
     * on index tables. Read/write conflicts should still need to be checked, since we do not need to read the index
     * table with the main table.
     * <p>
     * This conflict handler locks at cell level. As index tables use dynamic columns, locking at row level would
     * result in higher contention.
     * <p>
     */
    SERIALIZABLE_INDEX(true, false, true);

    private final boolean checkWriteWriteConflicts;
    private final boolean checkReadWriteConflicts;

    private final boolean lockCellsForConflicts;

    ConflictHandler(boolean lockCellsForConflicts, boolean checkWriteWriteConflicts, boolean checkReadWriteConflicts) {
        this.lockCellsForConflicts = lockCellsForConflicts;
        this.checkWriteWriteConflicts = checkWriteWriteConflicts;
        this.checkReadWriteConflicts = checkReadWriteConflicts;
    }

    public boolean lockCellsForConflicts() {
        return lockCellsForConflicts;
    }

    public boolean lockRowsForConflicts() {
        return !lockCellsForConflicts && checkConflicts();
    }

    public boolean checkWriteWriteConflicts() {
        return checkWriteWriteConflicts;
    }

    public boolean checkReadWriteConflicts() {
        return checkReadWriteConflicts;
    }

    private boolean checkConflicts() {
        return checkReadWriteConflicts || checkWriteWriteConflicts;
    }
}
