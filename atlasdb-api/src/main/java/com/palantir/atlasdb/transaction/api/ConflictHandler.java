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

public enum ConflictHandler {
    /**
     * If two transactions concurrently write to the same cell the one that has started later will win.  We
     * are ignoring write conflicts in this case.
     */
    IGNORE_ALL(false, false, false, false),

    /**
     * If two transactions concurrently write to the same cell the one that commits later will
     * throw a {@link TransactionConflictException} and should have details about how this happened.
     */
    RETRY_ON_WRITE_WRITE(false, true, true, false),

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
    RETRY_ON_VALUE_CHANGED(false, true, true, false),

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
    SERIALIZABLE(false, true, true, true),

    /**
     * Same as {@link RETRY_ON_WRITE_WRITE}, but checks for conflicts by locking cells during commit instead
     * of locking rows. Cell locks are more fine-grained, so this will produce less contention at the expense
     * of requiring more locks to be acquired.
     */
    RETRY_ON_WRITE_WRITE_CELL(true, false, true, false),

    /**
     * Same as {@link SERIALIZABLE}, but checks for conflicts by locking cells during commit instead
     * of locking rows. Cell locks are more fine-grained, so this will produce less contention at the expense
     * of requiring more locks to be acquired.
     */
    SERIALIZABLE_CELL(true, false, true, true),

    /**
     * This conflict handler is designed to be used by index tables. As any write/write conflict on an index table
     * will necessarily also be a write/write conflict on base table, we do not need to check write/write conflicts
     * on index tables. Read/write conflicts should still need to be checked, since we do not need to read the index
     * table with the main table.
     * <p>
     * This conflict does not lock, because it's assumed that a semantically equivalent lock is taken out on
     * the base table.
     */
    SERIALIZABLE_INDEX(false, false, false, true),

    /**
     * This conflict handler is designed to be used for migrating a table from SERIALIZABLE to SERIALIZABLE_CELL or
     * SERIALIZABLE_INDEX conflict handler, or vice versa without requiring a simultaneous shutdown of all clients.
     * <p>
     * Migration should be handled in two steps. First all the nodes should migrate to SERIALIZABLE_LOCK_LEVEL_MIGRATION
     * with a rolling upgrade. Once this is complete, then in the second step migration to target conflict handler from
     * SERIALIZABLE_LOCK_LEVEL_MIGRATION can be performed, in a rolling fashion.
     * <p>
     * Current implementation of lock service permits a client to lock a cell after locking the row that cell
     * belongs to. In a scenario where we migrate table A to SERIALIZABLE_CELL from SERIALIZABLE, and conduct a rolling
     * upgrade, if client_1 is on the newer version, it will try to acquire cell locks for its transaction t1; while
     * client_2 with an older version, acquiring row locks for its transaction t2. If t1 and t2 are modifying the same
     * cell then correctness issues may occur as there is no way for t1 to be aware of t2. Locking on both cell and row
     * level prevents this issue.
     */
    SERIALIZABLE_LOCK_LEVEL_MIGRATION(true, true, true, true),

    /**
     * This conflict handler is analogous to SERIALIZABLE_LOCK_LEVEL_MIGRATION, but for migrating between
     * RETRY_ON_WRITE_WRITE and RETRY_ON_WRITE_WRITE_CELL.
     */
    RETRY_ON_WRITE_WRITE_MIGRATION(true, true, true, false);

    private final boolean lockCellsForConflicts;
    private final boolean lockRowsForConflicts;

    private final boolean checkWriteWriteConflicts;
    private final boolean checkReadWriteConflicts;

    ConflictHandler(
            boolean lockCellsForConflicts,
            boolean lockRowsForConflicts,
            boolean checkWriteWriteConflicts,
            boolean checkReadWriteConflicts) {

        this.lockCellsForConflicts = lockCellsForConflicts;
        this.lockRowsForConflicts = lockRowsForConflicts;

        this.checkWriteWriteConflicts = checkWriteWriteConflicts;
        this.checkReadWriteConflicts = checkReadWriteConflicts;
    }

    public boolean lockCellsForConflicts() {
        return lockCellsForConflicts;
    }

    public boolean lockRowsForConflicts() {
        return lockRowsForConflicts;
    }

    public boolean checkWriteWriteConflicts() {
        return checkWriteWriteConflicts;
    }

    public boolean checkReadWriteConflicts() {
        return checkReadWriteConflicts;
    }
}
