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

import com.palantir.exception.NotInitializedException;

public interface TransactionManager extends AutoCloseable {
    /**
     * Whether this transaction manager has established a connection to the backing store and timestamp/lock services,
     * and is ready to service transactions.
     *
     * If an attempt is made to execute a transaction when this method returns {@code false}, a
     * {@link NotInitializedException} will be thrown.
     *
     * This method is used for TransactionManagers that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other TransactionManagers can keep the default
     * implementation, and return true (they're trivially fully initialized).
     *
     * @return true if and only if the TransactionManager has been fully initialized
     */
    default boolean isInitialized() {
        return true;
    }

    /**
     * Runs the given {@link TransactionTask}. If the task completes successfully
     * and does not call {@link Transaction#commit()} or {@link Transaction#abort()},
     * {@link Transaction#commit()} is called automatically.
     * <p>
     * The task is re-run if a conflict is detected (if a {@link TransactionConflictException} is thrown)
     * <p>
     * If <code>runTaskWithRetry</code> completes successfully (no exception is thrown)
     * and the task did not explicitly
     * abort the transaction, then the transaction was successfully committed.
     * If an exception is thrown by the <code>TransactionTask</code> and the task did not call
     * {@link Transaction#commit()}, then the transaction will be rolled back.
     *<p>
     * NOTE: If an exception is thrown by {@link Transaction#commit()}, the transaction might have
     * been committed.
     * <p>
     * It is important that the {@link TransactionTask} does not modify any of its input state
     * in any non-idempotent way.  If this task gets retried, and if you modified your input, then the
     * second try might not do the right thing.  For example: if you are passed a list of objects
     * and at the end of the {@link TransactionTask}, you clear the list.  If your task gets retried
     * it will have no work to do, because the list was cleared.
     *
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E;

    /**
     * {@link #runTaskWithRetry(TransactionTask)} should be preferred over
     * {@link #runTaskThrowOnConflict(TransactionTask)}.
     * This method should be used unless {@link #runTaskWithRetry(TransactionTask)} cannot be used because the arguments
     * passed are not immutable and will be modified by the transaction so doing automatic retry is unsafe.
     *
     * Runs the given {@link TransactionTask}. If the task completes successfully
     * and does not call {@link Transaction#commit()} or {@link Transaction#abort()},
     * {@link Transaction#commit()} is called automatically.
     * <p>
     * If <code>runTaskThrowOnConflict()</code> completes successfully (no exception is thrown)
     * and the task did not explicitly
     * abort the transaction, then the transaction was successfully committed.
     * If an exception is thrown by the <code>TransactionTask</code> and the task did not call
     * {@link Transaction#commit()}, then the transaction will be rolled back.
     *<p>
     * NOTE: If an exception is thrown by {@link Transaction#commit()}, the transaction might have
     * been committed.
     *
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws TransactionConflictException if a write-write conflict occurs
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException;

    /**
     * This will open and run a read-only transaction. Read-only transactions are similar to other
     * transactions, but will throw if any write operations are called. Furthermore, they often
     * make fewer network calls than their read/write counterparts so should be used where possible.
     *
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E;

    /**
     * Most AtlasDB TransactionManagers will provide {@link Transaction} objects that have less than full
     * serializability. The most common is snapshot isolation (SI).  SI has a start timestamp and a commit timestamp
     * and an open transaction can only read values that were committed before its start timestamp.
     * <p>
     * This method will return a timestamp that is before any uncommited/aborted open start timestamps.
     * <p>
     * Subsequent calls to this method will always be monotonically increasing for a single client.
     * <p>
     * You are only allowed to open historic/read-only transactions at a timestamp less than or equal to the
     * immutableTimestamp
     *
     * @return the latest timestamp for which there are no open transactions
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    long getImmutableTimestamp();

    /**
     * Provides a {@link KeyValueServiceStatus}, indicating the current availability of the key value store.
     * This can be used to infer product health - in the usual, conservative case, products can call
     * {@link KeyValueServiceStatus#isHealthy()}, which returns true only if all KVS nodes are up.
     * <p>
     * Products that use AtlasDB only for reads and writes (no schema mutations or deletes, including having sweep and
     * scrub disabled) can also treat {@link KeyValueServiceStatus#HEALTHY_BUT_NO_SCHEMA_MUTATIONS_OR_DELETES} as
     * healthy.
     * <p>
     * This call must be implemented so that it completes synchronously.
     */
    KeyValueServiceStatus getKeyValueServiceStatus();

    /**
     * Returns the timestamp that is before any open start timestamps. This is different from the immutable
     * timestamp, because it takes into account open read-only transactions. There is likely to be NO
     * running transactions open at a timestamp before the unreadable timestamp, however this cannot be guaranteed.
     * <p>
     * When using the unreadable timestamp for cleanup it is important to leave a sentinel value behind at a negative
     * timestamp so any transaction that is open will fail out if reading a value that is cleaned up instead of just
     * getting back no data. This is needed to ensure that all transactions either produce correct values or fail.
     * It is not an option to return incorrect data.
     *
     * @return the timestamp that is before any open start timestamps
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    long getUnreadableTimestamp();

    /**
     * Clear the timestamp cache. This is mostly useful for tests that perform operations that would invalidate
     * the cache, although this can also be used to free up some memory.
     */
    void clearTimestampCache();

    @Override
    void close();
}
