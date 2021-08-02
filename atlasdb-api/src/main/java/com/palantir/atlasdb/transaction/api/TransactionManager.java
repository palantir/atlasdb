/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManager;
import com.palantir.atlasdb.metrics.Timed;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.processors.AutoDelegate;
import com.palantir.processors.DoDelegate;
import com.palantir.processors.DoNotDelegate;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.util.List;
import java.util.function.Supplier;
import org.immutables.value.Value;

@AutoDelegate
public interface TransactionManager extends AutoCloseable {
    /**
     * Whether this transaction manager has established a connection to the backing store and timestamp/lock services,
     * and is ready to service transactions.
     *
     * If an attempt is made to execute a transaction when this method returns {@code false}, a
     * {@link NotInitializedException} will be thrown.
     *
     * This method is used for TransactionManagers that can be initializeppd asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other TransactionManagers can keep the default
     * implementation, and return true (they're trivially fully initialized).
     *
     * @return true if and only if the TransactionManager has been fully initialized
     */
    @DoDelegate
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
    @Timed
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
    @Timed
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
    @Timed
    <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E;

    /**
     * This method is basically the same as {@link #runTaskWithRetry(TransactionTask)} but it will
     * acquire locks right before the transaction is created and release them after the task is complete.
     * <p>
     * The created transaction will not commit successfully if these locks are invalid by the time commit is run.
     *
     * @param lockSupplier supplier for the lock request
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws LockAcquisitionException If the supplied lock request is not successfully acquired.
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    @Timed
    <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> lockSupplier, LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException, LockAcquisitionException;

    /**
     * This is the same as {@link #runTaskWithLocksWithRetry(Supplier, LockAwareTransactionTask)}, but instead
     * takes in a Guava supplier. This is deprecated in favour of the aforementioned method.
     *
     * @deprecated use {@link #runTaskWithLocksWithRetry(Supplier, LockAwareTransactionTask)} instead.
     * @see #runTaskWithLocksWithRetry(Supplier, LockAwareTransactionTask)
     */
    @DoNotDelegate
    @Deprecated
    @Timed
    default <T, E extends Exception> T runTaskWithLocksWithRetry(
            com.google.common.base.Supplier<LockRequest> guavaSupplier, LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException, LockAcquisitionException {
        Supplier<LockRequest> javaSupplier = guavaSupplier;
        return runTaskWithLocksWithRetry(javaSupplier, task);
    }

    /**
     * This method is the same as {@link #runTaskWithLocksWithRetry(Supplier, LockAwareTransactionTask)}
     * but it will also ensure that the existing lock tokens passed are still valid before committing.
     *
     * @param lockTokens lock tokens to acquire while transaction executes
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws LockAcquisitionException If the supplied lock request is not successfully acquired.
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    @Timed
    <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException, LockAcquisitionException;

    /**
     * This is the same as {@link #runTaskWithLocksWithRetry(Iterable, Supplier, LockAwareTransactionTask)}, but instead
     * takes in a Guava supplier. This is deprecated in favour of the aforementioned method.
     *
     * @deprecated use {@link #runTaskWithLocksWithRetry(Iterable, Supplier, LockAwareTransactionTask)} instead.
     * @see #runTaskWithLocksWithRetry(Iterable, Supplier, LockAwareTransactionTask)
     */
    @DoNotDelegate
    @Deprecated
    @Timed
    default <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            com.google.common.base.Supplier<LockRequest> guavaSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException, LockAcquisitionException {
        Supplier<LockRequest> javaSupplier = guavaSupplier;
        return runTaskWithLocksWithRetry(lockTokens, javaSupplier, task);
    }

    /**
     * This method is the same as {@link #runTaskThrowOnConflict(TransactionTask)} except the created transaction
     * will not commit successfully if these locks are invalid by the time commit is run.
     *
     * @param lockTokens lock tokens to refresh while transaction executes
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    @Timed
    <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> lockTokens, LockAwareTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException;

    /**
     * This method is basically the same as {@link #runTaskWithRetry(TransactionTask)}, but it will
     * acquire a {@link PreCommitCondition} right before the transaction is created and check it
     * immediately before the transaction commits.
     * <p>
     * The created transaction will not commit successfully if the check fails.
     *
     * @param conditionSupplier supplier for the condition
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    @Timed
    <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<C> conditionSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E;

    /**
     * This is the same as {@link #runTaskWithConditionWithRetry(Supplier, ConditionAwareTransactionTask)}, but instead
     * takes in a Guava supplier. This is deprecated in favour of the aforementioned method.
     *
     * @deprecated use {@link #runTaskWithConditionWithRetry(Supplier, ConditionAwareTransactionTask)} instead.
     * @see #runTaskWithConditionWithRetry(Supplier, ConditionAwareTransactionTask)
     */
    @DoNotDelegate
    @Deprecated
    @Timed
    default <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            com.google.common.base.Supplier<C> guavaSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E {
        Supplier<C> javaSupplier = guavaSupplier;
        return runTaskWithConditionWithRetry(javaSupplier, task);
    }

    /**
     * This method is basically the same as {@link #runTaskThrowOnConflict(TransactionTask)}, but it takes
     * a {@link PreCommitCondition} and checks it immediately before the transaction commits.
     * <p>
     * The created transaction will not commit successfully if the check fails.
     *
     * @param condition condition associated with the transaction
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    @Timed
    <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(
            C condition, ConditionAwareTransactionTask<T, C, E> task) throws E, TransactionFailedRetriableException;

    /**
     * This method is basically the same as {@link #runTaskReadOnly(TransactionTask)}, but it takes
     * a {@link PreCommitCondition} and checks it for validity before executing reads.
     * <p>
     * The created transaction will fail if the check is no longer valid after fetching the read
     * timestamp.
     *
     * @param condition condition associated with the transaction
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    @Timed
    <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionReadOnly(
            C condition, ConditionAwareTransactionTask<T, C, E> task) throws E;

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
     * Returns the lock service used by this transaction manager.
     *
     * @return the lock service for this transaction manager
     */
    LockService getLockService();

    /**
     * Returns the timelock service used by this transaction manager.
     *
     * @return the timelock service for this transaction manager
     */
    TimelockService getTimelockService();

    LockWatchManager getLockWatchManager();

    /**
     * Returns the timestamp service used by this transaction manager.
     *
     * @return the timestamp service for this transaction manager
     */
    TimestampService getTimestampService();

    /**
     * The timestamp management service is used by libraries providing additional functionality
     * around AtlasDB. End-user clients probably should not require it.
     *
     * @return the timestamp management service for this transaction manager
     */
    TimestampManagementService getTimestampManagementService();

    /**
     * The transaction service is used by libraries providing additional functionality around AtlasDB.
     * End-user clients probably should not require it.
     * Abuse of the transaction service, especially involving putting new records in, may result in severe and
     * irrecoverable data corruption.
     *
     * @return the transaction service for this transaction manager
     */
    TransactionService getTransactionService();

    /**
     * Returns the cleaner used by this transaction manager.
     *
     * @return the cleaner for this transaction manager
     */
    Cleaner getCleaner();

    /**
     * Returns the KVS used by this transaction manager. In general, this should not be used by clients, as
     * direct reads and writes to the KVS will bypass the Atlas transaction protocol.
     *
     * @return the key value service for this transaction manager
     */
    KeyValueService getKeyValueService();

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
     * Provides a {@link TimelockServiceStatus}, indicating the current availability of the timelock service.
     * This can be used to infer product health - in the usual, conservative case, products can call
     * {@link TimelockServiceStatus#isHealthy()}, which returns true only a healthy connection to timelock
     * service is established.
     *
     * @return status of the timelock service
     */
    TimelockServiceStatus getTimelockServiceStatus();

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
    @Timed
    long getUnreadableTimestamp();

    /**
     * Clear the timestamp cache. This is mostly useful for tests that perform operations that would invalidate
     * the cache, although this can also be used to free up some memory.
     */
    void clearTimestampCache();

    /**
     * Registers a Runnable that will be run after the transaction manager is closed.
     *
     * Concurrency: If this method races with close(), then closingCallback may not be called.
     */
    void registerClosingCallback(Runnable closingCallback);

    /**
     * This method can be used for direct control over the lifecycle of a batch of transactions. For example, if the
     * work done in each given transaction is interactive and cannot be expressed as a {@link TransactionTask} ahead of
     * time, this method allows for a long lived transaction object. For any data read or written to the transaction to
     * be valid, the transaction must be committed by calling {@link OpenTransaction#finish(TransactionTask)} to
     * also perform additional cleanup. Note that this does not clean up the pre commit condition associated with that
     * task. The order of transactions returned corresponds with the pre commit conditions passed in, however there are
     * no guarantees on the ordering of transactions returned with respect to their start timestamp.
     *
     * @return a batch of transactions with associated immutable timestamp locks
     * @deprecated Similar functionality will exist, but this method is likely to change in the future
     */
    @Deprecated
    @Timed
    List<OpenTransaction> startTransactions(List<? extends PreCommitCondition> condition);

    /**
     * See {@link #startTransactions(List)}. This starts *one* transaction.
     */
    StartedTransactionContext startTransaction();

    /**
     * Here be dragons!
     *
     * TODO (jkong): Hmm.
     */
    Transaction createTransactionWithDependentContext(long dependentTimestamp,
            LockImmutableTimestampResponse dependentImmutableTimestamp);

    /**
     * Frees resources used by this TransactionManager, and invokes any callbacks registered to run on close.
     * This includes the cleaner, the key value service (and attendant thread pools), and possibly the lock service.
     *
     * All callbacks will execute in the reverse order of registration, regardless
     * of any exceptions thrown. If any exceptions occur, they will be collected and rethrown as a new exception
     * with any exceptions that occurred set as suppressed exceptions.
     *
     * Concurrency: If this method races with registerClosingCallback(closingCallback), then closingCallback
     * may be called (but is not necessarily called). Callbacks registered before the invocation of close() are
     * guaranteed to be executed.
     */
    @Override
    void close();
}
