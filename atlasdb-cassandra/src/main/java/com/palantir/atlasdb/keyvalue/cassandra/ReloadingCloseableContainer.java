/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.common.base.CallableCheckedException;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.RunnableCheckedException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import com.palantir.util.Pair;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;

/**
 * Guarantees:
 * 1. A resource will never be closed while that resource is in scope of an invocation of {@link #runWithResource}.
 * 2. After a refresh, the new resource created must reflect the refreshed changes and not be closed.
 * 3. After a refresh, there exists a time T when all resources except the latest resource is closed.
 * 4. After {@link #close} has returned, the latest resource must be closed, and no further resources will be created.
 *    (With 3, there exists a time T after {@link #close} has been called where all the resources are closed)
 * 5. After {@link #close} has returned, all subsequent calls to runWith will fail.
 *
 * <b>When should you use this class?</b>
 * You should use this when:
 * 1. A resource is created from some live reloaded value, and new resources should be created when said value is
 * updated.
 * 2. The cost of retrying if the underlying resource is closed underneath you in an operation is expensive / not
 * possible, OR there's risk of starvation (e.g with Cassandra, the topology changing in quick succession could
 * starve existing connections if they're closed on every change)
 * 3. It's acceptable for inflight consumers to use a stale resource (e.g at a temporary performance penalty for
 * redirects)
 *
 */
public final class ReloadingCloseableContainer<T extends Closeable> implements Closeable {
    private static final SafeLogger log = SafeLoggerFactory.get(ReloadingCloseableContainer.class);

    // This lock is marked fair to prevent starving #close. After close is called, no further #runWith
    // calls will acquire the resource lock.
    private final ReadWriteLock isClosedLock = new ReentrantReadWriteLock(true);
    private final AtomicReference<Optional<Pair<ReadWriteLock, T>>> currentResourceWithLock =
            new AtomicReference<>(Optional.empty());

    private final Refreshable<Pair<ReadWriteLock, T>> refreshableResourceWithLock;
    private final Disposable refreshableSubscriptionDisposable;

    @GuardedBy("isClosedLock")
    private volatile boolean isClosed;

    public static <R, T extends Closeable> ReloadingCloseableContainer<T> of(
            Function<R, T> resourceFactory, Refreshable<R> refreshableFactoryParameter) {
        return new ReloadingCloseableContainer<>(resourceFactory, refreshableFactoryParameter);
    }

    private <R> ReloadingCloseableContainer(
            Function<R, T> resourceFactory, Refreshable<R> refreshableFactoryParameter) {
        isClosed = false;
        refreshableResourceWithLock = refreshableFactoryParameter.map(factoryParameter -> runIfNotClosed(() -> {
            T resource = resourceFactory.apply(factoryParameter);
            // This lock does not need to be fair, since the isClosed lock is fair (preventing #close from being
            // starved) and since subscribe happens after a map (and thus a new resource is created before attempting
            // to close the old), there will not be any more readers of a resource about to be closed.
            ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
            return new Pair<>(readWriteLock, resource);
        }));

        // The old resource is closed in a subscriber (rather than map) so that attempting to close the old resource
        // happens after the refreshable was updated to use a new resource. This avoids a situation where
        // a #runWithResource call using a resource that is closed, violating guarantee 1 (between the close lock
        // being released and map completing the update).

        // Furthermore, we don't want updates to have lock contention with a #runWithResource call:

        // Consider the case of a long-running #runWithResource call. The current resource lock is held.
        // If we attempt to close the resource in the map, then this must occur before refreshableResourceWithLock is
        // updated (assuming we don't run close asynchronously). As close holds the write lock for the
        // resource, subsequent #runWithResource calls would now block on acquiring the lock. This would be doubly
        // damaging if the resource lock was fair, as now #runWithResource calls are blocked until all calls prior to
        // closing the resource have finished.

        // To avoid this, we must not block an update with close. This can either be done with another thread, or
        // simply using a subscriber on the updated resource.
        refreshableSubscriptionDisposable = refreshableResourceWithLock.subscribe(resourceWithLock -> {
            Optional<Pair<ReadWriteLock, T>> maybeResourceWithLockToClose =
                    currentResourceWithLock.getAndSet(Optional.of(resourceWithLock));
            maybeResourceWithLockToClose.ifPresent(this::closeResourceAndLock);
        });
    }

    private void closeResourceAndLock(Pair<ReadWriteLock, T> resourceWithLockToClose) {
        Lock writeLock = resourceWithLockToClose.getLhSide().writeLock();
        runWithLock(writeLock, () -> {
            try {
                T resourceToClose = resourceWithLockToClose.getRhSide();
                resourceToClose.close();
            } catch (IOException e) {
                log.warn("Failed to close resource. This may result in a resource leak", e);
            }
        });
    }

    /***
     * Closes the container and the latest created resource.
     * After this method has returned, no further resources will be created in this container, and all subsequent
     * {@link #runWithResource} invocations will fail with a {@link SafeIllegalStateException}.
     *
     * Note: This method does <b>block</b> on any inflight {@link #runWithResource} invocations.
     *
     * Any {@link IOException} from closing the underlying resource will not be propagated.
     *
     * See {@link Closeable#close()}.
     */
    @Override
    public void close() {
        Lock isClosedWriteLock = isClosedLock.writeLock();
        runWithLock(isClosedWriteLock, () -> {
            isClosed = true;
            currentResourceWithLock.get().ifPresent(this::closeResourceAndLock);
            refreshableSubscriptionDisposable.dispose();
        });
    }

    /***
     * Returns the result of `task` applied with the current resource.
     * This resource is guaranteed not to be closed within scope of this function <i>by this container</i>.
     * There are no guarantees on the resource being closed externally e.g. if there is a shared reference, nor if the
     * resource is closed within scope by the task itself.
     *
     * If an update to the refreshable occurs during an invocation, any <i>inflight</i> invocations will <i>not</i> use
     * the new resource.
     *
     * <b>Do not</b> block on close or update the refreshable within the scope of runWith. Doing so will result in a
     * deadlock.
     *
     * There are no timeouts applied to these invocations. If required, consider using the
     * {@link com.google.common.util.concurrent.TimeLimiter} utilities. Long-running invocations will block close.
     *
     * @param task The function to execute using the current resource. This function may throw a checked exception,
     *             and this exception will propagate.
     * @param <V> The return type of the task
     * @param <K> The checked exception type, if any. If multiple checked exceptions are thrown, this will resolve to
     *          the lowest common ancestor, which may be {@link Exception}
     * @return The result of `task`
     * @throws K Any checked exceptions thrown by the underlying task.
     */
    public <V, K extends Exception> V runWithResource(FunctionCheckedException<T, V, K> task) throws K {
        return runIfNotClosed(() -> {
            Pair<ReadWriteLock, T> resourceWithLock = refreshableResourceWithLock.get();
            Lock readLock = resourceWithLock.getLhSide().readLock();
            return runWithLock(readLock, () -> task.apply(resourceWithLock.getRhSide()));
        });
    }

    /**
     * Utility method for {@link #runWithResource} for tasks that have no return value.
     *
     * See {@link #runWithResource} for semantics, guarantees and conditions.
     *
     */
    public <K extends Exception> void executeWithResource(ConsumerCheckedException<T, K> task) throws K {
        runWithResource(resource -> {
            task.accept(resource);
            return null;
        });
    }

    private static <V, K extends Exception> V runWithLock(Lock lock, CallableCheckedException<V, K> task) throws K {
        lock.lock();
        try {
            return task.call();
        } finally {
            lock.unlock();
        }
    }

    private static <K extends Exception> void runWithLock(Lock lock, RunnableCheckedException<K> task) throws K {
        runWithLock(lock, () -> {
            task.run();
            return null;
        });
    }

    private <V, K extends Exception> V runIfNotClosed(CallableCheckedException<V, K> task) throws K {
        Lock readLock = isClosedLock.readLock();
        return runWithLock(readLock, () -> {
            if (isClosed) {
                // This exception bubbles up to the caller of #runWithResource, and blocks an update for the
                // Refreshable#map call.
                throw new SafeIllegalStateException("Attempted to use resource after the container was closed.");
            }
            return task.call();
        });
    }

    @FunctionalInterface
    public interface ConsumerCheckedException<T, K extends Exception> {
        void accept(T input) throws K;
    }
}
