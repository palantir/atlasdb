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

package com.palantir.atlasdb.cassandra;

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;

/**
 * This container creates new resources when the underlying refreshable changes.
 * After a refresh, previous resources are closed to avoid resource leaks.
 *
 * There is no guarantee that a resource provided via {@link #get} will <i>not</i> be closed whilst in use.
 * Consumers should be resilient to any uses after the resource has been closed, and where necessary, retry.
 *
 * After {@link #close()} has returned, no further resources will be created, and all managed resources will be
 * closed.
 */
public final class ReloadingCloseableContainer<T extends AutoCloseable> implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(ReloadingCloseableContainer.class);

    private final AtomicReference<Optional<T>> currentResource;
    private final Refreshable<T> refreshableResource;
    private final Disposable refreshableSubscriptionDisposable;
    private final ReadWriteLock isClosedLock;

    @GuardedBy("isClosedLock")
    private volatile boolean isClosed;

    private <K> ReloadingCloseableContainer(Refreshable<K> refreshableParameter, Function<K, T> factory) {
        this.isClosed = false;
        this.isClosedLock = new ReentrantReadWriteLock(true);
        this.currentResource = new AtomicReference<>(Optional.empty());
        this.refreshableResource = refreshableParameter.map(factoryArg -> createNewResource(factoryArg, factory));

        this.refreshableSubscriptionDisposable = refreshableResource.subscribe(resource -> {
            Optional<T> maybeResourceToClose = currentResource.getAndSet(Optional.of(resource));
            maybeResourceToClose.ifPresent(this::shutdownResource);
        });
    }

    public static <T extends AutoCloseable, K> ReloadingCloseableContainer<T> of(
            Refreshable<K> refreshableParameter, Function<K, T> factory) {
        return new ReloadingCloseableContainer<>(refreshableParameter, factory);
    }

    private <K> T createNewResource(K factoryArg, Function<K, T> factory) {
        return runIfNotClosed(
                () -> factory.apply(factoryArg), "Attempted to create a new resource after the container was closed.");
    }

    /**
     * A lock is taken out to ensure no new resources are created after retrieving the current stored
     * resource to close. By doing so, we avoid closing a cluster and subsequently creating a new one that is
     * never closed.
     */
    @Override
    public void close() {
        runWithIsClosedWriteLock(() -> {
            isClosed = true;
            refreshableSubscriptionDisposable.dispose();
            currentResource.get().ifPresent(this::shutdownResource);
        });
    }

    /**
     * Gets the latest resource that reflects any changes in the refreshable, provided {@link #close()} has not
     * been called.
     *
     * The resource returned will be closed after {@link #close} is called, or the refreshable is refreshed, even
     * if the resource is in active use.
     *
     * @throws SafeIllegalStateException if the container was closed prior to getting a resource.
     */
    public T get() {
        return runIfNotClosed(refreshableResource, "Attempted to get a resource after the container was closed");
    }

    public boolean isClosed() {
        return isClosed;
    }

    private <K> K runIfNotClosed(Supplier<K> supplier, @CompileTimeConstant String ifClosedExceptionMessage) {
        if (!isClosed) {
            return runWithIsClosedReadLock(() -> {
                if (isClosed) {
                    throw new SafeIllegalStateException(ifClosedExceptionMessage);
                } else {
                    return supplier.get();
                }
            });
        } else {
            throw new SafeIllegalStateException(ifClosedExceptionMessage);
        }
    }

    private <K> K runWithIsClosedReadLock(Supplier<K> supplier) {
        Lock readLock = isClosedLock.readLock();
        readLock.lock();
        try {
            return supplier.get();
        } finally {
            readLock.unlock();
        }
    }

    private void runWithIsClosedWriteLock(Runnable runnable) {
        Lock writeLock = isClosedLock.writeLock();
        writeLock.lock();
        try {
            runnable.run();
        } finally {
            writeLock.unlock();
        }
    }

    private void shutdownResource(T resourceToClose) {
        try {
            resourceToClose.close();
        } catch (Exception e) {
            log.warn("Failed to close resource. This may result in a resource leak", e);
        }
    }
}
