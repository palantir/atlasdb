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
package com.palantir.common.pooling;

import com.google.common.base.Function;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.collect.EmptyQueue;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * This class will pool resources up to the passedLimit.  It will never block and instead will always allocate a new
 * resource if there are none in the pool.
 */
public abstract class AbstractPoolingContainer<T> implements PoolingContainer<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(AbstractPoolingContainer.class);

    private volatile Queue<T> pool;
    protected final AtomicLong allocatedResources = new AtomicLong();
    private final int maxPoolSize;

    /**
     * Pool up to itemsToPool resources.  If the internal pool is already full, then additional ones will be
     * thrown away.
     * <p>
     * If items is {@link Integer#MAX_VALUE}, then we will use an unbounded queue with {@link ConcurrentLinkedQueue}
     * which should have better performance because it is lock-free
     * @param itemsToPool must be at least 1
     */
    public AbstractPoolingContainer(int itemsToPool) {
        if (itemsToPool == Integer.MAX_VALUE) {
            pool = new ConcurrentLinkedQueue<T>();
        } else {
            pool = new ArrayBlockingQueue<T>(itemsToPool);
        }
        this.maxPoolSize = itemsToPool;
    }

    public long getAllocatedResources() {
        return allocatedResources.get();
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * This should create a new resource.  It should be non-null.
     */
    @Nonnull
    protected abstract T createNewPooledResource();

    /**
     * This will be called after a resource is done being used but the pool is full and it will be discarded
     * <p>
     * This should not throw because it is called in finally blocks.
     */
    protected void cleanupForDiscard(T discardedResource) {
        // nothing to do here
    }

    /**
     * This will be called after a resource is done being used and will be returned to the pool so it maybe used
     * again.
     * <p>
     * {@link #cleanupForDiscard(Object)} may be called for this resource after this returns if the
     * pool was already full or {@link #shutdownPooling()} has been called.
     * <p>
     * This should not throw because it is called in finally blocks.
     */
    protected void cleanupForReturnToPool(T resourceToReturn) {
        // nothing here
    }

    /**
     * This will run the provided function with a pooled resource.
     * {@link #createNewPooledResource()} will be called if it cannot get one from the pool.
     * This function is not allowed to hold onto this resource after it completes.  If it does, there will be
     * undefined behavior.
     * @return whatever the passed function returns
     */
    @Override
    public <V> V runWithPooledResource(Function<T, V> f) {
        T resource = getResource();
        try {
            return f.apply(resource);
        } finally {
            returnResource(resource);
        }
    }

    @Override
    public <V, K extends Exception> V runWithPooledResource(FunctionCheckedException<T, V, K> f) throws K {
        T resource = getResource();
        try {
            return f.apply(resource);
        } finally {
            returnResource(resource);
        }
    }

    /**
     * This should always be followed by a try/finally
     */
    protected final T getResource() {
        T resource = pool.poll();
        if (resource == null) {
            resource = createNewPooledResource();
            Preconditions.checkNotNull(resource, "resource should be non-null");
            allocatedResources.incrementAndGet();
        }
        logPoolStats();
        return resource;
    }

    /**
     * This method should only be called in the finally block of a try/finally
     */
    protected final void returnResource(T resource) {
        Preconditions.checkNotNull(resource);
        try {
            cleanupForReturnToPool(resource);
        } catch (RuntimeException e) {
            log.error("should not throw here", e);
        }
        boolean wasReturned = returnToQueue(resource);
        if (!wasReturned) {
            log.debug("Pool full, releasing resource: {}", UnsafeArg.of("resource", resource));
            allocatedResources.decrementAndGet();
            try {
                cleanupForDiscard(resource);
            } catch (RuntimeException e) {
                log.error("should not throw here", e);
            }
        }
        logPoolStats();
    }

    private void logPoolStats() {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Allocated {} instances, {} remaining in pool, {} max pool size",
                    SafeArg.of("resourceCount", getAllocatedResources()),
                    SafeArg.of("poolRemaining", pool.size()),
                    SafeArg.of("poolMax", getMaxPoolSize()));
        }
    }

    /**
     * This method will call {@link #cleanupForDiscard(Object)} on everything that is in the pool and will
     * make the pool of size zero.  Once this method is called using this pool will just result in {@link #createNewPooledResource()}
     * being called for every use and {@link #cleanupForDiscard(Object)} being called after it is used.
     */
    @Override
    public void shutdownPooling() {
        Queue<T> currentPool = pool;
        pool = EmptyQueue.of();
        discardFromPool(currentPool);
    }

    /**
     * This method is used when we still want to continue to pool, but we think that the current pooled resources
     * are no longer good to use.
     */
    protected void discardCurrentPool() {
        discardFromPool(pool);
    }

    private void discardFromPool(Queue<T> currentPool) {
        T item = currentPool.poll();
        while (item != null) {
            allocatedResources.decrementAndGet();
            try {
                cleanupForDiscard(item);
                log.debug("Discarded: {}", UnsafeArg.of("item", item));
            } catch (RuntimeException e) {
                log.error("should not throw here", e);
            }
            item = currentPool.poll();
        }
    }

    private boolean returnToQueue(T resource) {
        Queue<T> currentPool = pool;
        boolean ret = currentPool.offer(resource);
        if (currentPool != pool) {
            discardFromPool(currentPool);
        }
        if (ret) {
            log.debug("Returned: {}", UnsafeArg.of("resource", resource));
        }
        return ret;
    }
}
