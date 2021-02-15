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
package com.palantir.common.base;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchingVisitable} that will prefetch in a background thread. If an exception happens on
 * the fetch thread, it will be thrown in batchAccept after all prefetched items have been visited.
 */
public class PrefetchingBatchingVisitable<T> implements BatchingVisitable<T> {
    private static final Logger log = LoggerFactory.getLogger(PrefetchingBatchingVisitable.class);

    private final BatchingVisitable<T> delegate;
    private final int capacity;
    private final ExecutorService exec;
    private final String name;

    public PrefetchingBatchingVisitable(
            BatchingVisitable<T> delegate, int capacity, ExecutorService exec, String name) {
        this.delegate = delegate;
        this.capacity = capacity;
        this.exec = exec;
        this.name = name;
    }

    @Override
    public <K extends Exception> boolean batchAccept(final int batchSize, AbortingVisitor<? super List<T>, K> v)
            throws K {
        final Queue<List<T>> queue = Queues.newArrayDeque();
        final Lock lock = new ReentrantLock();
        final Condition itemAvailable = lock.newCondition();
        final Condition spaceAvailable = lock.newCondition();
        final AtomicBoolean futureIsDone = new AtomicBoolean(false);
        final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
        final Stopwatch fetchTime = Stopwatch.createUnstarted();
        final Stopwatch fetchBlockedTime = Stopwatch.createUnstarted();
        final Stopwatch visitTime = Stopwatch.createUnstarted();
        final Stopwatch visitBlockedTime = Stopwatch.createUnstarted();

        Future<?> future = exec.submit(() -> {
            try {
                fetchTime.start();
                delegate.batchAccept(batchSize, item -> {
                    fetchTime.stop();
                    fetchBlockedTime.start();
                    lock.lock();
                    try {
                        while (queue.size() >= capacity) {
                            spaceAvailable.await();
                        }
                        fetchBlockedTime.stop();
                        queue.add(item);
                        itemAvailable.signalAll();
                    } finally {
                        lock.unlock();
                    }
                    fetchTime.start();
                    return true;
                });
                fetchTime.stop();
            } catch (InterruptedException e) {
                // shutting down
            } catch (Throwable t) {
                exception.set(t);
            } finally {
                if (fetchTime.isRunning()) {
                    fetchTime.stop();
                }
                if (fetchBlockedTime.isRunning()) {
                    fetchBlockedTime.stop();
                }
                lock.lock();
                try {
                    futureIsDone.set(true);
                    itemAvailable.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        });

        try {
            while (true) {
                List<T> batch;
                visitBlockedTime.start();
                lock.lock();
                try {
                    while (queue.isEmpty()) {
                        if (futureIsDone.get()) {
                            if (exception.get() != null) {
                                throw Throwables.rewrapAndThrowUncheckedException(exception.get());
                            }
                            return true;
                        }
                        itemAvailable.await();
                    }
                    batch = queue.poll();
                    spaceAvailable.signalAll();
                } finally {
                    lock.unlock();
                }
                visitBlockedTime.stop();
                visitTime.start();
                boolean proceed = v.visit(batch);
                visitTime.stop();
                if (!proceed) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        } finally {
            log.debug(
                    "{} timings: fetch {}, fetchBlocked {}, visit {}, visitBlocked {}",
                    name,
                    fetchTime,
                    fetchBlockedTime,
                    visitTime,
                    visitBlockedTime);
            future.cancel(true);
        }
    }
}
