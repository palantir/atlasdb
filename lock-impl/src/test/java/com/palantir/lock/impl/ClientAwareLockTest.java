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
package com.palantir.lock.impl;

import com.google.common.collect.ImmutableList;
import com.palantir.common.concurrent.InterruptibleFuture;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.StringLockDescriptor;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests for {@link ClientAwareReadWriteLockImpl}.
 *
 * @author jtamer
 */
@ShouldRetry
public final class ClientAwareLockTest {

    private static final ExecutorService executor =
            PTExecutors.newCachedThreadPool(ClientAwareLockTest.class.getName());

    private final LockClient client = LockClient.of("client");
    private ClientAwareReadWriteLock readWriteLock;
    private KnownClientLock anonymousReadLock;
    private KnownClientLock anonymousWriteLock;
    private KnownClientLock knownClientReadLock;
    private KnownClientLock knownClientWriteLock;
    private CyclicBarrier barrier;

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    /** Sets up the tests. */
    @Before public void setUp() {
        readWriteLock = new LockServerLock(StringLockDescriptor.of("lock"), new LockClientIndices());
        anonymousReadLock = readWriteLock.get(LockClient.ANONYMOUS, LockMode.READ);
        anonymousWriteLock = readWriteLock.get(LockClient.ANONYMOUS, LockMode.WRITE);
        knownClientReadLock = readWriteLock.get(client, LockMode.READ);
        knownClientWriteLock = readWriteLock.get(client, LockMode.WRITE);
        barrier = new CyclicBarrier(2);
    }

    /** Tests using an anonymous (non-reentrant) client. */
    @Test public void testAnonymousClient() throws InterruptedException {
        Assert.assertNull(anonymousReadLock.tryLock());
        Assert.assertNotNull(anonymousWriteLock.tryLock());
        Assert.assertNotNull(anonymousWriteLock.tryLock(10, TimeUnit.MILLISECONDS));
        anonymousReadLock.lock();
        anonymousReadLock.unlock();
        anonymousReadLock.unlock();
        Assert.assertNull(anonymousWriteLock.tryLock(10, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(anonymousReadLock.tryLock(10, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(anonymousWriteLock.tryLock());
        anonymousWriteLock.unlock();
    }

    /** Tests that things fail when they should. */
    @Test public void testIllegalActions() throws InterruptedException {
        try {
            knownClientReadLock.unlock();
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected: no locks held. */
        }
        try {
            knownClientWriteLock.unlock();
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected: no locks held. */
        }
        knownClientReadLock.lock();
        try {
            knownClientWriteLock.tryLock();
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected: can't grab write lock while holding only read lock. */
        }
        try {
            knownClientWriteLock.tryLock(10, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected: can't grab write lock while holding only read lock. */
        }
    }

    /** Tests changing the owner of locks. */
    @Test public void testChangeOwner() {
        try {
            knownClientReadLock.changeOwner(LockClient.ANONYMOUS);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected; no locks held. */
        }
        try {
            knownClientWriteLock.changeOwner(LockClient.ANONYMOUS);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected; no locks held. */
        }
        knownClientWriteLock.lock();
        knownClientReadLock.lock();
        try {
            knownClientReadLock.changeOwner(LockClient.ANONYMOUS);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected; holding both read and write locks. */
        }
        try {
            knownClientWriteLock.changeOwner(LockClient.ANONYMOUS);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected; holding both read and write locks. */
        }
        knownClientReadLock.unlock();
        knownClientWriteLock.lock();
        try {
            knownClientWriteLock.changeOwner(LockClient.ANONYMOUS);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected; holding two write locks. */
        }
        knownClientWriteLock.unlock();
        knownClientWriteLock.changeOwner(LockClient.ANONYMOUS);
        anonymousWriteLock.unlock();
        knownClientReadLock.lock();
        knownClientReadLock.lock();
        knownClientReadLock.changeOwner(LockClient.ANONYMOUS);
        anonymousReadLock.unlock();
        knownClientReadLock.unlock();
    }

    /** Tests that a timed try lock can block but eventually succeed. */
    @Test public void testTimedTryLockCanSucceed() throws Exception {
        anonymousReadLock.lock();
        Future<?> future = executor.submit((Callable<Void>) () -> {
            Assert.assertNotNull(anonymousWriteLock.tryLock(10, TimeUnit.MILLISECONDS));
            barrier.await();
            Assert.assertNull(anonymousWriteLock.tryLock(100, TimeUnit.MILLISECONDS));
            return null;
        });
        barrier.await();
        Thread.sleep(10);
        anonymousReadLock.unlock();
        future.get();
        future = executor.submit((Callable<Void>) () -> {
            Assert.assertNotNull(anonymousReadLock.tryLock(10, TimeUnit.MILLISECONDS));
            barrier.await();
            Assert.assertNull(anonymousReadLock.tryLock(100, TimeUnit.MILLISECONDS));
            return null;
        });
        barrier.await();
        Thread.sleep(10);
        anonymousWriteLock.unlock();
        future.get();
        anonymousReadLock.unlock();
    }

    /** Tests that a timed try lock can fail and wake up blocking threads. */
    @Test public void testTimedTryLockCanFail() throws Exception {
        anonymousReadLock.lock();
        Assert.assertNull(anonymousReadLock.tryLock());
        anonymousReadLock.unlock();
        Future<?> future1 = executor.submit((Callable<Void>) () -> {
            barrier.await();
            Assert.assertNotNull(anonymousWriteLock.tryLock(100, TimeUnit.MILLISECONDS));
            return null;
        });
        barrier.await();
        Thread.sleep(10);
        Future<?> future2 = executor.submit(() -> {
            Assert.assertNotNull(anonymousReadLock.tryLock());
            anonymousReadLock.lock();
        });
        try {
            future2.get(10, TimeUnit.MILLISECONDS);
        } catch (TimeoutException expected) {
            /* Expected. */
        }
        future1.get(200, TimeUnit.MILLISECONDS);
        future2.get(10, TimeUnit.MILLISECONDS);
    }

    /** Tests that locks obey fair ordering. */
    @Test public void testFairness() throws Exception {
        final Queue<String> orderingQueue = new ConcurrentLinkedQueue<String>();
        anonymousReadLock.lock();
        addLockToQueue(anonymousWriteLock, orderingQueue, "one");
        addLockToQueue(anonymousReadLock, orderingQueue, "two");
        addLockToQueue(anonymousReadLock, orderingQueue, "two");
        addLockToQueue(anonymousWriteLock, orderingQueue, "three");
        addLockToQueue(anonymousWriteLock, orderingQueue, "four");
        addLockToQueue(anonymousReadLock, orderingQueue, "five");
        addLockToQueue(anonymousReadLock, orderingQueue, "five");
        anonymousReadLock.unlock();
        anonymousWriteLock.lock();
        Assert.assertEquals(ImmutableList.of("one", "two", "two", "three", "four", "five", "five"),
                ImmutableList.copyOf(orderingQueue));
        anonymousWriteLock.unlock();
    }

    private <T> void addLockToQueue(final KnownClientLock lock, final Queue<? super T> queue,
            final T index) throws Exception {
        executor.submit((Callable<Void>) () -> {
            barrier.await();
            lock.lock();
            queue.add(index);
            lock.unlock();
            return null;
        });
        barrier.await();
        Thread.sleep(20);
    }

    /** Tests that {@code lock()} handles thread interruptions properly. */
    @Test public void testUninterruptibleLock() throws Exception {
        anonymousReadLock.lock();
        InterruptibleFuture<?> future = new InterruptibleFuture<Void>() {
            @Override
            public Void call() throws Exception {
                barrier.await();
                Assert.assertFalse(Thread.interrupted());
                anonymousWriteLock.lock();
                Assert.assertTrue(Thread.interrupted());
                return null;
            }
        };
        executor.execute(future);
        barrier.await();
        Thread.sleep(10);
        future.cancel(true);
        Thread.sleep(10);
        anonymousReadLock.unlock();
        future.get();
        anonymousWriteLock.unlock();
    }

    /** Tests that {@code tryLock()} handles thread interruptions properly. */
    @Test public void testInterruptibleTryLock() throws Exception {
        anonymousWriteLock.lock();
        InterruptibleFuture<?> futureToCancel = new InterruptibleFuture<Void>() {
            @Override
            public Void call() throws Exception {
                Assert.assertFalse(Thread.interrupted());
                barrier.await();
                try {
                    knownClientWriteLock.tryLock(1, TimeUnit.SECONDS);
                    Assert.fail();
                } catch (InterruptedException expected) {
                    /* Expected. */
                }
                return null;
            }
        };
        executor.execute(futureToCancel);
        barrier.await();
        try {
            futureToCancel.get(10, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException expected) {
            /* Expected. */
        }
        Future<?> futureToSucceed = executor.submit((Callable<Void>) () -> {
            Assert.assertNotNull(anonymousReadLock.tryLock());
            barrier.await();
            anonymousReadLock.lock();
            return null;
        });
        barrier.await();
        try {
            futureToSucceed.get(10, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException expected) {
            /* Expected. */
        }
        futureToCancel.cancel(true);
        futureToCancel.get(1000, TimeUnit.MILLISECONDS);
        anonymousWriteLock.unlock();
        futureToSucceed.get(1000, TimeUnit.MILLISECONDS);
        anonymousReadLock.unlock();
        Assert.assertNull(knownClientWriteLock.tryLock());
        knownClientWriteLock.unlock();
    }

    /** Tests that unlockAndFreeze() works as expected. */
    @Test public void testFreezing() throws InterruptedException {
        knownClientReadLock.lock();
        try {
            knownClientReadLock.unlockAndFreeze();
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected. */
        }
        anonymousReadLock.lock();
        knownClientReadLock.unlock();
        anonymousReadLock.unlock();
        anonymousWriteLock.lock();
        try {
            anonymousWriteLock.unlockAndFreeze();
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected. */
        }
        anonymousWriteLock.unlock();
        knownClientWriteLock.lock();
        knownClientWriteLock.unlockAndFreeze();
        Assert.assertNull(knownClientWriteLock.tryLock());
        Assert.assertNull(knownClientReadLock.tryLock());
        knownClientReadLock.unlock();
        Assert.assertNull(knownClientWriteLock.tryLock(10, TimeUnit.MILLISECONDS));
        knownClientWriteLock.unlockAndFreeze();
        Assert.assertNotNull(knownClientWriteLock.tryLock());
        Assert.assertNotNull(knownClientWriteLock.tryLock(10, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(knownClientReadLock.tryLock());
        knownClientWriteLock.unlock();
        Assert.assertNull(knownClientReadLock.tryLock(10, TimeUnit.MILLISECONDS));
        knownClientReadLock.unlock();
    }

    @Test public void testReadLockReentrancy() throws Exception {
        knownClientReadLock.tryLock();
        InterruptibleFuture<?> future = new InterruptibleFuture<Void>() {
            @Override
            public Void call() throws Exception {
                Assert.assertFalse(Thread.interrupted());
                barrier.await();
                anonymousWriteLock.lock();
                return null;
            }
        };
        executor.execute(future);
        barrier.await();
        try {
            future.get(10, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException expected) {
            /* Expected. */
        }
        Assert.assertNull(knownClientReadLock.tryLock());
        knownClientReadLock.unlock();
        try {
            future.get(10, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException expected) {
            /* Expected. */
        }
        knownClientReadLock.unlock();
        future.get(5, TimeUnit.SECONDS);
        anonymousWriteLock.unlock();
    }

    /** Tests that our objects have {@code toString()} methods defined. */
    @Test public void testToStrings() {
        Assert.assertEquals("client", client.getClientId());
        Assert.assertEquals("lock", readWriteLock.getDescriptor().getLockIdAsString());
        assertGoodToString(readWriteLock);
        assertGoodToString(anonymousReadLock);
        assertGoodToString(knownClientWriteLock);
    }

    private void assertGoodToString(Object object) {
        Assert.assertTrue(object.toString().startsWith(object.getClass().getSimpleName() + "{"));
    }
}
