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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @Before
    public void setUp() {
        readWriteLock = new LockServerLock(StringLockDescriptor.of("lock"), new LockClientIndices());
        anonymousReadLock = readWriteLock.get(LockClient.ANONYMOUS, LockMode.READ);
        anonymousWriteLock = readWriteLock.get(LockClient.ANONYMOUS, LockMode.WRITE);
        knownClientReadLock = readWriteLock.get(client, LockMode.READ);
        knownClientWriteLock = readWriteLock.get(client, LockMode.WRITE);
        barrier = new CyclicBarrier(2);
    }

    /** Tests using an anonymous (non-reentrant) client. */
    @Test
    public void testAnonymousClient() throws InterruptedException {
        assertThat(anonymousReadLock.tryLock()).isNull();
        assertThat(anonymousWriteLock.tryLock()).isNotNull();
        assertThat(anonymousWriteLock.tryLock(10, TimeUnit.MILLISECONDS)).isNotNull();
        anonymousReadLock.lock();
        anonymousReadLock.unlock();
        anonymousReadLock.unlock();
        assertThat(anonymousWriteLock.tryLock(10, TimeUnit.MILLISECONDS)).isNull();
        assertThat(anonymousReadLock.tryLock(10, TimeUnit.MILLISECONDS)).isNotNull();
        assertThat(anonymousWriteLock.tryLock()).isNotNull();
        anonymousWriteLock.unlock();
    }

    /** Tests that things fail when they should. */
    @Test
    public void testIllegalActions() throws InterruptedException {
        assertThatThrownBy(() -> knownClientReadLock.unlock())
                .as("should not be able to unlock when no locks are held")
                .isInstanceOf(IllegalMonitorStateException.class);
        assertThatThrownBy(() -> knownClientWriteLock.unlock())
                .as("should not be able to unlock when no locks are held")
                .isInstanceOf(IllegalMonitorStateException.class);
        knownClientReadLock.lock();
        assertThatThrownBy(() -> knownClientWriteLock.tryLock())
                .as("should not be able to lock when lock is not free")
                .isInstanceOf(IllegalMonitorStateException.class);
        assertThatThrownBy(() -> knownClientWriteLock.tryLock(10, TimeUnit.MILLISECONDS))
                .as("should not be able to lock when lock is not free")
                .isInstanceOf(IllegalMonitorStateException.class);
    }

    /** Tests changing the owner of locks. */
    @Test
    public void testChangeOwner() {
        assertThatThrownBy(() -> knownClientReadLock.changeOwner(LockClient.ANONYMOUS))
                .isInstanceOf(IllegalMonitorStateException.class);
        assertThatThrownBy(() -> knownClientWriteLock.changeOwner(LockClient.ANONYMOUS))
                .isInstanceOf(IllegalMonitorStateException.class);
        knownClientWriteLock.lock();
        knownClientReadLock.lock();
        assertThatThrownBy(() -> knownClientReadLock.changeOwner(LockClient.ANONYMOUS))
                .isInstanceOf(IllegalMonitorStateException.class);
        assertThatThrownBy(() -> knownClientWriteLock.changeOwner(LockClient.ANONYMOUS))
                .isInstanceOf(IllegalMonitorStateException.class);
        knownClientReadLock.unlock();
        knownClientWriteLock.lock();
        assertThatThrownBy(() -> knownClientWriteLock.changeOwner(LockClient.ANONYMOUS))
                .isInstanceOf(IllegalMonitorStateException.class);
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
    @Test
    public void testTimedTryLockCanSucceed() throws Exception {
        anonymousReadLock.lock();
        Future<?> future = executor.submit((Callable<Void>) () -> {
            assertThat(anonymousWriteLock.tryLock(10, TimeUnit.MILLISECONDS)).isNotNull();
            barrier.await();
            assertThat(anonymousWriteLock.tryLock(100, TimeUnit.MILLISECONDS)).isNull();
            return null;
        });
        barrier.await();
        Thread.sleep(10);
        anonymousReadLock.unlock();
        future.get();
        future = executor.submit((Callable<Void>) () -> {
            assertThat(anonymousReadLock.tryLock(10, TimeUnit.MILLISECONDS)).isNotNull();
            barrier.await();
            assertThat(anonymousReadLock.tryLock(100, TimeUnit.MILLISECONDS)).isNull();
            return null;
        });
        barrier.await();
        Thread.sleep(10);
        anonymousWriteLock.unlock();
        future.get();
        anonymousReadLock.unlock();
    }

    /** Tests that a timed try lock can fail and wake up blocking threads. */
    @Test
    public void testTimedTryLockCanFail() throws Exception {
        anonymousReadLock.lock();
        assertThat(anonymousReadLock.tryLock()).isNull();
        anonymousReadLock.unlock();
        Future<?> future1 = executor.submit((Callable<Void>) () -> {
            barrier.await();
            assertThat(anonymousWriteLock.tryLock(100, TimeUnit.MILLISECONDS)).isNotNull();
            return null;
        });
        barrier.await();
        Thread.sleep(10);
        Future<?> future2 = executor.submit(() -> {
            assertThat(anonymousReadLock.tryLock()).isNotNull();
            anonymousReadLock.lock();
        });
        assertThatThrownBy(() -> future2.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        future1.get(200, TimeUnit.MILLISECONDS);
        future2.get(10, TimeUnit.MILLISECONDS);
    }

    /** Tests that locks obey fair ordering. */
    @Test
    public void testFairness() throws Exception {
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
        assertThat(ImmutableList.copyOf(orderingQueue))
                .containsExactly("one", "two", "two", "three", "four", "five", "five");
        anonymousWriteLock.unlock();
    }

    private <T> void addLockToQueue(final KnownClientLock lock, final Queue<? super T> queue, final T index)
            throws Exception {
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
    @Test
    public void testUninterruptibleLock() throws Exception {
        anonymousReadLock.lock();
        InterruptibleFuture<?> future = new InterruptibleFuture<Void>() {
            @Override
            public Void call() throws Exception {
                barrier.await();
                assertThat(Thread.interrupted()).isFalse();
                anonymousWriteLock.lock();
                assertThat(Thread.interrupted()).isTrue();
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
    @Test
    public void testInterruptibleTryLock() throws Exception {
        anonymousWriteLock.lock();
        InterruptibleFuture<?> futureToCancel = new InterruptibleFuture<Void>() {
            @Override
            public Void call() throws Exception {
                assertThat(Thread.interrupted()).isFalse();
                barrier.await();
                assertThatThrownBy(() -> knownClientWriteLock.tryLock(1, TimeUnit.SECONDS))
                        .isInstanceOf(InterruptedException.class);
                return null;
            }
        };
        executor.execute(futureToCancel);
        barrier.await();
        assertThatThrownBy(() -> futureToCancel.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        Future<?> futureToSucceed = executor.submit((Callable<Void>) () -> {
            assertThat(anonymousReadLock.tryLock()).isNotNull();
            barrier.await();
            anonymousReadLock.lock();
            return null;
        });
        barrier.await();
        assertThatThrownBy(() -> futureToSucceed.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        futureToCancel.cancel(true);
        futureToCancel.get(1000, TimeUnit.MILLISECONDS);
        anonymousWriteLock.unlock();
        futureToSucceed.get(1000, TimeUnit.MILLISECONDS);
        anonymousReadLock.unlock();
        assertThat(knownClientWriteLock.tryLock()).isNull();
        knownClientWriteLock.unlock();
    }

    /** Tests that unlockAndFreeze() works as expected. */
    @Test
    public void testFreezing() throws InterruptedException {
        knownClientReadLock.lock();
        assertThatThrownBy(() -> knownClientReadLock.unlockAndFreeze())
                .isInstanceOf(IllegalMonitorStateException.class);
        anonymousReadLock.lock();
        knownClientReadLock.unlock();
        anonymousReadLock.unlock();
        anonymousWriteLock.lock();
        assertThatThrownBy(() -> anonymousWriteLock.unlockAndFreeze()).isInstanceOf(IllegalMonitorStateException.class);
        anonymousWriteLock.unlock();
        knownClientWriteLock.lock();
        knownClientWriteLock.unlockAndFreeze();
        assertThat(knownClientWriteLock.tryLock()).isNull();
        assertThat(knownClientReadLock.tryLock()).isNull();
        knownClientReadLock.unlock();
        assertThat(knownClientWriteLock.tryLock(10, TimeUnit.MILLISECONDS)).isNull();
        knownClientWriteLock.unlockAndFreeze();
        assertThat(knownClientWriteLock.tryLock()).isNotNull();
        assertThat(knownClientWriteLock.tryLock(10, TimeUnit.MILLISECONDS)).isNotNull();
        assertThat(knownClientReadLock.tryLock()).isNotNull();
        knownClientWriteLock.unlock();
        assertThat(knownClientReadLock.tryLock(10, TimeUnit.MILLISECONDS)).isNull();
        knownClientReadLock.unlock();
    }

    @Test
    public void testReadLockReentrancy() throws Exception {
        knownClientReadLock.tryLock();
        InterruptibleFuture<?> future = new InterruptibleFuture<Void>() {
            @Override
            public Void call() throws Exception {
                assertThat(Thread.interrupted()).isFalse();
                barrier.await();
                anonymousWriteLock.lock();
                return null;
            }
        };
        executor.execute(future);
        barrier.await();
        assertThatThrownBy(() -> future.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        assertThat(knownClientReadLock.tryLock()).isNull();
        knownClientReadLock.unlock();
        assertThatThrownBy(() -> future.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        knownClientReadLock.unlock();
        future.get(5, TimeUnit.SECONDS);
        anonymousWriteLock.unlock();
    }

    /** Tests that our objects have {@code toString()} methods defined. */
    @Test
    public void testToStrings() {
        assertThat(client.getClientId()).isEqualTo("client");
        assertThat(readWriteLock.getDescriptor().getLockIdAsString()).isEqualTo("lock");
        assertGoodToString(readWriteLock);
        assertGoodToString(anonymousReadLock);
        assertGoodToString(knownClientWriteLock);
    }

    private void assertGoodToString(Object object) {
        assertThat(object.toString().startsWith(object.getClass().getSimpleName() + "{"))
                .isTrue();
    }
}
