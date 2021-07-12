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
package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InterruptibleFutureTest {

    private ExecutorService executor;

    @Before
    public void before() {
        executor = PTExecutors.newCachedThreadPool();
    }

    @After
    public void after() throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testSimple() throws Exception {
        RunnableFuture<Integer> interruptible = getInterruptible();
        executor.execute(interruptible);
        assertThat(interruptible.get()).isEqualTo(1);
    }

    @Test
    public void testNoStart() {
        RunnableFuture<Integer> interruptible = getInterruptible();
        assertThatThrownBy(() -> interruptible.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    @Test
    public void testCancelTrueBeforeStart() {
        RunnableFuture<Integer> interruptible = getInterruptible();
        interruptible.cancel(true);
        executor.execute(interruptible);
        assertThatThrownBy(interruptible::get).isInstanceOf(CancellationException.class);
    }

    @Test
    public void testCancelFalseBeforeStart() {
        RunnableFuture<Integer> interruptible = getInterruptible();
        interruptible.cancel(false);
        executor.execute(interruptible);
        assertThatThrownBy(interruptible::get).isInstanceOf(CancellationException.class);
    }

    @Test
    public void testCancelTrueWithSleep() throws Exception {
        InterruptibleWithSleep interruptible = getInterruptibleWithSleep();
        executor.execute(interruptible);
        interruptible.started.await();
        interruptible.cancel(true);
        assertThatThrownBy(interruptible::get)
                .isInstanceOfAny(CancellationException.class, ExecutionException.class)
                .hasRootCauseInstanceOf(InterruptedException.class);
    }

    @Test
    public void testCancelFalseWithSleep() throws Exception {
        InterruptibleWithSleep interruptible = getInterruptibleWithSleep();
        executor.execute(interruptible);
        interruptible.started.await();
        interruptible.cancel(false);
        assertThatThrownBy(() -> interruptible.get(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    @Test
    public void testCancelAfterRunning() throws Exception {
        RunnableFuture<Integer> interruptible = getInterruptible();
        interruptible.run();
        interruptible.cancel(true);
        assertThat(interruptible.get()).isEqualTo(1);
    }

    @Test
    public void testCancelButRunToCompletion() throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch hang = new CountDownLatch(1);
        RunnableFuture<Integer> interruptible = new InterruptibleFuture<>() {
            @Override
            public Integer call() {
                try {
                    started.countDown();
                    hang.await();
                    return 0;
                } catch (InterruptedException e) {
                    return 1;
                }
            }
        };
        executor.execute(interruptible);
        started.await();
        interruptible.cancel(true);
        assertThat(interruptible.get()).isEqualTo(1);
    }

    @Test
    public void testCompleteAndThenCancel() throws Exception {
        RunnableFuture<Integer> interruptible = new InterruptibleFuture<>() {
            @Override
            public Integer call() throws InterruptedException {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                return 1;
            }
        };
        executor.execute(interruptible);
        assertThat(interruptible.get()).isEqualTo(1);
        interruptible.cancel(true);
        assertThat(interruptible.get()).isEqualTo(1);
    }

    @Test
    public void testCancelTrueNoStart() {
        RunnableFuture<Integer> interruptible = getInterruptible();
        interruptible.cancel(true);
        assertThatThrownBy(interruptible::get).isInstanceOf(CancellationException.class);
    }

    @Test
    public void testCancelFalseNoStart() {
        RunnableFuture<Integer> interruptible = getInterruptible();
        interruptible.cancel(false);
        assertThatThrownBy(interruptible::get).isInstanceOf(CancellationException.class);
    }

    private static RunnableFuture<Integer> getInterruptible() {
        return new InterruptibleFuture<>() {
            @Override
            public Integer call() {
                return 1;
            }
        };
    }

    private static final class InterruptibleWithSleep extends InterruptibleFuture<Integer> {
        private final CountDownLatch started = new CountDownLatch(1);

        @Override
        protected Integer call() throws Exception {
            started.countDown();
            Thread.sleep(10 * 1000); // 10 seconds
            return 1;
        }
    }

    private static InterruptibleWithSleep getInterruptibleWithSleep() {
        return new InterruptibleWithSleep();
    }
}
