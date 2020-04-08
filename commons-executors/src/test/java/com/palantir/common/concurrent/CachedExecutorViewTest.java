/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.Test;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;

public class CachedExecutorViewTest {

    @Test
    public void testShutdownNow() throws InterruptedException {
        AtomicBoolean interrupted = new AtomicBoolean();
        ExecutorService cached = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CachedExecutorViewTest-%d")
                .build());
        ExecutorService view = CachedExecutorView.of(cached);
        assertThat(view.isShutdown()).isEqualTo(cached.isShutdown()).isFalse();
        assertThat(view.isTerminated()).isEqualTo(cached.isTerminated()).isFalse();
        CountDownLatch executionStartedLatch = new CountDownLatch(1);
        CountDownLatch interruptedLatch = new CountDownLatch(1);
        view.execute(() -> {
            try {
                executionStartedLatch.countDown();
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                interrupted.set(true);
                // Wait so we can validate the time between shutdown and terminated
                Uninterruptibles.awaitUninterruptibly(interruptedLatch);
            }
        });
        executionStartedLatch.await();
        assertThat(view.shutdownNow()).as("Cached executors have no queue").isEmpty();
        assertThat(view.isShutdown()).isTrue();
        assertThatThrownBy(() -> view.execute(Runnables.doNothing()))
                .as("Submitting work after invoking shutdown or shutdownNow should fail")
                .isInstanceOf(RejectedExecutionException.class);
        Awaitility.waitAtMost(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertThat(interrupted).isTrue();
            assertThat(view.isTerminated()).isFalse();
        });
        interruptedLatch.countDown();
        Awaitility.waitAtMost(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(view.isTerminated()).isTrue());

        assertThat(cached.isShutdown()).isFalse();
        assertThat(cached.isTerminated()).isFalse();
        assertThat(MoreExecutors.shutdownAndAwaitTermination(cached, 1, TimeUnit.SECONDS))
                .as("Failed to clean up the delegate executor")
                .isTrue();
    }

    @Test
    public void testShutdown() throws InterruptedException {
        AtomicBoolean interrupted = new AtomicBoolean();
        ExecutorService cached = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CachedExecutorViewTest-%d")
                .build());
        ExecutorService view = CachedExecutorView.of(cached);
        assertThat(view.isShutdown()).isEqualTo(cached.isShutdown()).isFalse();
        assertThat(view.isTerminated()).isEqualTo(cached.isTerminated()).isFalse();
        CountDownLatch executionStartedLatch = new CountDownLatch(1);
        view.execute(() -> {
            try {
                executionStartedLatch.countDown();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        executionStartedLatch.await();
        view.shutdown();
        assertThat(view.isShutdown()).isTrue();
        assertThatThrownBy(() -> view.execute(Runnables.doNothing()))
                .as("Submitting work after invoking shutdown or shutdown should fail")
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(view.isTerminated()).isFalse();
        Awaitility.waitAtMost(600, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(view.isTerminated()).isTrue());
        assertThat(interrupted).isFalse();

        assertThat(cached.isShutdown()).isFalse();
        assertThat(cached.isTerminated()).isFalse();
        assertThat(MoreExecutors.shutdownAndAwaitTermination(cached, 1, TimeUnit.SECONDS))
                .as("Failed to clean up the delegate executor")
                .isTrue();
    }
}
