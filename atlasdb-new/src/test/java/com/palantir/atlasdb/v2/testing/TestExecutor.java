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

package com.palantir.atlasdb.v2.testing;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

public final class TestExecutor {
    private static final Logger log = LoggerFactory.getLogger(TestExecutor.class);

    private final NavigableMap<Long, Deque<Runnable>> tasks = new TreeMap<>();
    private final Random random = new Random(0);

    private long now = 0;
    private long executed = 0;

    private Deque<Runnable> taskList(long time) {
        return tasks.computeIfAbsent(time, $ -> new ArrayDeque<>(1));
    }

    private Runnable nextTask() {
        long firstKey = tasks.firstKey();
        now = firstKey;
        Deque<Runnable> taskList = tasks.get(firstKey);
        Runnable task = tasks.get(firstKey).removeFirst();
        if (taskList.isEmpty()) {
            tasks.remove(firstKey);
        }
        return task;
    }

    public Executor nowScheduler() {
        return task -> {
            taskList(now).addLast(task);
        };
    }

    // 5% uniformly distributed jitter
    private long jitter(long input) {
        long jitterLimit = input / 20;
        long jitter = random.nextInt(Ints.checkedCast(jitterLimit));
        return input + jitter - jitterLimit;
    }

    public Executor soonScheduler() {
        return task -> {
            taskList(now + jitter(1_000)).addLast(task);
        };
    }

    public Executor notSoonScheduler() {
        return task -> {
            taskList(now + jitter(1_000_000)).addLast(task);
        };
    }

    public void start() {
        while (now < TimeUnit.DAYS.toMicros(10) && !tasks.isEmpty()) {
            try {
                executed++;
                nextTask().run();
            } catch (Throwable t) {
                log.info("Task threw", t);
            }
        }
    }

    public ScheduledExecutorService actuallyProgrammableScheduler() {
        return new ScheduledExecutorService() {
            @Override
            public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                taskList(now + unit.toMicros(delay)).addLast(command);
                return null;
            }

            @Override
            public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                    TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                    TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void shutdown() {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Runnable> shutdownNow() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isShutdown() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isTerminated() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Future<T> submit(Callable<T> task) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<?> submit(Runnable task) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void execute(Runnable command) {
                nowScheduler().execute(command);
            }
        };
    }

}
