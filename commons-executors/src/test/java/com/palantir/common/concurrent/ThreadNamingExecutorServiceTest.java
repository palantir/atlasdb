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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ThreadNamingExecutorServiceTest {

    @Test
    public void testThreadRenaming_runnable() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("ThreadNamingExecutorServiceTest-%d")
                .setDaemon(true)
                .build());
        try {
            String expectedBase = "ThreadNamingExecutorServiceTest-0";
            String expectedRenamed = "renamed-" + expectedBase;
            ExecutorService renaming = ThreadNamingExecutorService.builder()
                    .executor(executorService)
                    .threadNameFunction(original -> "renamed-" + original)
                    .build();
            AtomicReference<Thread> capturedThread = new AtomicReference<>();
            renaming.submit(() -> {
                Thread current = Thread.currentThread();
                capturedThread.set(current);
                assertThat(current.getName()).isEqualTo(expectedRenamed);
            })
                    .get();
            assertThat(capturedThread.get().getName())
                    .as("Expected the thread name to be reverted to its original value")
                    .isEqualTo(expectedBase);
        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS))
                    .as("Failed to clean up the delegate executor")
                    .isTrue();
        }
    }

    @Test
    public void testThreadRenaming_callable() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("ThreadNamingExecutorServiceTest-%d")
                .setDaemon(true)
                .build());
        try {
            String expectedBase = "ThreadNamingExecutorServiceTest-0";
            String expectedRenamed = "renamed-" + expectedBase;
            ExecutorService renaming = ThreadNamingExecutorService.builder()
                    .executor(executorService)
                    .threadNameFunction(original -> "renamed-" + original)
                    .build();
            AtomicReference<Thread> capturedThread = new AtomicReference<>();
            renaming.submit(() -> {
                Thread current = Thread.currentThread();
                capturedThread.set(current);
                assertThat(current.getName()).isEqualTo(expectedRenamed);
                return "result";
            })
                    .get();
            assertThat(capturedThread.get().getName())
                    .as("Expected the thread name to be reverted to its original value")
                    .isEqualTo(expectedBase);
        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS))
                    .as("Failed to clean up the delegate executor")
                    .isTrue();
        }
    }

    @Test
    public void testUncaughtExceptionHandlerCalledWithUpdatedName_runnable_execute() {
        List<String> uncaughtExceptionThreadNames = new CopyOnWriteArrayList<>();
        String expectedBase = "ThreadNamingExecutorServiceTest-0";
        String expectedRenamed = "renamed-" + expectedBase;
        Thread.UncaughtExceptionHandler handler =
                (thread, ignored) -> uncaughtExceptionThreadNames.add(thread.getName());
        ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("ThreadNamingExecutorServiceTest-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler(handler)
                .build());
        try {
            ExecutorService renaming = ThreadNamingExecutorService.builder()
                    .executor(executorService)
                    .threadNameFunction(original -> "renamed-" + original)
                    .build();
            renaming.execute(() -> {
                throw new RuntimeException();
            });
        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS))
                    .as("Failed to clean up the delegate executor")
                    .isTrue();
        }
        assertThat(uncaughtExceptionThreadNames).containsExactly(expectedRenamed);
    }

    @Test
    public void testUncaughtExceptionHandlerCalledWithUpdatedName_runnable_submit() {
        List<String> uncaughtExceptionThreadNames = new CopyOnWriteArrayList<>();
        Thread.UncaughtExceptionHandler handler =
                (thread, ignored) -> uncaughtExceptionThreadNames.add(thread.getName());
        ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("ThreadNamingExecutorServiceTest-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler(handler)
                .build());
        try {
            ExecutorService renaming = ThreadNamingExecutorService.builder()
                    .executor(executorService)
                    .threadNameFunction(original -> "renamed-" + original)
                    .build();
            Runnable throwingRunnable = () -> {
                throw new RuntimeException();
            };
            assertThatThrownBy(renaming.submit(throwingRunnable)::get)
                    .isInstanceOf(ExecutionException.class);
        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS))
                    .as("Failed to clean up the delegate executor")
                    .isTrue();
        }
        assertThat(uncaughtExceptionThreadNames)
                .as("executor.submit should not interact with the uncaught exception handler, "
                        + "failures are recorded to the returned future")
                .isEmpty();
    }

    @Test
    public void testUncaughtExceptionHandlerCalledWithUpdatedName_callable_submit() {
        List<String> uncaughtExceptionThreadNames = new CopyOnWriteArrayList<>();
        Thread.UncaughtExceptionHandler handler =
                (thread, ignored) -> uncaughtExceptionThreadNames.add(thread.getName());
        ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("ThreadNamingExecutorServiceTest-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler(handler)
                .build());
        try {
            ExecutorService renaming = ThreadNamingExecutorService.builder()
                    .executor(executorService)
                    .threadNameFunction(original -> "renamed-" + original)
                    .build();
            Callable<Object> throwingCallable = () -> {
                throw new RuntimeException();
            };
            assertThatThrownBy(renaming.submit(throwingCallable)::get)
                    .isInstanceOf(ExecutionException.class);
        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS))
                    .as("Failed to clean up the delegate executor")
                    .isTrue();
        }
        assertThat(uncaughtExceptionThreadNames)
                .as("executor.submit should not interact with the uncaught exception handler, "
                        + "failures are recorded to the returned future")
                .isEmpty();
    }
}
