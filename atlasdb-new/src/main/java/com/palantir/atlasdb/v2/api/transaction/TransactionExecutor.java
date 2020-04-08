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

package com.palantir.atlasdb.v2.api.transaction;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class TransactionExecutor implements Executor, Closeable {
    private static final Logger log = LoggerFactory.getLogger(TransactionExecutor.class);
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("transaction-executor-%d")
            .build();

    private final Thread thread;

    private final BlockingQueue<Runnable> crossThreadTaskQueue = new LinkedBlockingQueue<>();
    private final Queue<Runnable> sameThreadTaskQueue = new ArrayDeque<>();

    private volatile boolean closed = false;

    private TransactionExecutor() {
        this.thread = threadFactory.newThread(() -> {
            while (true) {
                Runnable runnable = getNextTask();
                if (runnable != null) {
                    try {
                        runnable.run();
                    } catch (Throwable t) {
                        log.warn("Unhandled exception from task", t);
                    }
                } else if (closed) {
                    return;
                }
            }
        });
        thread.start();
    }

    private Runnable getNextTask() {
        Runnable maybeFromSameThreadQueue = sameThreadTaskQueue.poll();
        if (maybeFromSameThreadQueue != null) {
            return maybeFromSameThreadQueue;
        }
        crossThreadTaskQueue.drainTo(sameThreadTaskQueue);
        maybeFromSameThreadQueue = sameThreadTaskQueue.poll();
        if (maybeFromSameThreadQueue != null) {
            return maybeFromSameThreadQueue;
        }
        try {
            return crossThreadTaskQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Runnable task) {
        if (closed) {
            throw new RejectedExecutionException("Was shut down");
        }
        if (isWorkerThread()) {
            sameThreadTaskQueue.add(task);
        } else {
            crossThreadTaskQueue.add(task);
        }
    }

    private boolean isWorkerThread() {
        return thread.getId() == Thread.currentThread().getId();
    }

    @Override
    public void close() {
        closed = true;
    }
}
