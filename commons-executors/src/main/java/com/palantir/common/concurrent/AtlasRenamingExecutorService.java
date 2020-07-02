/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.common.concurrent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.palantir.logsafe.Preconditions;

final class AtlasRenamingExecutorService extends AbstractExecutorService {

    private final ExecutorService delegate;

    private final Thread.UncaughtExceptionHandler handler;

    private final Supplier<String> nameSupplier;

    AtlasRenamingExecutorService(
            ExecutorService delegate, Thread.UncaughtExceptionHandler handler, Supplier<String> nameSupplier) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
        this.nameSupplier = Preconditions.checkNotNull(nameSupplier, "nameSupplier");
        this.handler = Preconditions.checkNotNull(handler, "handler");
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(new RenamingRunnable(command));
    }

    @Override
    public String toString() {
        return "AtlasRenamingExecutorService{delegate=" + delegate + '}';
    }

    final class RenamingRunnable implements Runnable {

        private final Runnable command;

        RenamingRunnable(Runnable command) {
            this.command = command;
        }

        @Override
        public void run() {
            final Thread currentThread = Thread.currentThread();
            final String originalName = currentThread.getName();
            currentThread.setName(nameSupplier.get());
            try {
                command.run();
            } catch (Throwable t) {
                handler.uncaughtException(currentThread, t);
            } finally {
                currentThread.setName(originalName);
            }
        }
    }

    static Supplier<String> threadNameSupplier(String name) {
        AtomicLong index = new AtomicLong();
        ThreadLocal<String> threadNameCache = ThreadLocal.withInitial(() -> name + "-" + index.getAndIncrement());
        return threadNameCache::get;
    }
}
