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

import com.palantir.logsafe.Preconditions;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Borrowed from jboss-threads. http://www.apache.org/licenses/LICENSE-2.0
 * https://github.com/jbossas/jboss-threads/blob/master/src/main/java/org/jboss/threads/ViewExecutor.java Changes have
 * been contributed and merged, this may be replaced by the upstream ViewExecutor pending a release including
 * https://github.com/jbossas/jboss-threads/pull/85.
 *
 * <p>Licensed under http://www.apache.org/licenses/LICENSE-2.0.
 * https://github.com/jbossas/jboss-threads/blob/5df767f325214acf3f7b80fa5354411c4453e073/LICENSE.txt
 *
 * <p>An executor service that is actually a "view" over another executor service.
 */
@SuppressWarnings("NullAway")
abstract class AtlasViewExecutor extends AbstractExecutorService {

    private static final Logger log = LoggerFactory.getLogger(AtlasViewExecutor.class);
    private volatile Thread.UncaughtExceptionHandler handler;
    private volatile Runnable terminationTask;

    // Intentionally package private to effectively seal the type.
    AtlasViewExecutor() {}

    @Override
    public final void shutdown() {
        shutdown(false);
    }

    abstract void shutdown(boolean interrupt);

    final Thread.UncaughtExceptionHandler getExceptionHandler() {
        return handler;
    }

    public final void setExceptionHandler(final Thread.UncaughtExceptionHandler value) {
        Preconditions.checkNotNull(value, "handler");
        this.handler = value;
    }

    final Runnable getTerminationTask() {
        return terminationTask;
    }

    final void setTerminationTask(final Runnable terminationTask) {
        this.terminationTask = terminationTask;
    }

    static Builder builder(Executor delegate) {
        Preconditions.checkNotNull(delegate, "delegate");
        return new Builder(delegate);
    }

    static final class Builder {
        private final Executor delegate;
        private short maxSize = 1;
        private int queueLimit = Integer.MAX_VALUE;
        private int queueInitialSize = 256;
        private Thread.UncaughtExceptionHandler handler = AtlasUncaughtExceptionHandler.INSTANCE;

        Builder(final Executor delegate) {
            this.delegate = delegate;
        }

        int getMaxSize() {
            return maxSize;
        }

        Builder setMaxSize(final int value) {
            Preconditions.checkArgument(value > 0, "maxSize must be positive");
            Preconditions.checkArgument(value <= Short.MAX_VALUE, "maxSize must not exceed " + Short.MAX_VALUE);
            this.maxSize = (short) value;
            return this;
        }

        int getQueueLimit() {
            return queueLimit;
        }

        Builder setQueueLimit(final int value) {
            Preconditions.checkArgument(value >= 0, "queueLimit must be non-negative");
            this.queueLimit = value;
            return this;
        }

        Executor getDelegate() {
            return delegate;
        }

        Thread.UncaughtExceptionHandler getUncaughtHandler() {
            return handler;
        }

        Builder setUncaughtHandler(final Thread.UncaughtExceptionHandler value) {
            this.handler = value;
            return this;
        }

        int getQueueInitialSize() {
            return queueInitialSize;
        }

        Builder setQueueInitialSize(final int queueInitialSize) {
            this.queueInitialSize = queueInitialSize;
            return this;
        }

        AtlasViewExecutor build() {
            if (queueLimit == 0) {
                return new AtlasQueuelessViewExecutor(
                        Preconditions.checkNotNull(delegate, "delegate"), maxSize, handler);
            }
            return new AtlasQueuedViewExecutor(
                    Preconditions.checkNotNull(delegate, "delegate"), maxSize, queueLimit, queueInitialSize, handler);
        }
    }

    protected final void runTermination() {
        final Runnable task = AtlasViewExecutor.this.terminationTask;
        AtlasViewExecutor.this.terminationTask = null;
        if (task != null) {
            try {
                task.run();
            } catch (Throwable t) {
                Thread.UncaughtExceptionHandler configuredHandler = handler;
                if (configuredHandler != null) {
                    try {
                        handler.uncaughtException(Thread.currentThread(), t);
                    } catch (Throwable tt) {
                        log.debug("failed to invoke uncaught exception handler", tt);
                    }
                }
            }
        }
    }
}
