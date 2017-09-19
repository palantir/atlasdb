/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.async.initializer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.logsafe.SafeArg;

/**
 * Implements basic infrastructure to allow an object to be asynchronously initialized.
 * In order to be ThreadSafe, the abstract methods of the inheriting class need to be synchronized.
 */
@ThreadSafe
public abstract class AsyncInitializer {
    private static final Logger log = LoggerFactory.getLogger(AsyncInitializer.class);
    private static final int NUM_THREADS = 100;
    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(
            NUM_THREADS, new NamedThreadFactory("AsyncInitializer", true));

    private volatile boolean initialized = false;

    public synchronized void initialize(boolean initializeAsync) {
        if (!initializeAsync) {
            tryToInitializeIfNotInitialized();
            return;
        }

        try {
            tryToInitializeIfNotInitialized();
        } catch (Throwable throwable) {
            log.info("Failed to initialize {} in the first attempt, will initialize asynchronously.",
                    SafeArg.of("className", this.getClass().getName()), throwable);
            cleanUpOnInitFailure();
            scheduleInitialization();
        }
    }

    private void scheduleInitialization() {
        EXECUTOR_SERVICE.schedule(() -> {
            synchronized (this) {
                try {
                    tryToInitializeIfNotInitialized();
                    log.info("Initialized {} asynchronously.",
                            SafeArg.of("className", this.getClass().getName()));
                } catch (Throwable throwable) {
                    cleanUpOnInitFailure();
                    scheduleInitialization();
                }
            }
        }, sleepIntervalInMillis(), TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected int sleepIntervalInMillis() {
        return 10_000;
    }

    private void tryToInitializeIfNotInitialized() {
        if (isInitialized()) {
            log.warn("{} was initialized underneath us.",
                    SafeArg.of("className", this.getClass().getName()));
        } else {
            tryInitialize();
            initialized = true;
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Override this method if there's anything to be cleaned up on initialization failure.
     * Default implementation is no-op.
     */
    protected synchronized void cleanUpOnInitFailure() {
        // no-op
    }

    protected abstract void tryInitialize();
}
